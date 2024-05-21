#ifndef LOG_LOGS_LOADER_H
#define LOG_LOGS_LOADER_H

#include <filesystem>
#include <iterator>

#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/strings/string_view.h"
#include "log.pb.h"
#include "log_reader.h"

namespace witnesskvs::log {

/**
 * A LogsLoader that takes in a directory and a prefix and provides an
 * iterator to read all the log entries out.
 *
 * Optionally takes in a sort function to sort the entries first before
 * returning them. Sorting requires a certain amount of memory to
 * load the entries in and then sort them.
 */
class LogsLoader {
 public:
  LogsLoader() = delete;

  // dir is the directory to find the log messages
  // prefix is the file naming scheme.
  //
  // TODO(mmucklo) - RAFT dissertation seems to call a prefix something
  // discardable like the timestamp we use to suffix our logfiles. Maybe we
  // should try to see if this is standard convention and if so, maybe call this
  // "suffix" or "ext". In our implementation it's the suffixes of log files
  // that may be discarded.
  LogsLoader(absl::string_view dir, absl::string_view prefix);

  // This constructor takes in a sort function that will be used to sort
  // the log messages before they are returned from the iterator.
  LogsLoader(
      absl::string_view dir, absl::string_view prefix,
      absl::AnyInvocable<bool(const Log::Message& a, const Log::Message& b)>
          sortfn);

  struct iterator {
    using difference_type = std::ptrdiff_t;

   private:
    LogsLoader* logs_loader;
    uint64_t counter;
    std::unique_ptr<Log::Message> cur;
    void next();
    void reset();

   public:
    iterator() { LOG(FATAL) << "not implemented."; };
    iterator(LogsLoader* ll);
    iterator(const iterator& it) {
      counter = it.counter;
      logs_loader = it.logs_loader;
      reset();
    }
    iterator& operator=(iterator& other) {
      counter = other.counter;
      logs_loader = other.logs_loader;
      reset();
      return *this;
    }
    iterator& operator=(iterator&& other) {
      counter = other.counter;
      logs_loader = other.logs_loader;
      cur = std::move(other.cur);
      return *this;
    }
    iterator(iterator&& it) {
      counter = it.counter;
      logs_loader = it.logs_loader;
      cur = std::move(it.cur);
    }
    iterator(LogsLoader* lr, std::unique_ptr<Log::Message> sentinel);
    Log::Message& operator*() { return *cur; }
    iterator& operator++() {
      next();
      return *this;
    }
    void operator++(int) { ++*this; }
    bool operator==(const iterator& it) const {
      return it.logs_loader == logs_loader && it.counter == counter &&
             cur == nullptr && it.cur == nullptr;
    }
  };
  static_assert(std::input_or_output_iterator<iterator>);

  iterator begin() { return iterator(this); }
  iterator end() { return iterator(this, nullptr); }

 private:
  // Reads the list of files satisfying the prefix from dir_.
  absl::StatusOr<std::vector<std::filesystem::path>> ReadDir(
      absl::string_view dir, absl::string_view prefix);

  // Resets the reading, intended to be called on begin().
  void reset();

  void Init(absl::string_view dir, absl::string_view prefix);

  // Advances to the next message, or the next file and the first
  // message in it, if we're at the end of the file. If files
  // are blank, then we skip them.
  //
  // TODO(mmucklo): if file is corrupt, skip.
  absl::StatusOr<Log::Message> next();

  // Returns a sorted version of the messages, intended to be
  // called from within next() when a sortfn_ is present.
  absl::StatusOr<Log::Message> next_sorted();

  // Loads and sorts messages from the current file.
  void LoadAndSortMessages();

  std::vector<std::filesystem::path> files_;  // List of files found.

  // TODO(mmucklo): use this function for sorting log messages as they come out.
  absl::AnyInvocable<bool(const Log::Message& a, const Log::Message& b)>
      sortfn_;

  int64_t current_file_idx_;  // -1 here is a sentinel value.
  uint64_t current_counter_;  // never negative.
  std::unique_ptr<LogReader> reader_;
  std::unique_ptr<LogReader::iterator> it_;

  // If we pre-sort the messages this will be the buffer we load into.
  uint64_t msgs_counter_;
  std::unique_ptr<std::vector<Log::Message>> msgs_;
};

}  // namespace witnesskvs::log

#endif
