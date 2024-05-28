#ifndef LOG_LOGS_LOADER_H
#define LOG_LOGS_LOADER_H

#include <cstddef>
#include <filesystem>
#include <iterator>

#include "absl/log/log.h"
#include "absl/strings/string_view.h"
#include "log.pb.h"
#include "log_reader.h"

namespace witnesskvs::log {

template <typename T, typename U>
struct logs_iterator {
 public:
  using difference_type = std::ptrdiff_t;
  logs_iterator() { LOG(FATAL) << "not implemented."; };
  logs_iterator(T* l) : loader(l), counter(0), cur(nullptr) {}
  logs_iterator(T* l, std::unique_ptr<Log::Message> sentinel)
      : loader(l), counter(0), cur(std::move(sentinel)) {}

  logs_iterator(const logs_iterator& it) {
    counter = it.counter;
    loader = it.loader;
  }
  logs_iterator& operator=(logs_iterator& other) {
    counter = other.counter;
    loader = other.loader;
    return *this;
  }
  logs_iterator& operator=(logs_iterator&& other) {
    counter = other.counter;
    loader = other.loader;
    cur = std::move(other.cur);
    return *this;
  }
  logs_iterator(logs_iterator&& it) {
    counter = it.counter;
    loader = it.loader;
    cur = std::move(it.cur);
  }
  Log::Message& operator*() { return *cur; }
  U& operator++() {
    next();
    return *reinterpret_cast<U*>(this);
  }
  void operator++(int) { ++*this; }
  bool operator==(const logs_iterator& it) const {
    return it.loader == loader && it.counter == counter && cur == nullptr &&
           it.cur == nullptr;
  }

 protected:
  T* loader;
  uint64_t counter;
  std::unique_ptr<Log::Message> cur;
  virtual void next() = 0;
};

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

  // Disable copy (and move) semantics.
  LogsLoader(const LogsLoader&) = delete;
  LogsLoader& operator=(const LogsLoader&) = delete;

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
      std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn);

  // Takes in a list of file paths that are assumed to be presorted.
  LogsLoader(std::vector<std::filesystem::path> files);
  LogsLoader(
      std::vector<std::filesystem::path> files,
      std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn);

  struct iterator : public logs_iterator<LogsLoader, iterator> {
   public:
    using logs_iterator::logs_iterator;
    iterator(LogsLoader* l) : logs_iterator(l) { reset(); }

   protected:
    virtual void next() override;

   private:
    void reset();
  };
  static_assert(std::input_or_output_iterator<iterator>);

  iterator begin() { return iterator(this); }
  iterator end() { return iterator(this, nullptr); }

 private:
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

  std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn_;

  int64_t current_file_idx_;  // -1 here is a sentinel value.
  uint64_t current_counter_;  // never negative.
  std::unique_ptr<LogReader> reader_;
  std::unique_ptr<LogReader::iterator> it_;

  // If we pre-sort the messages this will be the buffer we load into.
  uint64_t msgs_counter_;
  std::unique_ptr<std::vector<Log::Message>> msgs_;
};

/**
 * SortingLogsLoader sorts all the files under a directory with a specific
 * prefix according to the passed-in sort function, merging across files
 * such that the output via iterator is a completely sorted set.
 * 
 * It essentially runs an external merge sort across all the files within a constrained
 * memory limit that's controlled by a command-line flag.
 * https://en.wikipedia.org/wiki/External_sorting
 * 
 * We still have an overall limit on the log file size (as defined by flag in log_writer.cc), so the external merge sort will still possibly produce multiple files, however these will be completed sorted and ordered by timestamp (in microseconds).
 * 
 * TODO(mmucklo): relax fsync a bit during the merge sort algorithm as we only need to
 * fsync when the sorted file is completely written (since we already have a base durable copy under the prefix passed in).
 * 
 * TODO(mmucklo): cleanup merge-sort file fragments after merge sort is complete. Possibly rename all new sorted files such that the completed merge
 * sort is the durable version so that there's only one copy afterwards.
 */
class SortingLogsLoader {
 public:
  SortingLogsLoader() = delete;

  // Disable copy (and move) semantics.
  SortingLogsLoader(const SortingLogsLoader&) = delete;
  SortingLogsLoader& operator=(const SortingLogsLoader&) = delete;

  // dir is the directory to find the log messages.
  // prefix is the file naming scheme.
  // sortfn is the logs sorting function.
  SortingLogsLoader(
      absl::string_view dir, absl::string_view prefix,
      std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn);

  struct iterator : public logs_iterator<SortingLogsLoader, iterator> {
   public:
    using difference_type = std::ptrdiff_t;
    using logs_iterator::logs_iterator;
    iterator(SortingLogsLoader* l) : logs_iterator(l) { reset(); }

   protected:
    virtual void next() override;

   private:
    std::optional<LogsLoader::iterator> llit;
    void reset();
  };
  static_assert(std::input_or_output_iterator<iterator>);

  iterator begin() { return iterator(this); }
  iterator end() { return iterator(this, nullptr); }

  // If a merge was performed, this will be the prefix of the merged files.
  std::optional<std::string> prefix_merge() { return prefix_merge_; }

 private:
  void Init(
      absl::string_view dir, absl::string_view prefix,
      std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn);
  std::unique_ptr<LogsLoader> logs_loader_;
  std::optional<std::string> prefix_merge_;
};

}  // namespace witnesskvs::log

#endif
