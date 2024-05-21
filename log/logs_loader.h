#ifndef LOG_LOGS_LOADER_H
#define LOG_LOGS_LOADER_H

#include <filesystem>
#include <iterator>

#include "log_reader.h"
#include "log.pb.h"

#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/strings/string_view.h"

namespace witnesskvs::log {

class LogsLoader {
 public:
  LogsLoader() = delete;
  LogsLoader(absl::string_view dir, absl::string_view prefix);
  LogsLoader(absl::string_view dir, absl::string_view prefix, absl::AnyInvocable<bool(const Log::Message& a, const Log::Message& b)> sortfn);

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
      return it.logs_loader == logs_loader && it.counter == counter && cur == nullptr &&
             it.cur == nullptr;
    }
  };
  static_assert(std::input_or_output_iterator<iterator>);

  iterator begin() { return iterator(this); }
  iterator end() { return iterator(this, nullptr); }

 private:
  // Reads the list of files satisfying the prefix from dir_.
  absl::StatusOr<std::vector<std::filesystem::path>> ReadDir(absl::string_view dir, absl::string_view prefix);
  void reset();
  absl::StatusOr<Log::Message> next();

  std::vector<std::filesystem::path> files_; // List of files found.

   // TODO(mmucklo): use this function for sorting log messages as they come out.
  absl::AnyInvocable<bool(const Log::Message& a, const Log::Message& b)> sortfn_;

  int64_t current_file_idx_; // -1 here is a sentinel value.
  uint64_t current_counter_; // never negative.
  std::unique_ptr<LogReader> reader_;
  std::unique_ptr<LogReader::iterator> it_;
};

}  // namespace witnesskvs::log

#endif
