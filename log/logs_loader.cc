#include "logs_loader.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "log.pb.h"
#include "log_reader.h"
#include "log_util.h"
#include "log_writer.h"

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

// The maximum amount of memory we can use for loading and sorting the log
// entries if we need to sort.
ABSL_FLAG(uint64_t, logs_loader_max_memory_for_sorting, 1 << 30,
          "Max memory for sorting");

ABSL_DECLARE_FLAG(uint64_t, log_writer_max_msg_size);

namespace witnesskvs::log {

LogsLoader::LogsLoader(
    absl::string_view dir, absl::string_view prefix,
    std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn)
    : current_file_idx_(-1), current_counter_(0), sortfn_(std::move(sortfn)) {
  Init(dir, prefix);
}

LogsLoader::LogsLoader(absl::string_view dir, absl::string_view prefix)
    : current_file_idx_(-1),
      current_counter_(0),
      msgs_counter_(-1),
      sortfn_(nullptr) {
  Init(dir, prefix);
}

LogsLoader::LogsLoader(std::vector<std::filesystem::path> files)
    : files_(std::move(files)) {}
LogsLoader::LogsLoader(
    std::vector<std::filesystem::path> files,
    std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn)
    : files_(std::move(files)), sortfn_(std::move(sortfn)) {}

void LogsLoader::Init(absl::string_view dir, absl::string_view prefix) {
  CheckReadDir(dir);
  CheckPrefix(prefix);
  absl::StatusOr<std::vector<std::filesystem::path>> entries =
      ReadDir(dir, prefix, /*cleanup=*/true, /*sort=*/true);
  CHECK_OK(entries) << "Bad result reading the directory: "
                    << entries.status().ToString();
  files_ = std::move(entries.value());
}

void LogsLoader::reset() {
  if (files_.empty()) {
    return;
  }
  current_file_idx_ = -1;
  current_counter_ = 0;
  reader_.reset();
  it_.reset();
  msgs_counter_ = -1;
  msgs_.reset();
}

// TODO(mmucklo): maybe make this more functional for clarty or have it create
// it's own iterator.
void LogsLoader::LoadAndSortMessages() {
  // TODO(mmucklo): Deal with memory constraints and do external merge sort.
  LogReader reader(files_[current_file_idx_].string());
  LogReader::iterator it = reader.begin();
  msgs_ = std::make_unique<std::vector<Log::Message>>();
  while (it != reader.end()) {
    msgs_->push_back(*it);
    it++;
  }
  std::sort(msgs_->begin(), msgs_->end(), sortfn_);
  msgs_counter_ = 0;
}

absl::StatusOr<Log::Message> LogsLoader::next_sorted() {
  while (msgs_ == nullptr || msgs_counter_ >= msgs_->size()) {
    ++current_file_idx_;
    if (current_file_idx_ >= files_.size()) {
      return absl::OutOfRangeError("No more files (1)");
    }
    LoadAndSortMessages();
  }
  CHECK_LT(msgs_counter_, msgs_->size());
  ++current_counter_;
  return (std::move(msgs_->at(msgs_counter_++)));  // intentional postfix.
}

absl::StatusOr<Log::Message> LogsLoader::next() {
  if (current_file_idx_ >= static_cast<int64_t>(files_.size())) {
    VLOG(1) << "LogsLoader::next OutOfRangeError: current_file_idx_: "
            << current_file_idx_ << " files_.size(): " << files_.size();
    return absl::OutOfRangeError("No more files");
  }

  // Special sorted retrieval.
  //
  // TODO(mmucklo): deal with memory constraints and use external sorting
  // if necessary, spooling to disk.
  if (sortfn_) {
    return next_sorted();
  }

  // Basic (unsorted retrieval). Steps through the file, uses less memory.
  if (current_file_idx_ >= 0 && *it_ != reader_->end()) {
    ++(*it_);
  }
  while (current_file_idx_ < 0 || *it_ == reader_->end()) {
    ++current_file_idx_;
    if (current_file_idx_ >= files_.size()) {
      return absl::OutOfRangeError("No more files (2)");
    }
    reader_ = std::make_unique<LogReader>(files_[current_file_idx_].string());
    it_ = std::make_unique<LogReader::iterator>(reader_->begin());
  }
  ++current_counter_;
  return *(*it_);
}

void LogsLoader::iterator::reset() {
  cur = nullptr;
  counter = 0;
  loader->reset();
  if (loader->files_.empty()) {
    return;
  }
  next();
}

void LogsLoader::iterator::next() {
  VLOG(1) << "LogsLoader::iterator::next";
  // TODO(mmucklo) - reset and call next() a bunch of times if counter doesn't
  // match.
  CHECK_EQ(counter, loader->current_counter_);

  absl::StatusOr<Log::Message> msg_or = loader->next();
  counter = loader->current_counter_;
  if (msg_or.ok()) {
    VLOG(1) << "next ok";
    cur = std::make_unique<Log::Message>(std::move(msg_or.value()));
    return;
  }
  VLOG(1) << "next not ok" << msg_or.status().message();
  cur = nullptr;
  counter = 0;
}

// Sorts a specific log file according to the passed-in sort function.
// Outputs a new sorted log file with "_sorted" appended to the filename prefix.
// returns the final full filename prefix.
//
// Expects the path to be valid and already checked.
// No guarantees that the sorted log file will be output into a single file if
// the log writers flags have been changed (as far as the max space per log
// file) in between the time the log file was output and the time this sort
// done.
//
// Returns the list of files written to.
std::vector<std::string> SortLogsFile(
    std::filesystem::path path, absl::string_view prefix_sorted,
    const std::function<bool(const Log::Message& a, const Log::Message& b)>&
        sortfn) {
  CHECK(path.has_parent_path());
  std::filesystem::path parent_path = path.parent_path();

  CHECK(sortfn);
  std::vector<Log::Message> msgs;
  {
    LogReader reader(path.string());
    LogReader::iterator it = reader.begin();
    while (it != reader.end()) {
      msgs.push_back(*it);
      it++;
    }
  }
  std::sort(msgs.begin(), msgs.end(), sortfn);
  LogWriter log_writer(parent_path.string(), std::string(prefix_sorted));
  log_writer.SetSkipFlush(true);
  for (const Log::Message& msg : msgs) {
    CHECK_OK(log_writer.Log(msg));
  }
  return log_writer.filenames();
}

// Merges the list of input_prefixes into output_prefix in directory dir.
// Returns a list of filenames outputted into.
std::vector<std::string> MergeSortedFiles(
    absl::string_view dir, const std::vector<std::string>& input_prefixes,
    absl::string_view output_prefix,
    const std::function<bool(const Log::Message& a, const Log::Message& b)>&
        sortfn) {
  LogWriter log_writer{std::string(dir), std::string(output_prefix)};
  log_writer.SetSkipFlush(true);
  struct LogMessageContainer {
    std::shared_ptr<LogsLoader> logs_loader;
    LogsLoader::iterator it;
    Log::Message msg;
  };
  auto cmp = [&sortfn](const LogMessageContainer& a,
                       const LogMessageContainer& b) {
    return sortfn(a.msg, b.msg);
  };
  std::set<LogMessageContainer, decltype(cmp)> ordered_messages(cmp);

  // Initialize the set of messages.
  for (const auto& prefix : input_prefixes) {
    std::shared_ptr<LogsLoader> logs_loader =
        std::make_shared<LogsLoader>(dir, prefix);
    LogMessageContainer container{.logs_loader = logs_loader,
                                  .it = logs_loader->begin()};
    if (container.it != logs_loader->end()) {
      container.msg = *container.it;
      ordered_messages.insert(std::move(container));
    }
  }

  // Output the messages from the files in order.
  while (!ordered_messages.empty()) {
    auto it = ordered_messages.begin();
    LogMessageContainer container{
        .logs_loader = it->logs_loader,
        .it = it->it,
    };
    CHECK_OK(log_writer.Log(it->msg));
    ordered_messages.erase(it);
    ++container.it;
    if (container.it != container.logs_loader->end()) {
      container.msg = *container.it;
      ordered_messages.insert(std::move(container));
    }
  }
  return log_writer.filenames();
}
SortingLogsLoader::SortingLogsLoader(
    absl::string_view dir, absl::string_view prefix,
    std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn) {
  Init(dir, prefix, std::move(sortfn));
}

void CleanupDir(absl::string_view dir, absl::string_view prefix) {
  absl::StatusOr<std::vector<std::filesystem::path>> entries =
      ReadDir(dir, prefix, /*cleanup=*/false, /*sort=*/false);
  CHECK_OK(entries);
  if (entries.value().size() > 0) {
    std::vector<std::string> files;
    for (const auto& entry : entries.value()) {
      files.push_back(entry.string());
    }
    CleanupFiles(files);
  }
}

// Sorting the files will cause some temporary files to be created.
// Though we delete intermediary files, we will still end up leaving some
// temporary files around unless we have an atomic rename function that touches
// multiple files. While this is possible, the easier solution might be to
// delete on destruction of the SortingLogsLoader, since then we're sure that
// they will no longer be iterated over. With intermediary logs cleanup, the
// worst case space blowup should be O(2n).
void SortingLogsLoader::Init(
    absl::string_view dir, absl::string_view prefix,
    std::function<bool(const Log::Message& a, const Log::Message& b)> sortfn) {
  CheckReadDir(dir);
  CheckPrefix(prefix);
  const std::string prefix_sorted = absl::StrCat(prefix, "_sorted");
  const std::string prefix_merge = absl::StrCat(prefix, "_merge_sorted");
  CleanupDir(dir, prefix_sorted);
  CleanupDir(dir, prefix_merge);

  absl::StatusOr<std::vector<std::filesystem::path>> entries =
      ReadDir(dir, prefix, /*cleanup=*/true, /*sort=*/false);
  CHECK_OK(entries) << "Bad result reading the directory: "
                    << entries.status().ToString();
  std::vector<std::filesystem::path> files = std::move(entries.value());

  // Step 1, sort all the files.
  std::vector<std::string>
      sorted_prefixes;  // This will be a list of all the file prefixes that has
                        // been sorted.
  std::vector<std::string>
      cleanup_files;  // This will be an ongoing list of temporary intermediate
                      // files to cleanup.
  {
    uint64_t sort_idx = 0;
    for (auto& path : files) {
      std::string prefix_sorted_idx = absl::StrCat(prefix_sorted, sort_idx);
      sorted_prefixes.push_back(prefix_sorted_idx);
      std::vector<std::string> sorted_files =
          SortLogsFile(path, prefix_sorted_idx, sortfn);
      sort_idx++;
      cleanup_files.insert(cleanup_files.end(), sorted_files.begin(),
                           sorted_files.end());
    }
  }

  // Step 2, do the merge algorithm.
  const uint64_t max_files =
      std::ceil(static_cast<double>(
                    absl::GetFlag(FLAGS_logs_loader_max_memory_for_sorting)) /
                absl::GetFlag(FLAGS_log_writer_max_msg_size));
  CHECK_GT(max_files, 1);
  VLOG(1) << "SortingLogsLoader: max_files: " << max_files;
  int merge_round = 0;
  while (sorted_prefixes.size() > 0) {
    merge_round++;
    std::list<std::vector<std::string>> merge_lists;
    std::vector<std::string> cur_paths;
    for (size_t i = 0; i < sorted_prefixes.size(); i++) {
      if ((i + 1) % max_files == 0) {
        merge_lists.push_back(cur_paths);
        cur_paths.clear();
      }
      cur_paths.push_back(sorted_prefixes[i]);
    }
    merge_lists.push_back(cur_paths);
    cur_paths.clear();
    sorted_prefixes.clear();
    CHECK_GT(merge_lists.size(), 0);

    // Final merge.
    if (merge_lists.size() == 1) {
      temp_files_ =
          MergeSortedFiles(dir, merge_lists.front(), prefix_merge, sortfn);
      break;
    }

    // Intermediary merge.
    uint64_t group = 0;
    for (auto& merge_list : merge_lists) {
      ++group;
      std::string prefix_merge_round =
          absl::StrCat(prefix_merge, "_round_", merge_round, "_group_", group);
      CHECK_GT(merge_list.size(), 0);
      std::vector<std::string> merge_files =
          MergeSortedFiles(dir, merge_list, prefix_merge_round, sortfn);
      sorted_prefixes.push_back(prefix_merge_round);

      // Can get rid of intermediate files now.
      CleanupFiles(cleanup_files);
      cleanup_files.swap(merge_files);
    }
  }

  if (cleanup_files.size() > 0) {
    CleanupFiles(cleanup_files);
    cleanup_files.clear();
  }

  for (const auto& filename : cleanup_files) {
    if (!std::filesystem::remove(std::filesystem::path(filename))) {
      LOG(FATAL) << absl::StrCat("Can't cleanup: ", filename, " ",
                                 std::strerror(errno));
    }
  }
  logs_loader_ = std::make_unique<LogsLoader>(dir, prefix_merge);
  prefix_merge_ = prefix_merge;
}

void SortingLogsLoader::iterator::reset() {
  cur = nullptr;
  counter = 0;
  llit = loader->logs_loader_->begin();
  if (*llit == loader->logs_loader_->end()) {
    llit = std::nullopt;
    return;
  }
  cur = std::make_unique<Log::Message>(std::move(*(*llit)));
}

void SortingLogsLoader::iterator::next() {
  VLOG(2) << "SortingLogsLoader::iterator::next";
  CHECK(llit != std::nullopt);
  ++(*llit);
  if (*llit == loader->logs_loader_->end()) {
    cur = nullptr;
    counter = 0;
    return;
  }
  cur = std::make_unique<Log::Message>(std::move((*(*llit))));
  counter++;
}

SortingLogsLoader::~SortingLogsLoader() {
  // These were the sorted and merged files that we temporarily created
  // during recovery. Once we're done iterating through them and this class is
  // destructed, it's safe to delete them.
  CleanupFiles(temp_files_);
}

}  // namespace witnesskvs::log
