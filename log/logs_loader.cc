#include "logs_loader.h"

#include <filesystem>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "log.pb.h"
#include "log_reader.h"
#include "re2/re2.h"

// The maximum amount of memory we can use for loading and sorting the log
// entries if we need to sort.
ABSL_FLAG(uint64_t, logs_loader_max_memory_for_sorting, 1 << 30,
          "Max memory for sorting");

namespace witnesskv::log {

extern const char kFilenamePrefix[];

void checkDir(absl::string_view dir) {
  // Should be an existing readable, executable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  const std::filesystem::file_status dir_status =
      std::filesystem::status(std::string(dir));
  if (!std::filesystem::exists(dir_status)) {
    LOG(FATAL) << "LogsLoader: dir '" << dir << "' should exist.";
  }
  if (!std::filesystem::is_directory(dir_status)) {
    LOG(FATAL) << "LogsLoader: dir '" << dir << "' should be a directory.";
  }
  std::filesystem::perms perms = dir_status.permissions();
  if ((perms & std::filesystem::perms::owner_write) !=
          std::filesystem::perms::owner_write ||
      (perms & std::filesystem::perms::owner_exec) !=
          std::filesystem::perms::owner_exec) {
    LOG(FATAL) << "LogWriter: dir '" << dir
               << "' should be readable and executable.";
  }
}

void checkPrefix(absl::string_view prefix) {
  if (!re2::RE2::FullMatch(prefix, kFilenamePrefix)) {
    LOG(FATAL) << "LogLoader: prefix should match " << kFilenamePrefix;
  }
}

LogsLoader::LogsLoader(
    absl::string_view dir, absl::string_view prefix,
    absl::AnyInvocable<bool(const Log::Message& a, const Log::Message& b)>
        sortfn)
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

void LogsLoader::Init(absl::string_view dir, absl::string_view prefix) {
  checkDir(dir);
  checkPrefix(prefix);
  absl::StatusOr<std::vector<std::filesystem::path>> entries =
      ReadDir(dir, prefix);
  CHECK_OK(entries) << "Bad result reading the directory: "
                    << entries.status().ToString();
  files_ = std::move(entries.value());
}

absl::StatusOr<std::vector<std::filesystem::path>> LogsLoader::ReadDir(
    absl::string_view dir, absl::string_view prefix) {
  const std::filesystem::path path{std::string(dir)};
  struct FileEntry {
    uint64_t ext_micros;
    std::filesystem::path path;
  };
  std::vector<FileEntry> entries;
  for (const auto& dir_entry : std::filesystem::directory_iterator{path}) {
    if (!dir_entry.is_regular_file()) {
      // TODO(mmucklo): maybe consider following symlinks or not?
      continue;
    }
    std::string filename = dir_entry.path().filename();
    if (!absl::StartsWith(filename, prefix)) {
      continue;
    }
    const std::filesystem::file_status file_status =
        std::filesystem::status(dir_entry.path());
    std::filesystem::perms perms = file_status.permissions();
    if ((perms & std::filesystem::perms::owner_read) !=
        std::filesystem::perms::owner_read) {
      return absl::PermissionDeniedError(
          absl::StrFormat("log file %s is not readable", filename));
    }
    std::vector<absl::string_view> parts = absl::StrSplit(filename, ".");
    if (!parts.size() == 2) {
      LOG(WARNING) << "LogsLoader: expect parts of filename to be splittable "
                      "in two, instead there are "
                   << parts.size() << " for " << filename << " - skipping.";
      continue;
    }
    std::string ext = std::string(parts[1]);
    uint64_t ext_micros;
    if (!absl::SimpleAtoi(ext, &ext_micros)) {
      return absl::OutOfRangeError(absl::StrFormat(
          "file extension is not parsable as an uint64_t: %s", filename));
    }
    entries.push_back(
        FileEntry{.ext_micros = ext_micros, .path = dir_entry.path()});
  }

  // Read the earliest file first. As long as we don't write parallel log
  // streams, the files themselves should have a happens-before relationship.
  std::sort(entries.begin(), entries.end(),
            [](const FileEntry& a, const FileEntry& b) {
              return a.ext_micros < b.ext_micros;
            });
  std::vector<std::filesystem::path> ret;
  for (auto& entry : entries) {
    ret.push_back(std::move(entry.path));
  }
  return ret;
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
  CHECK(sortfn_);
  LogReader reader(files_[current_file_idx_].string());
  LogReader::iterator it = reader.begin();
  msgs_ = std::make_unique<std::vector<Log::Message>>();
  while (it != reader.end()) {
    msgs_->push_back(*it);
    it++;
  }
  std::sort(msgs_->begin(), msgs_->end(),
            [this](const Log::Message& a, const Log::Message& b) {
              // TODO(mmucklo): see if there's a way we can get rid of the
              // wrapping closure.
              return sortfn_(a, b);
            });
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

LogsLoader::iterator::iterator(LogsLoader* ll) : logs_loader(ll), counter(0) {
  reset();
}

void LogsLoader::iterator::reset() {
  cur = nullptr;
  counter = 0;
  logs_loader->reset();
  if (logs_loader->files_.empty()) {
    return;
  }
  next();
}

void LogsLoader::iterator::next() {
  VLOG(1) << "LogsLoader::iterator::next";
  // TODO(mmucklo) - reset and call next() a bunch of times if counter doesn't
  // match.
  CHECK_EQ(counter, logs_loader->current_counter_);

  absl::StatusOr<Log::Message> msg_or = logs_loader->next();
  counter = logs_loader->current_counter_;
  if (msg_or.ok()) {
    VLOG(1) << "next ok";
    cur = std::make_unique<Log::Message>(std::move(msg_or.value()));
    return;
  }
  VLOG(1) << "next not ok" << msg_or.status().message();
  cur = nullptr;
  counter = 0;
}

LogsLoader::iterator::iterator(LogsLoader* ll,
                               std::unique_ptr<Log::Message> sentinel)
    : logs_loader(ll), counter(0), cur(std::move(sentinel)) {}

}  // namespace witnesskvs::log
