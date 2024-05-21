#include "logs_loader.h"

#include <filesystem>
#include <vector>

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

namespace witnesskvs::log {

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
  checkDir(dir);
  checkPrefix(prefix);
  absl::StatusOr<std::vector<std::filesystem::path>> entries =
      ReadDir(dir, prefix);
  CHECK_OK(entries) << "Bad result reading the directory: "
                    << entries.status().ToString();
  files_ = std::move(entries.value());
}
LogsLoader::LogsLoader(absl::string_view dir, absl::string_view prefix)
    : current_file_idx_(-1), current_counter_(0) {
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
}

absl::StatusOr<Log::Message> LogsLoader::next() {
  if (current_file_idx_ >= static_cast<int64_t>(files_.size())) {
    VLOG(1) << "LogsLoader::next OutOfRangeError: current_file_idx_: "
            << current_file_idx_ << " files_.size(): " << files_.size();
    return absl::OutOfRangeError("No more files");
  }
  if (current_file_idx_ >= 0 && *it_ != reader_->end()) {
    ++(*it_);
  }
  while (current_file_idx_ < 0 || *it_ == reader_->end()) {
    ++current_file_idx_;
    if (current_file_idx_ >= files_.size()) {
      return absl::OutOfRangeError("(1)No more files");
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
