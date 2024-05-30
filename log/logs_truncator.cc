#include "logs_truncator.h"

#include <cstdint>
#include <queue>
#include <stop_token>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "file_writer.h"
#include "log.pb.h"
#include "log_reader.h"
#include "log_util.h"
#include "log_writer.h"
#include "util/status_macros.h"

namespace witnesskvs::log {

extern const uint64_t kIdxSentinelValue;

LogsTruncator::LogsTruncator(std::string dir, std::string prefix,
                             std::function<uint64_t(const Log::Message&)> idxfn)
    : dir_(std::move(dir)),
      prefix_(std::move(prefix)),
      idxfn_(std::move(idxfn)) {
  Init();
  worker_ =
      std::jthread([this](std::stop_token stop_token) { Run(stop_token); });
}

void LogsTruncator::Truncate(TruncationIdx max_idx) {
  absl::MutexLock l(&queue_lock_);
  queue_.push(max_idx);
}

void LogsTruncator::Register(TruncationFileInfo truncation_file_info) {
  absl::MutexLock l(&queue_lock_);
  queue_.push(std::move(truncation_file_info));
}

void LogsTruncator::Run(std::stop_token& stop_token) {
  while (!stop_token.stop_requested()) {
    absl::SleepFor(absl::Seconds(1));
    while (true) {
      std::variant<TruncationIdx, TruncationFileInfo> entry;
      {
        absl::MutexLock l(&queue_lock_);
        if (queue_.empty()) {
          break;
        }
        entry = queue_.front();
        queue_.pop();
      }
      if (std::holds_alternative<TruncationIdx>(entry)) {
        DoTruncation(std::get<TruncationIdx>(entry));
      } else {
        CHECK(std::holds_alternative<TruncationFileInfo>(entry));
        TruncationFileInfo& file_info = std::get<TruncationFileInfo>(entry);
        // We should always have a filename.
        CHECK(file_info.filename.has_value());

        absl::MutexLock l(&lock_);
        // Insertion shouldn't fail.
        CHECK(filename_max_idx_.emplace(file_info.filename.value(), file_info)
                  .second);
      }
    }
  }
}

void LogsTruncator::DoSingleFileTruncation(absl::string_view filename,
                                           uint64_t max_idx) {
  /***
   * Algorithm
   *   Read file and write out temporary version in a single file, discarding
   *   entries with an idx < max_idx.
   *
   *   Close file (ensuring sync).
   *
   *   Then move temporary file to a semi-permanent file.
   *
   *   Sync Directory.
   *
   *   Remove original file
   *
   *   Move semi-permanent to original file
   *
   *   Sync Directory.
   *
   *   On loading, look for both regular and semi-permanent files and do the
   * above swap if any were missed due to crash.
   */
  FileParts file_parts;
  {
    absl::StatusOr<FileParts> file_parts_or = ParseFilename(filename);
    if (!file_parts_or.ok()) {
      LOG(FATAL) << "Could not parse: " << filename
                 << file_parts_or.status().ToString();
    }
    file_parts = file_parts_or.value();
  }
  const std::string temp_filename =
      absl::StrCat(file_parts.prefix, "_temp_truncation.", file_parts.micros);
  {
    const std::string filename_str(filename);
    LogReader log_reader(filename_str);
    LogWriter log_writer(temp_filename, file_parts.micros);
    for (const Log::Message& msg : log_reader) {
      const uint64_t idx = idxfn_(msg);
      if (idx < max_idx) {
        // Okay to discard this older log entry.
        continue;
      }
      absl::Status status = log_writer.Log(msg);
      if (!status.ok()) {
        LOG(FATAL) << "Could not log: " << temp_filename << status.ToString();
      }
    }
  }

  const std::string perm_filename =
      absl::StrCat(file_parts.prefix, "_truncation.", file_parts.micros);
  std::error_code ec;
  std::filesystem::rename(std::filesystem::path(temp_filename),
                          std::filesystem::path(perm_filename), ec);
  if (ec) {
    LOG(FATAL) << absl::StrCat(
        "LogsTruncator::DoSingleFileTruncation: Can't rename: ", temp_filename,
        " to ", perm_filename, ": ", ec.message());
  }
  std::string dir = std::filesystem::path(perm_filename).parent_path().string();
  FileWriter::SyncDir(dir);
  // We are assured at this point that both prem_filename and filename exists.
  // Now we can safely delete filename.

  // If a crash happens here, our loading mechanism will reconcile the two
  // files.
  ReplaceFile(std::string(filename), perm_filename);
}

void LogsTruncator::DoTruncation(uint64_t max_idx) {
  absl::MutexLock l(&lock_);
  for (const auto& [filename, file_info] : filename_max_idx_) {
    if (file_info.max_idx < max_idx) {
      // Delete file.
      if (!std::filesystem::remove(std::filesystem::path(filename))) {
        LOG(FATAL) << absl::StrCat(
            "LogsTruncator::DoTruncation: Can't remove: ", filename, " ",
            std::strerror(errno));
      }
    } else if (file_info.min_idx < max_idx) {
      // can remove individual entries...
      DoSingleFileTruncation(filename, max_idx);
      CHECK(filename_max_idx_.contains(filename))
          << "Strange, expected filename in the filename_max_idx_ table: "
          << filename;
      CHECK_OK(ReadHeader(std::filesystem::path(filename), false))
          << "Trouble reading header of " << filename;
    }
  }
}

absl::Status LogsTruncator::ReadHeader(const std::filesystem::path& path,
                                       bool insert) {
  CHECK(idxfn_);
  LogReader log_reader(path.string());
  ASSIGN_OR_RETURN(Log::Header header, log_reader.header());
  uint64_t min_idx = header.min_idx();
  uint64_t max_idx = header.max_idx();
  if (min_idx == kIdxSentinelValue || max_idx == kIdxSentinelValue) {
    // need to compute min/max.

    for (const Log::Message& msg : log_reader) {
      const uint64_t idx = idxfn_(msg);
      if (idx != kIdxSentinelValue) {
        if (min_idx == kIdxSentinelValue || min_idx > idx) {
          min_idx = idx;
        }
        if (max_idx == kIdxSentinelValue || max_idx < idx) {
          max_idx = idx;
        }
      }
    }
    // Update the file.
    if (min_idx != kIdxSentinelValue && max_idx != kIdxSentinelValue) {
      absl::Cord cord = GetIdxCord(min_idx, max_idx);
      FileWriter::WriteHeader(path, cord);
    }
  }
  if (min_idx != kIdxSentinelValue && max_idx != kIdxSentinelValue) {
    if (insert) {
      CHECK(filename_max_idx_
                .emplace(path.string(), TruncationFileInfo{.min_idx = min_idx,
                                                           .max_idx = max_idx})
                .second)
          << " duplicate filename: " << path.string();
    } else {
      CHECK(!filename_max_idx_
                 .insert_or_assign(
                     path.string(),
                     TruncationFileInfo{.min_idx = min_idx, .max_idx = max_idx})
                 .second);
    }
  } else {
    VLOG(1) << "Could not find valid min/max idx for file: " << path.string();
  }
  return absl::OkStatus();
}

void LogsTruncator::Init() {
  CheckReadDir(dir_);
  CheckPrefix(prefix_);
  absl::StatusOr<std::vector<std::filesystem::path>> files =
      ReadDir(dir_, prefix_, /*cleanup=*/false, /*sort=*/false);
  CHECK_OK(files.status());
  absl::MutexLock l(&lock_);
  for (const auto& path : files.value()) {
    absl::Status status = ReadHeader(path);
    if (!status.ok()) {
      VLOG(1) << "ReadHeader - bad header in file: " << path.string() << ": "
              << status.ToString();
    }
  }
}

absl::flat_hash_map<std::string, LogsTruncator::TruncationFileInfo>
LogsTruncator::filename_max_idx() {
  absl::MutexLock l(&lock_);
  return filename_max_idx_;
}

}  // namespace witnesskvs::log
