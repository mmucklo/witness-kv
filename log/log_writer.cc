#include "log_writer.h"

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>
#include <type_traits>

#include "absl/crc/crc32c.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "byte_conversion.h"
#include "log.pb.h"
#include "log_util.h"
#include "re2/re2.h"

// TODO(mmucklo): explore encrypted at rest
// TODO(mmucklo): compression
// Compression options:
//   1) Compress whole file after rotation
//   2) Compress each entry on the fly.
//   3) Compress both entries and whole file (would this even be good?)
//   seems like #1 would be best, but results in larger files on disk
//   temporarily, then need a background file compressor
//
// TODO(mmucklo): Sortable log files - swap prefix and suffix?

ABSL_FLAG(uint64_t, log_writer_max_file_size, 1 << 30,
          "Maximum file size for logs");

ABSL_FLAG(uint64_t, log_writer_max_msg_size, 1 << 20,
          "Maximum message size in bytes (when coded to string).");

ABSL_FLAG(uint64_t, log_writer_max_write_size_threshold, 1 << 17,  // 128k
          "Threshold after which we will release the lock on the queue. This "
          "ensures a bit more fairness on high-contention workloads");

namespace witnesskvs::log {

// Number of bytes we use to store the size + checksum
constexpr int kSizeChecksumBytes = 12;

extern const uint64_t kIdxSentinelValue;

LogWriter::LogWriter(std::string dir, std::string prefix)
    : LogWriter(dir, prefix, nullptr) {}

LogWriter::LogWriter(std::string dir, std::string prefix,
                     std::function<uint64_t(const Log::Message&)> idxfn)
    : dir_(std::move(dir)),
      prefix_(std::move(prefix)),
      skip_flush_(false),
      idxfn_(std::move(idxfn)),
      total_entries_output_(0),
      entries_count_(0),
      max_idx_(kIdxSentinelValue),
      min_idx_(kIdxSentinelValue),
      rotation_enabled_(true) {
  CheckWriteDir(dir_);
  CheckPrefix(prefix_);
  absl::MutexLock l(&lock_);
  InitFileWriterLocked();
}

LogWriter::LogWriter(std::string filename, int64_t micros,
                     std::function<uint64_t(const Log::Message&)> idxfn)
    : skip_flush_(false),
      idxfn_(std::move(idxfn)),
      entries_count_(0),
      total_entries_output_(0),
      max_idx_(kIdxSentinelValue),
      min_idx_(kIdxSentinelValue),
      rotation_enabled_(false) {
  absl::MutexLock l(&lock_);
  InitFileWriterWithFileLocked(std::move(filename), micros);
}

LogWriter::~LogWriter() {
  CHECK(file_writer_ != nullptr);
  LOG(INFO) << "LogWriter::~LogWriter: total logged: " << total_entries_output_;
  LOG(INFO) << "LogWriter::~LogWriter: filenames: "
            << absl::StrJoin(filenames_, ",");
  // If we have an open file, we need to write out the max_idx.
  if (file_writer_->bytes_received() > 0 && max_idx_ != kIdxSentinelValue) {
    std::filesystem::path prev_path =
        std::filesystem::path(file_writer_->filename());
    file_writer_.release();
    CHECK_EQ(file_writer_, nullptr);
    VLOG(2) << "Writing min_idx: " << min_idx_ << " max_idx: " << max_idx_;
    FileWriter::WriteHeader(prev_path, GetIdxCord(min_idx_, max_idx_));
  }
}

void LogWriter::Write(absl::string_view str) {
  absl::Cord cord;
  const size_t size = str.size();
  cord.Append(byte_str(static_cast<uint64_t>(size)));
  uint32_t crc32_res = static_cast<uint32_t>(absl::ComputeCrc32c(str));
  VLOG(1) << "crc32_res: " << crc32_res;
  cord.Append(byte_str(crc32_res));
  cord.Append(str);
  VLOG(2) << "LogWriter::Write size bytes: "
          << byte_str(static_cast<uint64_t>(str.size()));
  VLOG(2) << "LogWriter::Write crc32_res: " << crc32_res;
  VLOG(2) << "LogWriter::Write str length: " << str.size();
  file_writer_->Write(cord);
  return;
}

void LogWriter::InitFileWriterLocked() {
  lock_.AssertHeld();

  const int64_t micros = absl::ToUnixMicros(absl::Now());
  const std::string filename = absl::StrCat(
      dir_, std::string(1, std::filesystem::path::preferred_separator), prefix_,
      ".", micros);
  InitFileWriterWithFileLocked(filename, micros);
}

void LogWriter::InitFileWriterWithFileLocked(const std::string& filename,
                                             const uint64_t micros) {
  file_writer_ = std::make_unique<FileWriter>(filename);
  filenames_.push_back(filename);
  Log::Header header;
  header.set_timestamp_micros(micros);
  header.set_prefix(prefix_);
  header.set_id(micros);
  std::string header_str;
  header.SerializeToString(&header_str);

  // We will update the max index at the end. For now set it to a temporary
  // value.
  file_writer_->Write(GetIdxCord(kIdxSentinelValue, kIdxSentinelValue));
  Write(header_str);
  VLOG(1) << "LogWriter::InitFileWriterLocked header bytes: "
          << file_writer_->bytes_received();
}

void LogWriter::MaybeForceRotate() {
  absl::MutexLock l(&lock_);
  if (min_idx_ == kIdxSentinelValue) {
    // It seems we haven't written anything otherwise this would be not the
    // sentinel.
    LOG(INFO) << "Not rotating an empty log file.";
    return;
  }
  RotateLocked();
}

void LogWriter::RotateLocked() {
  lock_.AssertHeld();
  // Rotate the log:
  std::filesystem::path prev_path =
      std::filesystem::path(file_writer_->filename());
  InitFileWriterLocked();
  FileWriter::WriteHeader(prev_path, GetIdxCord(min_idx_, max_idx_));
  if (rotate_callback_) {
    // Let those who need to know that we rotated (e.g. LogTruncator who tracks
    // the file available to truncate).
    rotate_callback_(prev_path.string(), min_idx_, max_idx_);
  }
  min_idx_ = kIdxSentinelValue;
  max_idx_ = kIdxSentinelValue;
}

void LogWriter::MaybeRotate(uint64_t size_est) {
  VLOG(1) << "LogWriter::MaybeRotate size_est: " << size_est;
  VLOG(1) << "LogWriter::MaybeRotate size_est + bytes_received: "
          << size_est + file_writer_->bytes_received();
  CHECK_NE(file_writer_, nullptr);

  if (!rotation_enabled_) {
    VLOG(1) << "LogWriter::MaybeRotate skipping rotation since disabled.";
    return;
  }

  // We may need to rotate the log if the file is too big
  // to contain the next message.
  if (size_est + file_writer_->bytes_received() >
      absl::GetFlag(FLAGS_log_writer_max_file_size)) {
    if (!entries_count_) {
      LOG(FATAL) << "LogWriter: msg  (" << size_est << ") + base log size ("
                 << file_writer_->bytes_received()
                 << ") bigger than FLAGS_log_writer_max_file_size: "
                 << absl::GetFlag(FLAGS_log_writer_max_file_size);
    }

    // Should rotate the log:
    RotateLocked();
  }
}

std::vector<std::shared_ptr<LogWriter::ListEntry>> LogWriter::GetNextMsgBatch(
    std::weak_ptr<ListEntry> entry) {
  // Get the waiting messages off the list up to a certain size, outside of I/O
  // ops. After a call to this function, other threads should be able to
  // continue to add messages to the list.
  //
  // It's quite possible due to queuing at the lock and concurrent threads
  // that we will write a thread's message out while it's
  // blocked on lock_, we check this first and will return early.
  std::vector<std::shared_ptr<ListEntry>> msgs;
  {
    absl::MutexLock wl(&write_list_lock_);
    uint64_t size = 0;

    // Always get our own entry off the list first.
    if (std::shared_ptr<ListEntry> shared = entry.lock(); shared != nullptr) {
      size += shared->msg->size() + kSizeChecksumBytes;
      msgs.push_back(shared);
      write_list_.erase(shared->it);
    } else {
      // Our entry has already been written. Just return. Let another thread
      // handle the waiting messages. Since each thread is at minimum
      // responsible for its own message we are guaranteed that each message
      // enqueued will be written as long as threads aren't killed
      // mid-operation, but if that happens, it's probably a FATAL event
      // regardless.
      return msgs;
    }
    while (!write_list_.empty() &&
           size < absl::GetFlag(FLAGS_log_writer_max_write_size_threshold)) {
      std::shared_ptr<ListEntry> list_entry = write_list_.front();
      size += list_entry->msg->size() + kSizeChecksumBytes;
      msgs.push_back(std::move(list_entry));
      write_list_.pop_front();
    }
  }
  return msgs;
}

absl::Status LogWriter::Log(const Log::Message& msg) {
  VLOG(2) << "LogWriter::Log msg(1): " << msg.DebugString();
  // Append the message to the queue first.
  std::weak_ptr<ListEntry> my_entry;
  {
    std::string msg_str;
    msg.AppendToString(&msg_str);
    if (msg_str.size() > absl::GetFlag(FLAGS_log_writer_max_msg_size)) {
      return absl::OutOfRangeError(absl::StrFormat(
          "msg size when serialized '%d' is greater than max '%d'.",
          msg_str.size(), absl::GetFlag(FLAGS_log_writer_max_msg_size)));
    }
    VLOG(2) << "LogWriter::Log msg(2): " << msg.DebugString();
    const uint64_t idx = idxfn_ ? idxfn_(msg) : kIdxSentinelValue;
    VLOG(2) << "found idx: " << idx;
    std::shared_ptr<ListEntry> entry = std::make_shared<ListEntry>(
        std::make_unique<std::string>(std::move(msg_str)), idx);
    my_entry = entry;
    absl::MutexLock wl(&write_list_lock_);
    // unique_ptr used here as a hack to get around copies when going in and out
    // of the queue.
    write_list_.push_back(entry);
    entry->it = --(write_list_.end());
  }

  // Read all pending messages off the queue and write them.
  //
  // NOTE: because this will in some cases result in an I/O operation,
  // other threads waiting to write could pile up here.
  //
  // However because the message list is under an independent lock, they
  // can safely enqueue their message and just wait to attempt to write it
  // (although it's possible that another earlier thread may batch write it out
  // for us).
  {
    absl::MutexLock l(&lock_);
    if (file_writer_ == nullptr) {
      InitFileWriterLocked();
    }

    // Loop over any waiting messages and write them, as long as messages
    // are still on the queue
    while (true) {
      // Get messages off the queue.
      std::vector<std::shared_ptr<ListEntry>> msgs = GetNextMsgBatch(my_entry);
      if (msgs.empty()) {
        break;
      }

      // This is where we write the log messages to disk.
      //
      // Do the I/O operation outside of queue access.
      for (std::shared_ptr<ListEntry>& entry : msgs) {
        // Size estimate of the next log entry (includes length (8) and checksum
        // (4))
        CHECK_NE(entry->msg, nullptr);
        const uint64_t size_est = entry->msg->length() + kSizeChecksumBytes;
        MaybeRotate(size_est);
        Write(*entry->msg);
        ++entries_count_;
        ++total_entries_output_;

        // We will write max_idx_ out later as the first few bytes of the file
        // (plus a checksum).
        if (entry->idx != kIdxSentinelValue) {
          if (max_idx_ == kIdxSentinelValue || max_idx_ < entry->idx) {
            max_idx_ = entry->idx;
            VLOG(2) << "max_idx: " << max_idx_;
          }
          if (min_idx_ == kIdxSentinelValue || min_idx_ > entry->idx) {
            min_idx_ = entry->idx;
            VLOG(2) << "min_idx: " << min_idx_;
          }
        }
      }
    }
    // This will be the operation that could stall a bit.
    // So we do this after all writes have been done.
    if (!skip_flush_) {
      file_writer_->Flush();
    }
  }
  return absl::OkStatus();
}

std::string LogWriter::filename() const {
  // Though file_writer_ returns a reference, we need to return
  // a copy since after the lock, the reference could go away.
  absl::MutexLock l(&lock_);
  if (file_writer_ == nullptr) {
    return "";
  }
  return file_writer_->filename();
}

std::vector<std::string> LogWriter::filenames() const {
  absl::MutexLock l(&lock_);
  return filenames_;
}

void LogWriter::SetSkipFlush(bool skip_flush) {
  absl::MutexLock l(&lock_);
  skip_flush_ = skip_flush;
}

bool LogWriter::skip_flush() const {
  absl::MutexLock l(&lock_);
  return skip_flush_;
}

void LogWriter::RegisterRotateCallback(
    absl::AnyInvocable<void(std::string, uint64_t, uint64_t)> fn) {
  absl::MutexLock l(&lock_);
  rotate_callback_ = std::move(fn);
}

int64_t LogWriter::total_entries_output() const {
  absl::MutexLock l(&lock_);
  return total_entries_output_;
}

}  // namespace witnesskvs::log
