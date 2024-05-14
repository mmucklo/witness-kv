#include "log_writer.h"

#include <cstdint>
#include <filesystem>
#include <type_traits>

#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "byte_conversion.h"
#include "crc32.h"
#include "log.pb.h"

// TODO(mmucklo): explore encrypted at rest
// TODO(mmucklo): compression
// Compression options:
//   1) Compress whole file after rotation
//   2) Compress each entry on the fly.
//   3) Compress both entries and whole file (would this even be good?)
//   seems like #1 would be best, but results in larger files on disk temporarily, then need a background file compressor
//   
// TODO(mmucklo): Sortable log files - swap prefix and suffix?

ABSL_FLAG(uint64_t, log_writer_max_file_size, 1 << 30,
          "Maximum file size for logs");

ABSL_FLAG(uint64_t, log_writer_max_msg_size, 1 << 20,
          "Maximum message size in bytes (when coded to string).");

namespace witnesskvs::log {

// TODO(mmucklo): is there a better way to do this?
void checkDir(std::string dir) {
  // Should be an existing writable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  std::filesystem::file_status dir_status = std::filesystem::status(dir);
  if (!std::filesystem::exists(dir_status)) {
    LOG(FATAL) << "LogWriter: dir '" << dir << "' should exist.";
  }
  if (!std::filesystem::is_directory(dir_status)) {
    LOG(FATAL) << "LogWriter: dir '" << dir << "' should be a directory.";
  }
  std::filesystem::perms perms = dir_status.permissions();
  if ((perms & std::filesystem::perms::owner_write) !=
      std::filesystem::perms::owner_write) {
    LOG(FATAL) << "LogWriter: dir '" << dir << "' should be writable.";
  }
}

void checkPrefix(std::string prefix) {
  // TODO: maybe check that the filename prefix is valid here (i.e. doesn't
  // contain any directory separators, or has only certain characters).
}

LogWriter::LogWriter(std::string dir, std::string prefix)
    : dir_(dir), prefix_(prefix) {
  checkDir(dir_);
  checkPrefix(prefix);
}

void LogWriter::Write(absl::string_view str) {
  absl::Cord cord;
  cord.Append(byte_str(static_cast<uint64_t>(str.size())));
  //  LOG(INFO) << "size: " << str.size() << " size_bytes_length: " <<
  //  size_bytes(str.size()).size();
  uint32_t crc32_res = crc32(str.data(), str.size());
  cord.Append(byte_str(crc32_res));
  cord.Append(str);
  //  LOG(INFO) << "crc32_res: " << crc32_res;
  //  LOG(INFO) << "crc32_length: " << crc32_bytes(crc32_res).size();
  file_writer_->Write(cord);
}

void LogWriter::InitFileWriterLocked() {
  lock_.AssertHeld();

  int64_t micros = absl::ToUnixMicros(absl::Now());
  std::string filename = absl::StrCat(
      dir_, std::string(1, std::filesystem::path::preferred_separator), prefix_,
      ".", micros);
  file_writer_ = std::make_unique<FileWriter>(filename);
  filenames_.push_back(filename);
  Log::Header header;
  header.set_timestamp_micros(micros);
  header.set_prefix(prefix_);
  header.set_idx(micros);
  std::string header_str;
  header.SerializeToString(&header_str);
  Write(header_str);
  VLOG(1) << "LogWriter::InitFileWriterLocked header bytes: "
          << file_writer_->bytes_received();
}

void LogWriter::MaybeRotate(uint64_t size_est) {
  VLOG(1) << "LogWriter::MaybeRotate size_est: " << size_est;
  VLOG(1) << "LogWriter::MaybeRotate size_est + bytes_received: "
          << size_est + file_writer_->bytes_received();
  CHECK_NE(file_writer_, nullptr);

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
    InitFileWriterLocked();
  }
}
std::vector<std::unique_ptr<std::string>> LogWriter::GetWriteQueueMsgs() {
  // Get the waiting messages off the queue, outside of I/O ops.
  // After this block, other threads should be able to continue
  // to add messages to the queue.
  //
  // It's quite possible due to queuing and concurrent threads
  // that we will write a threads messages out while it's
  // blocked on lock_.
  std::vector<std::unique_ptr<std::string>> msgs;
  {
    absl::MutexLock wl(&write_queue_lock_);
    while (!write_queue_.empty()) {
      msgs.push_back(std::move(write_queue_.front()));
      write_queue_.pop();
    }
  }
  return msgs;
}

absl::Status LogWriter::Log(const Log::Message& msg) {
  // Append the message to the queue first.
  {
    std::string msg_str;
    msg.AppendToString(&msg_str);
    if (msg_str.size() > absl::GetFlag(FLAGS_log_writer_max_msg_size)) {
      return absl::OutOfRangeError(absl::StrFormat("msg size when serialized '%d' is greater than max '%d'.", msg_str.size(), absl::GetFlag(FLAGS_log_writer_max_msg_size)));
    }
    absl::MutexLock wl(&write_queue_lock_);
    // unique_ptr used here as a hack to get around copies when going in and out
    // of the queue.
    write_queue_.push(std::make_unique<std::string>(std::move(msg_str)));
  }

  // Read all pending messages off the queue and write them.
  //
  // NOTE: because this blocks when others are writing, multiple
  // messages could get queued up and then written at a time.
  {
    absl::MutexLock l(&lock_);
    if (file_writer_ == nullptr) {
      InitFileWriterLocked();
    }

    // Loop over any waiting messages and write them, as long as messages
    // are still on the queue
    while (true) {
      // Get messages off the queue.
      // TODO(mmucklo): Put a bound on the number of messages to do at a time.
      // TODO(mmucklo): Make sure we do our own, however.
      std::vector<std::unique_ptr<std::string>> msgs = GetWriteQueueMsgs();
      if (msgs.empty()) {
        break;
      }

      // This is where we write the log messages to disk.
      //
      // Do the I/O operation outside of queue access.
      for (std::unique_ptr<std::string>& msg_str : msgs) {
        // Size estimate of the next log entry (includes length and checksum)
        CHECK_NE(msg_str, nullptr);
        const uint64_t size_est = msg_str->length() + 16;
        MaybeRotate(size_est);
        Write(*msg_str);
        ++entries_count_;
      }
    }
    // This will be the operation that could stall a bit.
    // So we do this after all writes have been done.
    file_writer_->Flush();
  }
  return absl::OkStatus();
}

std::string LogWriter::filename() const {
  // Though file_writer_ returns a reference, we need to return
  // a copy since after the lock, the reference could go away.
  absl::MutexLock l(&lock_);
  if (file_writer_ == nullptr) { return ""; }
  return file_writer_->filename();
}

std::vector<std::string> LogWriter::filenames() const {
  absl::MutexLock l(&lock_);
  return filenames_;
}

}  // namespace witnesskvs::log