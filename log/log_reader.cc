#include "log_reader.h"

#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "absl/crc/crc32c.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "byte_conversion.h"
#include "third_party/mediapipe/status_macros.h"

ABSL_DECLARE_FLAG(uint64_t, log_writer_max_msg_size);

namespace witnesskvs::log {

// TODO(mmucklo): is there a better way to do this?
std::string CheckFile(std::string filename) {
  // Should be an existing readable file.
  // Do a bunch of tests to make sure, otherwise we crash.
  std::filesystem::file_status file_status = std::filesystem::status(filename);
  if (!std::filesystem::exists(file_status)) {
    LOG(FATAL) << "LogReader: '" << filename << "' should exist.";
  }
  if (!std::filesystem::is_regular_file(file_status)) {
    LOG(FATAL) << "LogWriter: file '" << filename
               << "' should be a regular file.";
  }
  std::filesystem::perms perms = file_status.permissions();
  if ((perms & std::filesystem::perms::owner_read) !=
      std::filesystem::perms::owner_read) {
    LOG(FATAL) << "LogWriter: file '" << filename << "' should be readable.";
  }
  return filename;
}

LogReader::LogReader(std::string filename)
    : filename_(std::move(filename)),
      f_(nullptr),
      pos_header_(-1),
      pos_(0),
      last_pos_(0) {
  CheckFile(filename_);
  absl::MutexLock l(&lock_);
  f_ = std::fopen(filename_.c_str(), "rb");
  CHECK(f_ != nullptr) << filename_
                       << " not openable: " << std::strerror(errno);
}

LogReader::~LogReader() {
  if (f_ != nullptr) {
    if (std::fclose(f_) == EOF) {
      LOG(FATAL) << "Error closing fd: for filename " << filename_
                 << " errno: " << errno << " " << std::strerror(errno);
    }
  }
}

void LogReader::MaybeSeekLocked(long pos) {
  lock_.AssertHeld();
  if (pos != pos_) {
    CHECK_NE(f_, nullptr);
    std::fseek(f_, pos, SEEK_SET);
    pos_ = pos;
  }
}

absl::StatusOr<uint64_t> LogReader::ReadUInt64Locked() {
  lock_.AssertHeld();
  std::vector<unsigned char> size_buf(8);
  std::size_t bytes = std::fread(&size_buf[0], sizeof(unsigned char), 8, f_);
  if (bytes != 8) {
    return absl::DataLossError(
        absl::StrFormat("Not able to read size of header, expected 8 bytes, "
                        "instead only read: %d bytes",
                        bytes));
  }
  return fromBytes<uint64_t, std::endian::little>(size_buf);
}

absl::StatusOr<uint32_t> LogReader::ReadCRC32Locked() {
  lock_.AssertHeld();
  std::vector<unsigned char> crc32_buf(4);
  size_t bytes = std::fread(&crc32_buf[0], sizeof(unsigned char), 4, f_);
  if (bytes != 4) {
    return absl::DataLossError(absl::StrFormat(
        "Not able to read crc32 of header, instead only read: %d bytes",
        bytes));
  }
  return fromBytes<uint32_t, std::endian::little>(crc32_buf);
}

absl::StatusOr<std::unique_ptr<char[]>> LogReader::ReadBufferLocked(
    const uint64_t size, const uint32_t crc32_val) {
  lock_.AssertHeld();
  std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size);
  size_t bytes = std::fread(buffer.get(), sizeof(char), size, f_);
  if (bytes != size) {
    return absl::DataLossError(
        absl::StrFormat("Not able to read msg of header, expected %d bytes, "
                        "instead only read: %d bytes",
                        size, bytes));
  }

  uint32_t crc32_buffer = static_cast<uint32_t>(
      absl::ComputeCrc32c(absl::string_view(buffer.get(), bytes)));
  if (crc32_val != crc32_buffer) {
    return absl::DataLossError(
        absl::StrFormat("Not crc32 invalid, expected %04x, instead got %04x",
                        crc32_val, crc32_buffer));
  }
  return buffer;
}

absl::StatusOr<Log::Message> LogReader::ReadNextMessage(long& pos) {
  absl::MutexLock l(&lock_);
  MaybeSeekLocked(pos);
  absl::StatusOr<Log::Message> msg_or = NextLocked();
  pos = std::ftell(f_);
  return msg_or;
}

absl::StatusOr<uint64_t> LogReader::ReadIdxLocked() {
  ASSIGN_OR_RETURN(uint64_t idx, ReadUInt64Locked());
  ASSIGN_OR_RETURN(uint32_t crc32_idx, ReadCRC32Locked());
  uint32_t crc32_idx_computed =
      static_cast<uint32_t>(absl::ComputeCrc32c(absl::StrCat(idx)));

  if (crc32_idx != crc32_idx_computed) {
    return absl::DataLossError(
        absl::StrFormat("idx{%d} crc32 {%d} not the same as computed {%d}.",
                        idx, crc32_idx, crc32_idx_computed));
  }
  return idx;
}

absl::StatusOr<long> LogReader::ReadHeader() {
  absl::MutexLock l(&lock_);

  VLOG(1) << "ReadHeader";
  // TODO(mmucklo): maybe header_valid_ should store a status so we don't
  // re-read an invalid header in a loop.
  if (pos_header_ != -1) {
    return pos_header_;
  }

  CHECK_NE(nullptr, f_);
  // Move to the beginning of the file.
  MaybeSeekLocked(0);

  // Read min and max idx
  ASSIGN_OR_RETURN(uint64_t min_idx, ReadIdxLocked());
  VLOG(2) << "LogReader::ReadHeader - min_idx" << min_idx;
  ASSIGN_OR_RETURN(uint64_t max_idx, ReadIdxLocked());
  VLOG(2) << "LogReader::ReadHeader - max_idx" << max_idx;

  // Read size
  ASSIGN_OR_RETURN(uint64_t size, ReadUInt64Locked());
  if (size > absl::GetFlag(FLAGS_log_writer_max_msg_size)) {
    return absl::OutOfRangeError(absl::StrFormat(
        "Size of header msg is out of range (%d bytes, when max is %d bytes)",
        size, absl::GetFlag(FLAGS_log_writer_max_msg_size)));
  }
  VLOG(2) << "header after size position: " << std::ftell(f_)
          << " size: " << size;
  CHECK_NE(size, 0);

  ASSIGN_OR_RETURN(uint32_t crc32, ReadCRC32Locked());
  VLOG(2) << "header after crc32 position: " << std::ftell(f_);
  ASSIGN_OR_RETURN(std::unique_ptr<char[]> buffer,
                   ReadBufferLocked(size, crc32));
  VLOG(2) << "header after buffer position: " << std::ftell(f_);
  if (!header_.ParseFromString(absl::string_view(buffer.get(), size))) {
    return absl::DataLossError(
        absl::StrFormat("Unable to ParseFromString the header."));
  }
  header_.set_min_idx(min_idx);
  header_.set_max_idx(max_idx);
  return (pos_ = pos_header_ = std::ftell(f_));
}

absl::StatusOr<Log::Header> LogReader::header() {
  RETURN_IF_ERROR(ReadHeader().status());

  absl::MutexLock l(&lock_);
  return header_;
}

LogReader::iterator::iterator(LogReader* lr) : log_reader(lr), pos(0) {
  reset();
}

void LogReader::iterator::reset() {
  absl::StatusOr<long> pos_or = log_reader->ReadHeader();
  if (!pos_or.ok()) {
    // Invalid header on this file.
    // TODO(mmucklo): maybe log.
    VLOG(1) << "Invalid header: " << pos_or.status().message();
    return;
  }
  pos = pos_or.value();
  next();
}

LogReader::iterator::iterator(LogReader* lr,
                              std::unique_ptr<Log::Message> sentinel)
    : log_reader(lr), pos(0), cur(std::move(sentinel)) {}

void LogReader::iterator::next() {
  VLOG(1) << "next";
  absl::StatusOr<Log::Message> msg_or = log_reader->ReadNextMessage(pos);
  if (msg_or.ok()) {
    VLOG(1) << "next ok";
    cur = std::make_unique<Log::Message>(std::move(msg_or.value()));
    return;
  }
  VLOG(1) << "next not ok" << msg_or.status().message();
  cur = nullptr;
  pos = 0;
}

absl::StatusOr<Log::Message> LogReader::next() {
  absl::MutexLock l(&lock_);
  return NextLocked();
}

absl::StatusOr<Log::Message> LogReader::NextLocked() {
  if (last_pos_ == pos_) {
    // Hack to reset the file pointer so we can continue to read off from the
    // last position if possible.
    MaybeSeekLocked(pos_ - 1);  // this moves pos_ back 1.
    MaybeSeekLocked(pos_ + 1);
  }
  last_pos_ = pos_;
  CHECK_NE(nullptr, f_);
  // Read size
  ASSIGN_OR_RETURN(uint64_t size, ReadUInt64Locked());
  if (size > absl::GetFlag(FLAGS_log_writer_max_msg_size)) {
    return absl::OutOfRangeError(absl::StrFormat(
        "Size of msg is out of range (%d bytes, when max is %d bytes)",
        size, absl::GetFlag(FLAGS_log_writer_max_msg_size)));
  }
  ASSIGN_OR_RETURN(uint32_t crc32, ReadCRC32Locked());
  if (size == 0) {
    // Just a blank message.
    return Log::Message();
  }
  ASSIGN_OR_RETURN(std::unique_ptr<char[]> buffer,
                   ReadBufferLocked(size, crc32));
  Log::Message msg;
  bool msg_valid = msg.ParseFromString(absl::string_view(buffer.get(), size));
  if (!msg_valid) {
    return absl::DataLossError(
        absl::StrFormat("Unable to ParseFromString the next msg."));
  }
  pos_ = std::ftell(f_);
  return msg;
}

}  // namespace witnesskvs::log
