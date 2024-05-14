#include "log_reader.h"

#include <bit>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <vector>

#include "byte_conversion.h"
#include "crc32.h"
#include "status_macros.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"

ABSL_DECLARE_FLAG(uint64_t, log_writer_max_msg_size);

namespace witnesskvs::log {

// TODO(mmucklo): is there a better way to do this?
std::string checkFile(std::string filename) {
  // Should be an existing readable file.
  // Do a bunch of tests to make sure, otherwise we crash.
  std::filesystem::file_status file_status = std::filesystem::status(filename);
  if (!std::filesystem::exists(file_status)) {
    LOG(FATAL) << "LogReader: '" << filename << "' should exist.";
  }
  if (!std::filesystem::is_regular_file(file_status)) {
    LOG(FATAL) << "LogWriter: file '" << filename << "' should be a regular file.";
  }
  std::filesystem::perms perms = file_status.permissions();
  if ((perms & std::filesystem::perms::owner_read) !=
      std::filesystem::perms::owner_read) {
    LOG(FATAL) << "LogWriter: file '" << filename << "' should be readable.";
  }
  return filename;
}

LogReader::LogReader(std::string filename) : filename_(std::move(filename)), f_(nullptr), pos_header_(-1), pos_(0) {
  absl::MutexLock l(&lock_);
  f_ = std::fopen(filename_.c_str(), "rb");
  CHECK(f_ != nullptr);
}

LogReader::~LogReader() {
    if (f_ != nullptr) {
        std::fclose(f_);
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

absl::StatusOr<uint64_t> LogReader::ReadSizeBytesLocked() {
  lock_.AssertHeld();
  std::vector<unsigned char> size_buf(8);
  std::size_t bytes = std::fread(&size_buf[0], sizeof(unsigned char), 8, f_);
  if (bytes != 8) {
    return absl::DataLossError(absl::StrFormat("Not able to read size of header, expected 8 bytes, instead only read: %d bytes", bytes));
  }
  uint64_t size = fromBytes<uint64_t, std::endian::little>(size_buf);
  if (size > absl::GetFlag(FLAGS_log_writer_max_msg_size)) {
    return absl::OutOfRangeError(absl::StrFormat("Size of header msg is out of range (%d bytes, when max is %d bytes)", bytes, absl::GetFlag(FLAGS_log_writer_max_msg_size)));
  }
  pos_ = std::ftell(f_);
  return size;
}

absl::StatusOr<uint32_t> LogReader::ReadCRC32Locked() {
  lock_.AssertHeld();
  std::vector<unsigned char> crc32_buf(4);
  size_t bytes = std::fread(&crc32_buf[0], sizeof(unsigned char), 4, f_);
  if (bytes != 4) {
    return absl::DataLossError(absl::StrFormat("Not able to read crc32 of header, instead only read: %d bytes", bytes));
  }
  pos_ = std::ftell(f_);
  return fromBytes<uint32_t, std::endian::little>(crc32_buf);
}

absl::StatusOr<std::unique_ptr<char[]>> LogReader::ReadBufferLocked(const uint64_t size, const uint32_t crc32_val) {
  lock_.AssertHeld();
  std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size);
  size_t bytes = std::fread(buffer.get(), sizeof(char), 4, f_);
  if (bytes != size) {
    return absl::DataLossError(absl::StrFormat("Not able to read msg of header, expected %d bytes, instead only read: %d bytes", size, bytes));
  }
  uint32_t crc32_buffer = crc32(buffer.get(), bytes);
  if (crc32_val != crc32_buffer) {
    return absl::DataLossError(absl::StrFormat("Not crc32 invalid, expected %04x, instead got %04x", crc32, crc32_buffer));
  }
  pos_ = std::ftell(f_);
  return buffer;
}

absl::StatusOr<Log::Message> LogReader::ReadNextMessage(long from_pos, long& pos) {
  absl::MutexLock l(&lock_);
  MaybeSeekLocked(from_pos);
  if (std::ftell(f_) == SEEK_END) {
    return absl::OutOfRangeError("Already at EOF.");
  }
  CHECK_NE(nullptr, f_);
  // Read size
  ASSIGN_OR_RETURN(uint64_t size, ReadSizeBytesLocked());
  CHECK_NE(size, 0);

  ASSIGN_OR_RETURN(uint32_t crc32, ReadCRC32Locked());
  ASSIGN_OR_RETURN(std::unique_ptr<char[]> buffer, ReadBufferLocked(size, crc32));
  Log::Message msg;
  bool msg_valid = msg.ParseFromString(absl::string_view(buffer.get(), size));
  if (!msg_valid) {
    return absl::DataLossError(absl::StrFormat("Unable to ParseFromString the next msg."));
  }
  pos = std::ftell(f_);
  return msg;
}

absl::StatusOr<long> LogReader::ReadHeader() {
  absl::MutexLock l(&lock_);

  // TODO(mmucklo): maybe header_valid_ should store a status so we don't
  // re-read an invalid header in a loop.
  if (pos_header_ != -1) { 
    // TODO(mmucklo): should this fail if called a second time?
    return pos_header_;
  }

  CHECK_NE(nullptr, f_);
  // Move to the beginning of the file.
  MaybeSeekLocked(0);

  // Read size
  ASSIGN_OR_RETURN(uint64_t size, ReadSizeBytesLocked());
  CHECK_NE(size, 0);

  ASSIGN_OR_RETURN(uint32_t crc32, ReadCRC32Locked());
  ASSIGN_OR_RETURN(std::unique_ptr<char[]> buffer, ReadBufferLocked(size, crc32));
  if (!header_.ParseFromString(absl::string_view(buffer.get(), size))) {
    return absl::DataLossError(absl::StrFormat("Unable to ParseFromString the header."));
  }
  return (pos_header_ = std::ftell(f_));
}

LogReader::Iterator::Iterator(LogReader* lr) : log_reader(lr), pos(0) {
  absl::StatusOr<long> pos_or = log_reader->ReadHeader();
  if (!pos_or.ok()) {
    // Invalid header on this file.
    // TODO(mmucklo): maybe log.
    return;
  }
  pos = pos_or.value();
  ReadNext();
}

LogReader::Iterator::Iterator(LogReader* lr, std::unique_ptr<Log::Message> sentinel) : log_reader(lr), pos(0), cur(std::move(sentinel)) {}

void LogReader::Iterator::ReadNext() {
  absl::StatusOr<Log::Message> msg_or = log_reader->ReadNextMessage(pos, pos);
  if (msg_or.ok()) { 
    cur = std::make_unique<Log::Message>(std::move(msg_or.value()));
    return;
  }
  cur = nullptr;
}

}  // namespace witnesskvs::log
