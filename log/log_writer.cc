#include "log_writer.h"

#include <filesystem>

#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "log.pb.h"

ABSL_FLAG(int64_t, log_writer_max_file_size, 1 << 30,
          "Maximum file size for logs");

void checkDir(std::string dir) {
  // Should be an existing writable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  // TODO(mmucklo): maybe convert these into LOG(FATAL)s so more information
  // about the state can be outputted?
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

LogWriter::LogWriter(std::string dir) : dir_(dir) { checkDir(dir_); }

void LogWriter::InitFileWriter() {
  lock_.AssertHeld();
  absl::Time now = absl::Now();
  std::string filename = "/tmp/file_writer_test.";
  filename.append(absl::StrCat(absl::ToUnixMicros(now)));
}

void LogWriter::Log(const Log::Message& msg) {
  {
    absl::MutexLock l(&write_queue_lock_);
    write_queue_.push(msg);
  }
  { absl::MutexLock l(&lock_); }
}