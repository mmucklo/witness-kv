#ifndef LOG_LOG_WRITER_H
#define LOG_LOG_WRITER_H

#include <file_writer.h>

#include <memory>
#include <queue>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "log.pb.h"

class LogWriter {
 public:
  LogWriter() = delete;
  // Create a LogWriter with directory and filename prefix as specified.
  LogWriter(std::string dir, std::string prefix);
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;

  // Logs msg, returns when sync'd.
  void Log(Log::Message msg);

 private:
  // Initializes a new FileWriter.
  void InitFileWriter() ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Writes a raw str to the log, preceeding with size, and ending with a 32-bit
  // checksum.
  void Write(absl::string_view str) ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  absl::Mutex write_queue_lock_;  // Only locks write queue access.
  absl::Mutex lock_;              // Main lock.
  std::string dir_;
  std::string prefix_;
  std::queue<std::unique_ptr<Log::Message>> write_queue_ ABSL_GUARDED_BY(
      write_queue_lock_);  // TODO: maybe switch to a concurrent data structure.
  std::unique_ptr<FileWriter> file_writer_ ABSL_GUARDED_BY(lock_)
      ABSL_ACQUIRED_BEFORE(write_queue_lock_);
};

#endif
