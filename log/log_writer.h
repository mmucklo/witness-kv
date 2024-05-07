#ifndef LOG_LOG_WRITER
#define LOG_LOG_WRITER

#include <file_writer.h>
#include <memory>
#include <queue>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

#include "log.pb.h"

class LogWriter {
public:
  LogWriter() = delete;
  LogWriter(std::string dir);
  LogWriter( const LogWriter& ) = delete;
  LogWriter& operator=( const LogWriter& ) = delete;

  // Logs msg, returns when sync'd.
  void Log(const Log::Message& msg);
private:
  // Initializes a new FileWriter.
  void InitFileWriter();

  absl::Mutex write_queue_lock_; // Only locks write queue access.
  absl::Mutex lock_;  // Main lock.
  std::string dir_;
  std::queue<Log::Message> write_queue_ ABSL_GUARDED_BY(write_queue_lock_); // TODO: maybe switch to a concurrent data structure.
  std::unique_ptr<FileWriter> file_writer_ ABSL_GUARDED_BY(lock_);
};

#endif
