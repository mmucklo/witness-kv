#ifndef LOG_LOG_WRITER_H
#define LOG_LOG_WRITER_H

#include <file_writer.h>

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "log.pb.h"

namespace witnesskvs::log {

class LogWriterTestPeer;

class LogWriter {
 public:
  LogWriter() = delete;
  // Create a LogWriter with directory and filename prefix as specified.
  LogWriter(std::string dir, std::string prefix);

  // Disable copy (and move) semantics.
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;

  // Logs msg, returns when sync'd.
  absl::Status Log(const Log::Message& msg);

  // Returns the current filename in use.
  std::string filename() const ABSL_LOCKS_EXCLUDED(lock_);

  // Returns the list of filenames written to, including the ones rotated.
  std::vector<std::string> filenames() const;

 private:
  struct ListEntry {
    std::list<std::shared_ptr<ListEntry>>::iterator it;
    std::unique_ptr<std::string> msg;
    explicit ListEntry(std::unique_ptr<std::string> m) : msg(std::move(m)) {}
  };

  // Initializes a new FileWriter.
  void InitFileWriterLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Writes a raw str to the log, preceeding with size, and ending with a 32-bit
  // checksum.
  void Write(absl::string_view str) ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Maybe rotate the log file.
  void MaybeRotate(uint64_t size_est) ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Gets all messages off the queue, popping the queue until it's empty.
  std::vector<std::unique_ptr<std::string>> GetWriteQueueMsgs(
      std::weak_ptr<ListEntry> entry);

  absl::Mutex write_list_lock_;  // Only locks write queue access.
  mutable absl::Mutex lock_;      // Main lock.
  std::string dir_;
  std::string prefix_;
  std::list<std::shared_ptr<ListEntry>> write_list_
      ABSL_GUARDED_BY(write_list_lock_);
  std::unique_ptr<FileWriter> file_writer_ ABSL_GUARDED_BY(lock_)
      ABSL_ACQUIRED_BEFORE(write_list_lock_);
  int64_t entries_count_ ABSL_GUARDED_BY(
      lock_);  // Number of entries written to current file_writer_
  std::vector<std::string> filenames_
      ABSL_GUARDED_BY(lock_);  // List of files written to.

  friend LogWriterTestPeer;
};

}  // namespace witnesskvs::log
#endif
