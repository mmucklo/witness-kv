#ifndef LOG_LOG_WRITER_H
#define LOG_LOG_WRITER_H

#include <file_writer.h>

#include <cstdint>
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
  LogWriter(std::string dir, std::string prefix,
            std::function<uint64_t(const Log::Message&)> idxfn);

  // For file-swapping purposes, intialize a single, non-rotating LogRotating.
  LogWriter(std::string filename, int64_t micros);

  ~LogWriter();
  // Disable copy (and move) semantics.
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;

  // Logs msg, returns when sync'd.
  absl::Status Log(const Log::Message& msg);

  // Returns the current filename in use.
  std::string filename() const ABSL_LOCKS_EXCLUDED(lock_);

  // Returns the list of filenames written to, including the ones rotated.
  std::vector<std::string> filenames() const;

  void SetSkipFlush(bool skip_flush);
  bool skip_flush() const;

 private:
  struct ListEntry {
    std::list<std::shared_ptr<ListEntry>>::iterator it;
    std::unique_ptr<std::string> msg;
    uint64_t idx;
    explicit ListEntry(std::unique_ptr<std::string> m, uint64_t i)
        : msg(std::move(m)), idx(i) {}
  };

  // Initializes a new FileWriter.
  void InitFileWriterLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);
  void InitFileWriterWithFileLocked(const std::string& filename, uint64_t micros) ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Writes a raw str to the log, preceeding with size, and ending with a 32-bit
  // checksum.
  void Write(absl::string_view str) ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Writes the idx specified at the beginning of the file, with a checksum.
  void WriteIdx(uint64_t idx);

  // Maybe rotate the log file.
  void MaybeRotate(uint64_t size_est) ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Gets a batch of waiting messages off the list, popping the front of the
  // list until it's empty, or we've filled up to the maximum size to write at a
  // time.
  //
  // We read things this way as it's more efficient to batch-log all waiting
  // messages at once before flushing (syncing to disk). However we put a bound
  // on the size of data that we will batch at any one time via a flag. There's
  // therefore a possibility we leave some messages on the list for the next
  // write.
  //
  // Nevertheless we use the weak_ptr entry to ensure that we at least return a
  // vector containing that element so that we can be sure we've logged our own
  // message even if we leave other messages still waiting on the list.
  //
  // The reason we expect that messages will queue up is that we serialize
  // writes out to the log file. These writes in order to be durable call a sync
  // to the filesystem which means some manner of file I/O may happen and delay
  // for a period of time.
  //
  // During this delay other messages may queue up wanting to be written. It's
  // very inefficient just to then write out each waiting message one at a time
  // then call fsync(), it's better to write them out in batches so that we one
  // do one fsync per batch.
  //
  // This mechanism effectively batches the waiting messages on the list.
  std::vector<std::shared_ptr<ListEntry>> GetNextMsgBatch(
      std::weak_ptr<ListEntry> entry);

  absl::Mutex write_list_lock_;  // Only locks write queue access.
  mutable absl::Mutex lock_;     // Main lock.
  std::string dir_;
  std::string prefix_;
  bool skip_flush_ ABSL_GUARDED_BY(lock_);
  std::list<std::shared_ptr<ListEntry>> write_list_
      ABSL_GUARDED_BY(write_list_lock_);
  std::unique_ptr<FileWriter> file_writer_ ABSL_GUARDED_BY(lock_)
      ABSL_ACQUIRED_BEFORE(write_list_lock_);
  int64_t entries_count_ ABSL_GUARDED_BY(
      lock_);  // Number of entries written to current file_writer_
  std::vector<std::string> filenames_
      ABSL_GUARDED_BY(lock_);  // List of files written to.
  std::function<uint64_t(const Log::Message&)> idxfn_;  //
  uint64_t max_idx_;
  uint64_t min_idx_;
  const bool rotation_enabled_;
  friend LogWriterTestPeer;
};

}  // namespace witnesskvs::log
#endif
