#ifndef LOG_FILE_WRITER_H
#define LOG_FILE_WRITER_H

#include <sys/types.h>

#include <filesystem>
#include <memory>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/cord.h"

namespace witnesskvs::log {

/**
 * A single writer for a single file. It will output in file system block size
 * chunks.
 *
 * The chunk size is presently a constant in file_writer.cc but can become a
 * flag if necessary.
 * A single writer for a single file. It will output in file system block size
 * chunks.
 *
 * The chunk size is presently a constant in file_writer.cc but can become a
 * flag if necessary.
 */
class FileWriter {
 public:
 public:
  FileWriter(std::string filename);
  FileWriter() = delete;

  // Disable copy (and move) semantics.
  FileWriter(const FileWriter&) = delete;
  FileWriter& operator=(const FileWriter&) = delete;
  ~FileWriter();

  // TODO: maybe create a std::unique_ptr<> returning New method for storing
  // these things on the heap?

  // Writes the cord out, may buffer.
  void Write(const absl::Cord& msg);

  // Flushes all buffers to disk.
  void Flush();

  // The number of bytes received (includes bytes written to disk and bytes
  // still buffered).
  ssize_t bytes_received() { return bytes_received_; }

  // The number of bytes actually written to disk.
  ssize_t bytes_written() { return bytes_written_; }

  std::string& filename() { return filename_; }

 private:
  void InitialSync(const std::filesystem::path& path);
  void WriteBuffer();

  int fd_;
  std::string filename_;
  std::unique_ptr<char[]> buffer_;  // A buffer for contents we will output.
  int buffer_size_;                 // The current filled size of the buffer.
  const uint64_t buffer_size_max_;  // The size of the buffer when full.
  ssize_t bytes_written_;
  ssize_t bytes_received_;
};

}  // namespace witnesskvs::log
#endif
