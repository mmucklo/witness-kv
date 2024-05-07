#ifndef LOG_FILE_WRITER
#define LOG_FILE_WRITER

#include "absl/cleanup/cleanup.h"
#include "absl/strings/cord.h"
#include <sys/types.h>
#include <string>

/**
 * A single writer for a single file. It will output in file system block size chunks.
 * 
 * The chunk size is presently a constant in file_writer.cc but can become a flag if necessary.
 */
class FileWriter
{
public:
  FileWriter( std::string filename );
  FileWriter() = delete;
  FileWriter( const FileWriter& ) = delete;
  FileWriter& operator=( const FileWriter& ) = delete;
  ~FileWriter();

  // TODO: maybe create a std::unique_ptr<> returning New method for storing these things on the heap?

  // Writes the cord out, may buffer.
  void Write(const absl::Cord& msg);

  // Flushes all buffers to disk.
  void Flush();

  ssize_t bytes_written() { return bytes_written_; }

private:
  void WriteBuffer();

  int fd_;
  std::string filename_;
  std::unique_ptr<char[]> buffer_; // A buffer for contents we will output.
  int buffer_size_; // The current filled size of the buffer.
  ssize_t bytes_written_;
};
#endif