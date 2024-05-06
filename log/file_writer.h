#ifndef LOG_FILE_WRITER
#define LOG_FILE_WRITER

#include "absl/cleanup/cleanup.h"
#include "absl/strings/cord.h"
#include <string>

/**
 * A single writer for a single file.
 * 
 * TODO(mmucklo): maybe record the size of the file we've written so far, so we can close it when done.
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

  void Write(const absl::Cord& msg);
  void Flush();

private:
  int fd_;
  std::string filename_;
  std::unique_ptr<char[]> buffer_;
  int buffer_size_;
};
#endif