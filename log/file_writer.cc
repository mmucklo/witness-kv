#include "file_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <filesystem>
#include <memory>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"

constexpr int BLOCK_SIZE
    = 4096;  // TODO(mmucklo): maybe make this flag-tunable.
constexpr int BLOCK_SIZE
    = 4096;  // TODO(mmucklo): maybe make this flag-tunable.

void verifyFilename( std::string filename )
{
void verifyFilename( std::string filename )
{
  // Should be a new file in an exisiting writeable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  // TODO(mmucklo): maybe convert these into LOG(FATAL)s so more information
  // about the state can be outputted?
  CHECK( !std::filesystem::exists( std::filesystem::status( filename ) ) )
      << "file should not exist already.";
  std::filesystem::path path { filename };
  CHECK( path.has_parent_path() ) << "file should have a parent path.";
  std::filesystem::file_status path_status
      = std::filesystem::status( path.parent_path() );
  CHECK( std::filesystem::is_directory( path_status ) )
      << "parent path should be a directory.";
  // TODO(mmucklo): maybe convert these into LOG(FATAL)s so more information
  // about the state can be outputted?
  CHECK( !std::filesystem::exists( std::filesystem::status( filename ) ) )
      << "file should not exist already.";
  std::filesystem::path path { filename };
  CHECK( path.has_parent_path() ) << "file should have a parent path.";
  std::filesystem::file_status path_status
      = std::filesystem::status( path.parent_path() );
  CHECK( std::filesystem::is_directory( path_status ) )
      << "parent path should be a directory.";
  std::filesystem::perms perms = path_status.permissions();
  CHECK( ( perms & std::filesystem::perms::owner_write )
         == std::filesystem::perms::owner_write )
      << "parent path should be writable.";
  CHECK( ( perms & std::filesystem::perms::owner_write )
         == std::filesystem::perms::owner_write )
      << "parent path should be writable.";
}

FileWriter::FileWriter( std::string filename )
    : filename_( std::move( filename ) ), buffer_size_( 0 ), bytes_written_( 0 )
{
FileWriter::FileWriter( std::string filename )
    : filename_( std::move( filename ) ), buffer_size_( 0 ), bytes_written_( 0 )
{
  // This may crash if we have an invalid filename.
  verifyFilename( filename_ );
  verifyFilename( filename_ );

  // Use low-level I/O since we need to call fsync or fdatasync.
  fd_ = open( filename_.c_str(), O_APPEND | O_CREAT | O_WRONLY,
              S_IRUSR | S_IWUSR );
  if ( fd_ == -1 ) {
    LOG( FATAL ) << "Could not open file descriptor for " << filename_
                 << " errno: " << errno << " " << strerror( errno );
  fd_ = open( filename_.c_str(), O_APPEND | O_CREAT | O_WRONLY,
              S_IRUSR | S_IWUSR );
  if ( fd_ == -1 ) {
    LOG( FATAL ) << "Could not open file descriptor for " << filename_
                 << " errno: " << errno << " " << strerror( errno );
  }

  // Initialize the buffer
  // TODO(mmucklo): do we need to initialize this to be filled with "\0"?
  buffer_ = std::make_unique<char[]>( BLOCK_SIZE );
  buffer_ = std::make_unique<char[]>( BLOCK_SIZE );

  // Alloc MUST succeed;
  CHECK_NE( buffer_, nullptr );
  CHECK_NE( buffer_, nullptr );
}

FileWriter::~FileWriter()
{
  if ( buffer_size_ > 0 ) { Flush(); }
  if ( close( fd_ ) == -1 ) {
    LOG( FATAL ) << "Error closing fd: " << fd_ << " for filename " << filename_
                 << " errno: " << errno << " " << strerror( errno );
FileWriter::~FileWriter()
{
  if ( buffer_size_ > 0 ) { Flush(); }
  if ( close( fd_ ) == -1 ) {
    LOG( FATAL ) << "Error closing fd: " << fd_ << " for filename " << filename_
                 << " errno: " << errno << " " << strerror( errno );
  }
}

void FileWriter::Write( const absl::Cord& msg )
void FileWriter::Write( const absl::Cord& msg )
{
  // Loop through all the chunks in the cord and buffer them out.
  for ( absl::string_view chunk : msg.Chunks() ) {
  for ( absl::string_view chunk : msg.Chunks() ) {
    int chunk_idx = 0;
    const int chunk_size = chunk.size();
    // Write the largest part of the chunk to the buffer as possible.
    while ( chunk_idx < chunk_size ) {
    while ( chunk_idx < chunk_size ) {
      int remaining = BLOCK_SIZE - buffer_size_;
      int chunk_remaining = chunk_size - chunk_idx;  // off by one error?
      if ( chunk_remaining <= remaining ) {
      int chunk_remaining = chunk_size - chunk_idx;  // off by one error?
      if ( chunk_remaining <= remaining ) {
        // copy entire chunk into buffer.
        chunk.copy( buffer_.get() + buffer_size_, chunk_remaining, chunk_idx );
        chunk.copy( buffer_.get() + buffer_size_, chunk_remaining, chunk_idx );
        chunk_idx += chunk_remaining;
        buffer_size_ += chunk_remaining;
      } else {
        chunk.copy( buffer_.get() + buffer_size_, remaining, chunk_idx );
        chunk.copy( buffer_.get() + buffer_size_, remaining, chunk_idx );
        chunk_idx += remaining;
        buffer_size_ += remaining;
      }

      // We should never overwrite the buffer - if so, it's certainly a
      // crashable event.
      CHECK_LE( buffer_size_, BLOCK_SIZE );
      // We should never overwrite the buffer - if so, it's certainly a
      // crashable event.
      CHECK_LE( buffer_size_, BLOCK_SIZE );

      // Now we may want to write the buffer if it is full.
      if ( buffer_size_ == BLOCK_SIZE ) { WriteBuffer(); }
      if ( buffer_size_ == BLOCK_SIZE ) { WriteBuffer(); }
    }
  }

  // If buffer_size_ was greater, we should have written above.
  CHECK_LT( buffer_size_, BLOCK_SIZE );
  CHECK_LT( buffer_size_, BLOCK_SIZE );
}

void FileWriter::WriteBuffer()
{
  if ( buffer_size_ == 0 ) { return; }
void FileWriter::WriteBuffer()
{
  if ( buffer_size_ == 0 ) { return; }

  ssize_t res = write( fd_, buffer_.get(), buffer_size_ );
  if ( res == -1 ) {
    LOG( FATAL ) << "Error writing chunk of Cord to file, errno: " << errno
                 << ": " << strerror( errno ) << ", filename: " << filename_;
  ssize_t res = write( fd_, buffer_.get(), buffer_size_ );
  if ( res == -1 ) {
    LOG( FATAL ) << "Error writing chunk of Cord to file, errno: " << errno
                 << ": " << strerror( errno ) << ", filename: " << filename_;
  }
  bytes_written_ += res;
  if ( res < buffer_size_ ) {
    LOG( FATAL ) << "Error writing chunk of Cord to file, size written: " << res
                 << ", size expected: " << buffer_size_;
  if ( res < buffer_size_ ) {
    LOG( FATAL ) << "Error writing chunk of Cord to file, size written: " << res
                 << ", size expected: " << buffer_size_;
  }
  buffer_size_ = 0;
}

void FileWriter::Flush()
{
  WriteBuffer();

  // On some systems fsync() is better - this is assuming a modern linux kernel
  // where fdatasync works as intended.
  // TODO(mmucklo): check kernel version - maybe make a #define / compiler flag
  // or a passable cli flag.
  if ( fdatasync( fd_ ) == -1 ) {
    LOG( ERROR ) << "fdatasync returned -1, errno: " << errno << ": "
                 << strerror( errno ) << ", filename: " << filename_;
  // On some systems fsync() is better - this is assuming a modern linux kernel
  // where fdatasync works as intended.
  // TODO(mmucklo): check kernel version - maybe make a #define / compiler flag
  // or a passable cli flag.
  if ( fdatasync( fd_ ) == -1 ) {
    LOG( ERROR ) << "fdatasync returned -1, errno: " << errno << ": "
                 << strerror( errno ) << ", filename: " << filename_;
  }
}
