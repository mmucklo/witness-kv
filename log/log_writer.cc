#include "log_writer.h"

#include <filesystem>

#include "absl/log/log.h"
#include "log.pb.h"

constexpr int64_t MAX_FILE_SIZE = 1 << 30; // 1 GiB 

void checkDir( std::string dir )
{
  // Should be an existing writable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  // TODO(mmucklo): maybe convert these into LOG(FATAL)s so more information
  // about the state can be outputted?
  std::filesystem::file_status dir_status = std::filesystem::status( dir );
  if ( !std::filesystem::exists( dir_status ) ) {
    LOG( FATAL ) << "LogWriter: dir '" << dir << "' should exist.";
  }
  if ( !std::filesystem::is_directory( dir_status ) ) {
    LOG( FATAL ) << "LogWriter: dir '" << dir << "' should be a directory.";
  }
  std::filesystem::perms perms = dir_status.permissions();
  if ( ( perms & std::filesystem::perms::owner_write )
       != std::filesystem::perms::owner_write ) {
    LOG( FATAL ) << "LogWriter: dir '" << dir << "' should be writable.";
  }
}

LogWriter::LogWriter( std::string dir ) : dir_( dir ) { checkDir( dir_ ); }

void LogWriter::InitFileWriter() {
    lock_.AssertHeld();

}

void LogWriter::Log( const Log::Message& msg )
{
  {
    absl::MutexLock l( &write_queue_lock_ );
    write_queue_.push( msg );
  }
  {
    absl::MutexLock l( &lock_ );
    
  }
}