#include "file_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"

constexpr int BLOCK_SIZE = 4096;

// TODO(mmucklo): maybe make this a bit bigger to encourage sequential writing,
// but not so big as to take up a significant portion of the available memory
// which we want to instead reserve for the KV store itself.
ABSL_FLAG(uint64_t, file_writer_buffer_size, BLOCK_SIZE,
          "Default buffer size for writing. It's suggested to make this a "
          "multiple of BLOCK_SIZE");

namespace witnesskvs::log {

constexpr mode_t kFileMode = S_IRUSR | S_IWUSR;

void VerifyFilename(const std::filesystem::path& path, bool exists = false) {
  // Should be a new file in an exisiting writeable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  // TODO(mmucklo): maybe convert these into LOG(FATAL)s so more information
  // about the state can be outputted?
  if (!exists) {
    CHECK(!std::filesystem::exists(std::filesystem::status(path)))
        << path << ": file should not exist already.";
  }
  CHECK(path.has_parent_path()) << path << ": file should have a parent path.";
  std::filesystem::file_status path_status =
      std::filesystem::status(path.parent_path());
  CHECK(std::filesystem::is_directory(path_status))
      << path << ": parent path should be a directory.";
  std::filesystem::perms perms = path_status.permissions();
  CHECK((perms & std::filesystem::perms::owner_write) ==
        std::filesystem::perms::owner_write)
      << path << ": parent path should be writable.";
  CHECK((perms & std::filesystem::perms::owner_write) ==
        std::filesystem::perms::owner_write)
      << path << ": parent path should be writable.";
}

FileWriter::FileWriter(std::string filename)
    : filename_(std::move(filename)),
      buffer_size_(0),
      buffer_size_max_(absl::GetFlag(FLAGS_file_writer_buffer_size)),
      bytes_written_(0),
      bytes_received_(0) {
  // This may crash if we have an invalid filename.
  std::filesystem::path path{filename_};
  VerifyFilename(path);

  // Use low-level I/O since we need to call fsync or fdatasync.
  // We could also consider using O_DIRECT | O_DSYNC or O_DIRECT | O_SYNC
  // There's a slight performance gain of not using those flags according to
  // this analysis:
  // https://www.percona.com/blog/fsync-performance-storage-devices/
  //
  // Some storage engines such as postgres allow different strategies to
  // be selected. This could be another option.
  //
  // TODO(mmucklo): Time permitting microbenchmark various sync strategies.
  fd_ = open(filename_.c_str(), O_APPEND | O_CREAT | O_WRONLY, kFileMode);
  if (fd_ == -1) {
    LOG(FATAL) << "Could not open file descriptor for " << filename_
               << " errno: " << errno << " " << std::strerror(errno);
  }

  // Sync out the file and directory entry so we can be sure to find the file
  // after writing to it.
  InitialSync(path);

  // Initialize the buffer
  // TODO(mmucklo): do we need to initialize this to be filled with "\0"?
  buffer_ = std::make_unique<char[]>(buffer_size_max_);

  // Alloc MUST succeed;
  CHECK_NE(buffer_, nullptr);
}

FileWriter::~FileWriter() {
  if (buffer_size_ > 0) {
    Flush();
  }

  if (close(fd_) == -1) {
    LOG(FATAL) << "Error closing fd: " << fd_ << " for filename " << filename_
               << " errno: " << errno << " " << std::strerror(errno);
  }
}

void FileWriter::InitialSync(const std::filesystem::path& path) {
  // Sync the file first.
  if (fsync(fd_) == -1) {
    LOG(FATAL) << "first fsync returned -1, errno: " << errno << ": "
               << std::strerror(errno) << ", filename: " << filename_;
  }
  const std::string dir = path.parent_path().string();
  SyncDir(dir);
}

void FileWriter::SyncDir(const std::string& dir) {
  int dirfd = open(dir.c_str(), O_DIRECTORY);
  if (dirfd == -1) {
    LOG(FATAL) << "Could not open file descriptor for " << dir
               << " errno: " << errno << " " << std::strerror(errno);
  }
  if (fsync(dirfd) == -1) {
    LOG(FATAL) << "fsync of directory returned -1, errno: " << errno << ": "
               << std::strerror(errno) << ", dir: " << dir;
  }
  if (close(dirfd) == -1) {
    LOG(FATAL) << "Error closing dirfd: " << dirfd << " for directory "
               << dir << " errno: " << errno << " "
               << std::strerror(errno);
  }
}

void FileWriter::Write(const absl::Cord& msg) {
  // Loop through all the chunks in the cord and buffer them out.
  for (absl::string_view chunk : msg.Chunks()) {
    int chunk_idx = 0;
    const int chunk_size = chunk.size();
    bytes_received_ += chunk_size;
    // Write the largest part of the chunk to the buffer as possible.
    while (chunk_idx < chunk_size) {
      int remaining = buffer_size_max_ - buffer_size_;
      int chunk_remaining = chunk_size - chunk_idx;  // off by one error?
      if (chunk_remaining <= remaining) {
        // copy entire chunk into buffer.
        chunk.copy(buffer_.get() + buffer_size_, chunk_remaining, chunk_idx);
        chunk_idx += chunk_remaining;
        buffer_size_ += chunk_remaining;
      } else {
        chunk.copy(buffer_.get() + buffer_size_, remaining, chunk_idx);
        chunk_idx += remaining;
        buffer_size_ += remaining;
      }

      // We should never overwrite the buffer - if so, it's certainly a
      // crashable event.
      CHECK_LE(buffer_size_, buffer_size_max_);

      // Now we may want to write the buffer if it is full.
      if (buffer_size_ == buffer_size_max_) {
        WriteBuffer();
      }
    }
  }

  // If buffer_size_ was greater, we should have written above.
  CHECK_LT(buffer_size_, buffer_size_max_);
}

void FileWriter::WriteBuffer() {
  if (buffer_size_ == 0) {
    return;
  }

  ssize_t res = write(fd_, buffer_.get(), buffer_size_);
  if (res == -1) {
    LOG(FATAL) << "Error writing chunk of Cord to file, errno: " << errno
               << ": " << std::strerror(errno) << ", filename: " << filename_;
  }
  bytes_written_ += res;
  if (res < buffer_size_) {
    LOG(FATAL) << "Error writing chunk of Cord to file, size written: " << res
               << ", size expected: " << buffer_size_;
  }
  buffer_size_ = 0;
}

void FileWriter::Flush() {
  WriteBuffer();

  // TODO(mmucklo): We need to fsync as every write should increase the file
  // size, therefore we need to write the file's metadata as well. However
  // a more efficient strategy might be to pre-allocate the file and then
  // use fdatasync as the file size wouldn't need to be updated on every
  // flush at that point.
  // (Source:
  // https://yoshinorimatsunobu.blogspot.com/2009/05/overwriting-is-much-faster-than_28.html))
  //
  // Also we could instead just use fdatasync here and "trust" that the
  // underlying operating system will write the metadata appropriately, but
  // given this would appear to be on every call due to the append-nature
  // of our log, just call fsync directly.
  //
  // TODO(mmucklo): microbenchmark difference in fsync vs fdatasync for
  // append-only files.
  //
  // TODO(mmucklo): We could also use a flag to select fsync vs fdatasync.
  if (fsync(fd_) == -1) {
    LOG(FATAL) << "fsync returned -1, errno: " << errno << ": "
               << std::strerror(errno) << ", filename: " << filename_;
  }
}

void FileWriter::WriteHeader(const std::filesystem::path& path,
                             absl::Cord header) {
  VerifyFilename(path, /*exists=*/true);
  std::string path_str = path.string();
  int fd = open(path_str.c_str(), O_WRONLY, kFileMode);
  if (fd == -1) {
    LOG(FATAL) << "Could not open file descriptor for " << path.string()
               << " errno: " << errno << " " << std::strerror(errno);
  }
  for (absl::string_view chunk : header.Chunks()) {
    ssize_t res = write(fd, chunk.data(), chunk.size());
    if (res != chunk.size()) {
      LOG(FATAL)
          << "FileWriter::WriteHeader - Could not write header chunk of size "
          << chunk.size() << " instead wrote " << res;
    }
  }
  if (fdatasync(fd) == -1) {
    LOG(FATAL) << "fdatasync returned -1, errno: " << errno << ": "
               << std::strerror(errno) << ", filename: " << path_str;
  }
  if (close(fd) == -1) {
    LOG(FATAL) << "Error closing fd: " << fd << " for filename " << path_str
               << " errno: " << errno << " " << std::strerror(errno);
  }
}

}  // namespace witnesskvs::log
