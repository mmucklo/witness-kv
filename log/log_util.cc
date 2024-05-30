#include "log_util.h"

#include <filesystem>
#include <string>
#include <system_error>
#include <vector>

#include "byte_conversion.h"
#include "file_writer.h"

#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"
#include "util/status_macros.h"

namespace witnesskvs::log {

extern constexpr char kFilenamePrefix[] = "^[A-Za-z0-9_-]+$";
extern constexpr uint64_t kIdxSentinelValue =
    std::numeric_limits<uint64_t>::max();

void CheckReadDir(absl::string_view dir) {
  // Should be an existing readable, executable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  const std::filesystem::file_status dir_status =
      std::filesystem::status(std::string(dir));
  if (!std::filesystem::exists(dir_status)) {
    LOG(FATAL) << "LogsLoader: dir '" << dir << "' should exist.";
  }
  if (!std::filesystem::is_directory(dir_status)) {
    LOG(FATAL) << "LogsLoader: dir '" << dir << "' should be a directory.";
  }
  std::filesystem::perms perms = dir_status.permissions();
  if ((perms & std::filesystem::perms::owner_write) !=
          std::filesystem::perms::owner_write ||
      (perms & std::filesystem::perms::owner_exec) !=
          std::filesystem::perms::owner_exec) {
    LOG(FATAL) << "LogWriter: dir '" << dir
               << "' should be readable and executable.";
  }
}

void CheckPrefix(absl::string_view prefix) {
  if (!re2::RE2::FullMatch(prefix, kFilenamePrefix)) {
    LOG(FATAL) << "LogLoader: prefix should match " << kFilenamePrefix;
  }
}

absl::StatusOr<FileParts> ParseFilename(absl::string_view filename) {
  std::vector<absl::string_view> parts = absl::StrSplit(filename, ".");
  if (parts.size() != 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("LogsLoader: expect parts of filename to be splittable "
                     "in two, instead there are ",
                     parts.size(), " for ", filename));
  }
  FileParts file_parts{.prefix = std::string(parts[0])};
  std::string ext = std::string(parts[1]);
  if (!absl::SimpleAtoi(ext, &file_parts.micros)) {
    return absl::OutOfRangeError(absl::StrFormat(
        "file extension is not parsable as an uint64_t: %s", filename));
  }
  return file_parts;
}

// Reads the list of files under prefix from the directory.
// and optionally sorts them by their suffix (presumes suffix is a
// uint64).
absl::StatusOr<std::vector<std::filesystem::path>> ReadDir(
    absl::string_view dir, absl::string_view prefix, bool sort, bool cleanup) {
  const std::filesystem::path path{std::string(dir)};
  struct FileEntry {
    uint64_t ext_micros;
    std::filesystem::path path;
  };
  absl::flat_hash_map<std::string, FileEntry> entries;
  std::vector<std::string> cleanup_files;
  absl::flat_hash_map<std::string, FileEntry> truncated_entries;
  for (const auto& dir_entry : std::filesystem::directory_iterator{path}) {
    if (!dir_entry.is_regular_file()) {
      // TODO(mmucklo): maybe consider following symlinks or not?
      continue;
    }
    std::string filename = dir_entry.path().filename();
    if (!absl::StartsWith(filename, prefix)) {
      continue;
    }
    const std::filesystem::file_status file_status =
        std::filesystem::status(dir_entry.path());
    std::filesystem::perms perms = file_status.permissions();
    if ((perms & std::filesystem::perms::owner_read) !=
        std::filesystem::perms::owner_read) {
      return absl::PermissionDeniedError(
          absl::StrFormat("log file %s is not readable", filename));
    }
    ASSIGN_OR_RETURN(FileParts file_parts, ParseFilename(filename));
    if (file_parts.prefix != prefix) {
      // Either files to cleanup or we crashed during a truncation.
      if (!cleanup) {
        continue;
      }
      if (absl::StrCat(file_parts.prefix, "_truncated") == prefix) {
        truncated_entries.emplace(filename,
                                  FileEntry{.ext_micros = file_parts.micros,
                                            .path = dir_entry.path()});
      }
    } else {
      entries.emplace(filename, FileEntry{.ext_micros = file_parts.micros,
                                          .path = dir_entry.path()});
    }
  }

  if (cleanup && cleanup_files.size() > 0) {
    CleanupFiles(cleanup_files);
  }

  // From our truncation algorithm, if we crash mid-file swap, we may have
  // legit, but not swapped into place "_truncated" files hanging around.
  //
  // Any temporaries will get cleaned up separately as they have a different
  // prefix.
  if (truncated_entries.size() > 0) {
    for (const auto& [filename, file_entry] : truncated_entries) {
      std::string untruncated_filename =
          absl::StrCat(prefix, ".", file_entry.ext_micros);
      if (!entries.contains(untruncated_filename)) {
        entries.emplace(
            untruncated_filename,
            FileEntry{.ext_micros = file_entry.ext_micros,
                      .path = std::filesystem::path(untruncated_filename)});
      }
      ReplaceFile(untruncated_filename, filename);
    }
  }

  // Convert to something sortable.
  // TODO(mmucklo): technically we're done with other data-structures after this point.
  std::vector<FileEntry> entries_list;
  for (const auto& [_, file_entry] : entries) {
    entries_list.push_back(file_entry);
  }

  if (sort) {
    // Read the earliest file first. As long as we don't write parallel log
    // streams, the files themselves should have a happens-before relationship.
    // However given that we have the ability to have multiple threads writing
    // to the log file simulatenously, we still have the possibility for
    // unsorted entries to cross log file boundaries, this should be handled
    // by our external merge sort on the files themselves.
    std::sort(entries_list.begin(), entries_list.end(),
              [](const FileEntry& a, const FileEntry& b) {
                return a.ext_micros < b.ext_micros;
              });
  }
  std::vector<std::filesystem::path> ret;
  for (auto& entry : entries_list) {
    ret.push_back(std::move(entry.path));
  }
  return ret;
}

// TODO(mmucklo): is there a better way to do this?
void CheckWriteDir(const std::string& dir) {
  // Should be an existing writable directory.
  // Do a bunch of tests to make sure, otherwise we crash.
  const std::filesystem::file_status dir_status = std::filesystem::status(dir);
  if (!std::filesystem::exists(dir_status)) {
    LOG(FATAL) << "LogWriter: dir '" << dir << "' should exist.";
  }
  if (!std::filesystem::is_directory(dir_status)) {
    LOG(FATAL) << "LogWriter: dir '" << dir << "' should be a directory.";
  }
  std::filesystem::perms perms = dir_status.permissions();
  if ((perms & std::filesystem::perms::owner_write) !=
      std::filesystem::perms::owner_write) {
    LOG(FATAL) << "LogWriter: dir '" << dir << "' should be writable.";
  }
}

// Returns a cord containg an encoded idx plus checksum of the same.
absl::Cord GetIdxCord(uint64_t min_idx, uint64_t max_idx) {
  absl::Cord cord;
  for (uint64_t idx : {min_idx, max_idx}) {
    cord.Append(byte_str(idx));
    uint32_t crc32_res =
        static_cast<uint32_t>(absl::ComputeCrc32c(absl::StrCat(idx)));
    VLOG(1) << "WriteIdx::crc32_res: " << crc32_res;
    cord.Append(byte_str(crc32_res));
  }
  return cord;
}

void ReplaceFile(std::string orig_filename,
                 std::string new_filename) {
  // If a crash happens here, our loading mechanism will reconcile the two
  // files.
  std::filesystem::path new_path(new_filename);
  std::filesystem::path orig_path(orig_filename);
  if (std::filesystem::exists(orig_path)) {
    if (!std::filesystem::remove(orig_path)) {
      LOG(FATAL) << absl::StrCat(
          "LogTruncator::DoSingleFileTruncation: Can't remove: ", orig_filename,
          " ", std::strerror(errno));
    }
  }

  // Now rename the perm_filename
  std::error_code ec;
  std::filesystem::rename(new_path, orig_path, ec);
  if (ec) {
    LOG(FATAL) << absl::StrCat(
        "LogTruncator::DoSingleFileTruncation: Can't rename: ", new_filename,
        " to ", orig_filename, ": ", ec.message());
  }

  // Sync the directory.
  std::string dir = std::filesystem::path(orig_filename).parent_path().string();
  FileWriter::SyncDir(dir);
}

void CleanupFiles(const std::vector<std::string>& files) {
  for (const auto& filename : files) {
    if (!std::filesystem::remove(std::filesystem::path(filename))) {
      LOG(FATAL) << absl::StrCat("Can't cleanup: ", filename, " ",
                                 std::strerror(errno));
    }
  }
}

}  // namespace witnesskvs::log