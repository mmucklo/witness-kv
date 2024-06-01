#ifndef LOG_LOG_TRUNCATOR_H
#define LOG_LOG_TRUNCATOR_H

#include <filesystem>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace witnesskvs::log {

// TODO(mmucklo): would be nice to unit test these functions.
// TODO(mmucklo): document all these helper functions.
void CheckWriteDir(const std::string& dir);
void CheckReadDir(absl::string_view dir);
void CheckPrefix(absl::string_view prefix);
absl::Cord GetIdxCord(uint64_t min_idx, uint64_t max_idx);
absl::StatusOr<std::vector<std::filesystem::path>> ReadDir(
    absl::string_view dir, absl::string_view prefix, bool cleanup = false,
    bool sort = true);
struct FileParts {
  std::string prefix;
  uint64_t micros;
};
absl::StatusOr<FileParts> ParseFilename(absl::string_view filename);
void ReplaceFile(std::string orig_filename, std::string new_filename);
void CleanupFiles(const std::vector<std::string>& files);

}  // namespace witnesskvs::log

#endif
