#include "test_util.h"

#include <filesystem>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"

// https://superuser.com/questions/45342/when-should-i-use-dev-shm-and-when-should-i-use-tmp
ABSL_FLAG(std::string, tests_test_util_temp_dir, ".",
          "Where to store temporary files during testing. Note it's "
          "recommended not to use /tmp for running log-related tests since "
          "/tmp is generally run on tmpfs which doesn't respect fsync.");
namespace witnesskvs::test {

absl::Status Cleanup(std::vector<std::string> filenames) {
  bool success = true;
  for (auto& filename : filenames) {
    success =
        success && std::filesystem::remove(std::filesystem::path(filename));
  }
  if (success) {
    return absl::OkStatus();
  }
  return absl::UnknownError(
      absl::StrCat("Could not delete files: ", absl::StrJoin(filenames, ",")));
}

void Cleanup(const std::string& directory, const std::string& prefix) {
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.is_regular_file() &&
        entry.path().filename().string().starts_with(prefix)) {
      std::filesystem::remove(entry.path());
    }
  }
}

std::string GetTempPrefix(std::string base_prefix) {
  absl::Time now = absl::Now();
  base_prefix.append(absl::StrCat(absl::ToUnixMicros(now)));
  base_prefix.append("_test");
  return base_prefix;
}

}  // namespace witnesskvs::test
