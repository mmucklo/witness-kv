#ifndef TESTS_TEST_UTIL_H
#define TESTS_TEST_UTIL_H

#include <string>
#include <vector>

#include "absl/status/status.h"

namespace witnesskvs::test {

absl::Status Cleanup(std::vector<std::string> filenames);
std::string GetTempPrefix(std::string base_prefix);

}  // namespace witnesskvs::test

#endif
