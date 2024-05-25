#ifndef TESTS_TEST_UTIL_H
#define TESTS_TEST_UTIL_H

#include <string>
#include <vector>

#include "absl/status/status.h"

namespace witnesskv::test {

absl::Status Cleanup(std::vector<std::string> filenames);

}  // namespace witnesskvs::test

#endif