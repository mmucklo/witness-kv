#include "absl/base/internal/raw_logging.h"
#include "absl/base/log_severity.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "gtest/gtest.h"

void InternalLogFn(absl::LogSeverity severity, const char* file, int line,
                   const std::string& message) {
  // TODO: is there a better more absl way to do this?
  switch (severity) {
    case absl::LogSeverity::kError:
      LOG(ERROR).AtLocation(file, line) << message;
      break;
    case absl::LogSeverity::kWarning:
      LOG(WARNING).AtLocation(file, line) << message;
      break;
    case absl::LogSeverity::kFatal:
      LOG(FATAL).AtLocation(file, line) << message;
      break;
    case absl::LogSeverity::kInfo:
      LOG(INFO).AtLocation(file, line) << message;
      break;
    default:
      LOG(ERROR).AtLocation(file, line) << "(unknown severity)" << message;
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();
  absl::raw_log_internal::RegisterInternalLogFunction(InternalLogFn);

  return RUN_ALL_TESTS();
}
