#include "log_writer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"
#include "log.pb.h"
#include "tests/test_macros.h"

ABSL_DECLARE_FLAG(uint64_t, log_writer_max_file_size);
ABSL_DECLARE_FLAG(uint64_t, log_writer_max_msg_size);

using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::Not;
MATCHER(IsError, "") { return (!arg.ok()); }

namespace witnesskv::log {
namespace {

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

TEST(LogWriterTest, Basic) {
  std::vector<std::string> cleanup_files;
  {
    LogWriter log_writer("/tmp", "log_writer_test");
    Log::Message log_message;
    log_message.mutable_paxos()->set_round(4);
    log_message.mutable_paxos()->set_proposal_id(9);
    log_message.mutable_paxos()->set_value("test1234");
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogWriterTest, TooBig) {
  std::vector<std::string> cleanup_files;
  {
    absl::SetFlag(&FLAGS_log_writer_max_msg_size, 1);
    LogWriter log_writer("/tmp", "log_writer_test");
    Log::Message log_message;
    log_message.mutable_paxos()->set_round(4);
    log_message.mutable_paxos()->set_proposal_id(9);
    log_message.mutable_paxos()->set_value("test1234");
    absl::Status status = log_writer.Log(log_message);
    EXPECT_THAT(status, IsError());
    EXPECT_THAT(status.ToString(),
                AllOf(HasSubstr("is greater than max"), HasSubstr("")));
    EXPECT_EQ(log_writer.filename(), "");
    cleanup_files = log_writer.filenames();
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogWriterTest, Rotation) {
  std::vector<std::string> cleanup_files;
  {
    // Make the max filesize small.
    absl::SetFlag(&FLAGS_log_writer_max_file_size, 100);
    LogWriter log_writer("/tmp", "log_writer_test_rotation");
    Log::Message log_message;
    log_message.mutable_paxos()->set_round(4);
    log_message.mutable_paxos()->set_proposal_id(9);
    log_message.mutable_paxos()->set_value("test1234");
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    std::string filename = log_writer.filename();
    log_message.mutable_paxos()->set_round(5);
    log_message.mutable_paxos()->set_proposal_id(10);
    log_message.mutable_paxos()->set_value("12345678901234567890");
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    // Log should have rotated.
    EXPECT_NE(log_writer.filename(), filename);
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}
}  // namespace
}  // namespace witnesskvs::log
