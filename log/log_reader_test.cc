#include "log_reader.h"

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
#include "log_writer.h"
#include "tests/protobuf_matchers.h"
#include "tests/test_macros.h"

using ::protobuf_matchers::EqualsProto;
using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::UnorderedElementsAre;

MATCHER(IsError, "") { return (!arg.ok()); }

namespace witnesskvs::log {
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

TEST(LogReaderTest, Basic) {
  std::vector<std::string> cleanup_files;
  std::string filename;
  Log::Message log_message;
  log_message.mutable_paxos()->set_round(4);
  log_message.mutable_paxos()->set_proposal_id(9);
  log_message.mutable_paxos()->set_value("test1234");
  {
    LogWriter log_writer("/tmp", "log_reader_test");
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
    filename = log_writer.filename();
  }
  {
    LogReader log_reader(filename);
    auto it = log_reader.begin();
    ASSERT_NE(it, log_reader.end());
    EXPECT_THAT(*it, EqualsProto(log_message));
    it++;  // Test postfix notation.
    EXPECT_EQ(it, log_reader.end());
    // Reset iterator.
    it = log_reader.begin();
    ASSERT_NE(it, log_reader.end());
    EXPECT_THAT(*it, EqualsProto(log_message));
    ++it;  // Test prefix notation.
    EXPECT_EQ(it, log_reader.end());
    int count = 0;
    for (auto& log_msg : log_reader) {
      count++;
      EXPECT_THAT(log_msg, EqualsProto(log_message));
    }
    EXPECT_EQ(count, 1);
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogReaderTest, BlankMessage) {
  std::vector<std::string> cleanup_files;
  std::string filename;
  Log::Message log_message;
  {
    LogWriter log_writer("/tmp", "log_reader_test");
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
    filename = log_writer.filename();
  }
  {
    LogReader log_reader(filename);
    auto it = log_reader.begin();
    ASSERT_NE(it, log_reader.end());
    EXPECT_THAT(*it, EqualsProto(log_message));
    it++;  // Test postfix notation.
    EXPECT_EQ(it, log_reader.end());
    // Reset iterator.
    it = log_reader.begin();
    ASSERT_NE(it, log_reader.end());
    EXPECT_THAT(*it, EqualsProto(log_message));
    ++it;  // Test prefix notation.
    EXPECT_EQ(it, log_reader.end());
    int count = 0;
    for (auto& log_msg : log_reader) {
      count++;
      EXPECT_THAT(log_msg, EqualsProto(log_message));
    }
    EXPECT_EQ(count, 1);
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogReaderTest, MultiTest) {
  std::vector<std::string> cleanup_files;
  std::string filename;
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_round(4);
  log_message1.mutable_paxos()->set_proposal_id(9);
  log_message1.mutable_paxos()->set_value("test1234");
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_round(5);
  log_message2.mutable_paxos()->set_proposal_id(2);
  log_message2.mutable_paxos()->set_value("test12344");
  {
    LogWriter log_writer("/tmp", "log_reader_test");
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    EXPECT_THAT(log_writer.Log(log_message2), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
    filename = log_writer.filename();
  }
  {
    LogReader log_reader(filename);
    auto it = log_reader.begin();
    ASSERT_NE(it, log_reader.end());
    EXPECT_THAT(*it, EqualsProto(log_message1));
    ++it;
    ASSERT_NE(it, log_reader.end());
    EXPECT_THAT(*it, EqualsProto(log_message2));
    it++;
    EXPECT_EQ(it, log_reader.end());
    int count = 0;
    std::vector<Log::Message> msgs;
    for (auto& log_msg : log_reader) {
      count++;
      msgs.push_back(log_msg);
    }
    EXPECT_EQ(count, 2);
    EXPECT_THAT(msgs, ElementsAre(EqualsProto(log_message1),
                                  EqualsProto(log_message2)));
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

}  // namespace
}  // namespace witnesskvs::log
