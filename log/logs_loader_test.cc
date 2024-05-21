#include "logs_loader.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>

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

std::string GetTempPrefix() {
  absl::Time now = absl::Now();
  std::string filename = "logs_loader_";
  filename.append(absl::StrCat(absl::ToUnixMicros(now)));
  filename.append("_test");
  return filename;
}

TEST(LogsLoader, Basic) {
  std::vector<std::string> cleanup_files;
  std::string prefix = GetTempPrefix();
  Log::Message log_message;
  log_message.mutable_paxos()->set_round(4);
  log_message.mutable_paxos()->set_proposal_id(9);
  log_message.mutable_paxos()->set_value("test1234");
  {
    LogWriter log_writer("/tmp", prefix);
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    LogsLoader logs_loader("/tmp", prefix);
    auto it = logs_loader.begin();
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message));
    it++;  // Test postfix notation.
    EXPECT_EQ(it, logs_loader.end());
    // Reset iterator.
    it = logs_loader.begin();
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message));
    ++it;  // Test prefix notation.
    EXPECT_EQ(it, logs_loader.end());
    int count = 0;
    for (auto& log_msg : logs_loader) {
      count++;
      EXPECT_THAT(log_msg, EqualsProto(log_message));
    }
    EXPECT_EQ(count, 1);
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, MultiTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = GetTempPrefix();
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_round(4);
  log_message1.mutable_paxos()->set_proposal_id(9);
  log_message1.mutable_paxos()->set_value("test1234");
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_round(5);
  log_message2.mutable_paxos()->set_proposal_id(2);
  log_message2.mutable_paxos()->set_value("test12344");
  {
    LogWriter log_writer("/tmp", prefix);
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    EXPECT_THAT(log_writer.Log(log_message2), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    LogsLoader logs_loader("/tmp", prefix);
    auto it = logs_loader.begin();
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message1));
    ++it;
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message2));
    it++;
    EXPECT_EQ(it, logs_loader.end());
    int count = 0;
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      count++;
      msgs.push_back(log_msg);
    }
    EXPECT_EQ(count, 2);
    EXPECT_THAT(msgs, ElementsAre(EqualsProto(log_message1),
                                  EqualsProto(log_message2)));
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, MultiFileTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = GetTempPrefix();
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_round(4);
  log_message1.mutable_paxos()->set_proposal_id(9);
  log_message1.mutable_paxos()->set_value("test1234");
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_round(5);
  log_message2.mutable_paxos()->set_proposal_id(2);
  log_message2.mutable_paxos()->set_value("test12344");
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_round(6);
  log_message3.mutable_paxos()->set_proposal_id(4);
  log_message3.mutable_paxos()->set_value("test1234124");
  {
    LogWriter log_writer("/tmp", prefix);
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    LogWriter log_writer("/tmp", prefix);
    LOG(INFO) << "second log message";
    EXPECT_THAT(log_writer.Log(log_message2), IsOk());
    LOG(INFO) << "third log message";
    EXPECT_THAT(log_writer.Log(log_message3), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    for (const auto& filename : log_writer.filenames()) {
      cleanup_files.push_back(filename);
    }
  }
  {
    LogsLoader logs_loader("/tmp", prefix);
    auto it = logs_loader.begin();
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message1));
    ++it;
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message2));
    it++;
    ASSERT_NE(it, logs_loader.end());
    int count = 0;
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      count++;
      msgs.push_back(log_msg);
    }
    EXPECT_EQ(count, 3);
    EXPECT_THAT(
        msgs, ElementsAre(EqualsProto(log_message1), EqualsProto(log_message2),
                          EqualsProto(log_message3)));
  }
  ASSERT_THAT(Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, SortingTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = GetTempPrefix();
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_round(4);
  log_message1.mutable_paxos()->set_proposal_id(9);
  log_message1.mutable_paxos()->set_value("test1234");
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_round(5);
  log_message2.mutable_paxos()->set_proposal_id(2);
  log_message2.mutable_paxos()->set_value("test12344");
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_round(6);
  log_message3.mutable_paxos()->set_proposal_id(4);
  log_message3.mutable_paxos()->set_value("test1234124");
  {
    LogWriter log_writer("/tmp", prefix);
    LOG(INFO) << "second log message";
    EXPECT_THAT(log_writer.Log(log_message3), IsOk());
    LOG(INFO) << "third log message";
    EXPECT_THAT(log_writer.Log(log_message2), IsOk());
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    for (const auto& filename : log_writer.filenames()) {
      cleanup_files.push_back(filename);
    }
  }
  {
    // Unsorted should return 3, 2, 1 in terms of message order.
    LogsLoader logs_loader("/tmp", prefix);
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      msgs.push_back(log_msg);
    }
    EXPECT_THAT(
        msgs, ElementsAre(EqualsProto(log_message3), EqualsProto(log_message2),
                          EqualsProto(log_message1)));
  }
  {
    // Try a logs loader that sorts by round.
    LogsLoader logs_loader("/tmp", prefix, [](const Log::Message& a, const Log::Message& b) {
      return a.paxos().round() < b.paxos().round();
    });
    auto it = logs_loader.begin();
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message1));
    ++it;
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message2));
    it++;
    ASSERT_NE(it, logs_loader.end());
    int count = 0;
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      count++;
      msgs.push_back(log_msg);
    }
    EXPECT_EQ(count, 3);
    EXPECT_THAT(
        msgs, ElementsAre(EqualsProto(log_message1), EqualsProto(log_message2),
                          EqualsProto(log_message3)));
  }
}

}  // namespace
}  // namespace witnesskvs::log
