#include "logs_truncator.h"

#include <gmock/gmock.h>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "log.pb.h"
#include "log_util.h"
#include "log_writer.h"
#include "logs_loader.h"
#include "tests/test_macros.h"
#include "tests/test_util.h"

using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::Pair;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

namespace witnesskvs::log {
extern const uint64_t kIdxSentinelValue;

TEST(LogsTruncator, Basic) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_truncator_");
  {
    LogWriter log_writer("/tmp", prefix);
    for (int i = 0; i < 5; i++) {
      Log::Message log_message;
      log_message.mutable_paxos()->set_idx(i);
      log_message.mutable_paxos()->set_min_proposal(4);
      log_message.mutable_paxos()->set_accepted_proposal(9);
      log_message.mutable_paxos()->set_accepted_value("test1234");
      log_message.mutable_paxos()->set_is_chosen(true);
      ASSERT_THAT(log_writer.Log(log_message), IsOk());
    }
    cleanup_files = log_writer.filenames();
    ASSERT_THAT(cleanup_files, SizeIs(1));
  }
  uint64_t max_idx = kIdxSentinelValue;
  uint64_t min_idx = kIdxSentinelValue;
  std::vector<uint64_t> indexes;
  {
    LogsTruncator logs_truncator("/tmp", prefix, [](const Log::Message& msg) {
      return msg.paxos().idx();
    });
    absl::flat_hash_map<std::string, LogsTruncator::TruncationFileInfo>
        filename_max_idx = logs_truncator.filename_max_idx();
    EXPECT_THAT(filename_max_idx, SizeIs(1));
    for (const auto& [filename, file_info] : filename_max_idx) {
      EXPECT_EQ(filename, cleanup_files[0]);
      EXPECT_EQ(file_info.max_idx, 4);
      EXPECT_EQ(file_info.min_idx, 0);
    }

    logs_truncator.Truncate(3);
    absl::SleepFor(absl::Seconds(4));
    SortingLogsLoader loader("/tmp", prefix,
                             [](const Log::Message& a, const Log::Message& b) {
                               return a.paxos().idx() < b.paxos().idx();
                             });

    for (const Log::Message& msg : loader) {
      if (max_idx == kIdxSentinelValue) {
        max_idx = msg.paxos().idx();
        min_idx = msg.paxos().idx();
      } else {
        if (max_idx < msg.paxos().idx()) {
          max_idx = msg.paxos().idx();
        }
        if (min_idx > msg.paxos().idx()) {
          min_idx = msg.paxos().idx();
        }
      }
      indexes.push_back(msg.paxos().idx());
    }
  }
  EXPECT_EQ(min_idx, 3);
  EXPECT_EQ(max_idx, 4);
  EXPECT_THAT(indexes, ElementsAre(3, 4));
  CleanupFiles(cleanup_files);
}

TEST(LogsTruncator, MultiFile) {
  std::vector<std::string> cleanup_files;
  std::string first_file;
  std::string prefix = test::GetTempPrefix("logs_truncator_");
  {
    LogWriter log_writer("/tmp", prefix);
    for (int i = 0; i < 5; i++) {
      Log::Message log_message;
      log_message.mutable_paxos()->set_idx(i);
      log_message.mutable_paxos()->set_min_proposal(4);
      log_message.mutable_paxos()->set_accepted_proposal(9);
      log_message.mutable_paxos()->set_accepted_value("test1234");
      log_message.mutable_paxos()->set_is_chosen(true);
      ASSERT_THAT(log_writer.Log(log_message), IsOk());
    }
    first_file = log_writer.filename();
  }
  {
    LogWriter log_writer("/tmp", prefix);
    for (int i = 9; i < 15; i++) {
      Log::Message log_message;
      log_message.mutable_paxos()->set_idx(i);
      log_message.mutable_paxos()->set_min_proposal(4);
      log_message.mutable_paxos()->set_accepted_proposal(9);
      log_message.mutable_paxos()->set_accepted_value("test1234");
      log_message.mutable_paxos()->set_is_chosen(true);
      ASSERT_THAT(log_writer.Log(log_message), IsOk());
    }
    cleanup_files.push_back(log_writer.filename());
    ASSERT_THAT(cleanup_files, SizeIs(1));
  }

  uint64_t max_idx = kIdxSentinelValue;
  uint64_t min_idx = kIdxSentinelValue;
  std::vector<uint64_t> indexes;
  {
    LogsTruncator logs_truncator("/tmp", prefix, [](const Log::Message& msg) {
      return msg.paxos().idx();
    });
    absl::flat_hash_map<std::string, LogsTruncator::TruncationFileInfo>
        filename_max_idx = logs_truncator.filename_max_idx();
    EXPECT_THAT(filename_max_idx, SizeIs(2));
    EXPECT_EQ(filename_max_idx[first_file].min_idx, 0);
    EXPECT_EQ(filename_max_idx[first_file].max_idx, 4);
    EXPECT_EQ(filename_max_idx[cleanup_files[0]].min_idx, 9);
    EXPECT_EQ(filename_max_idx[cleanup_files[0]].max_idx, 14);

    logs_truncator.Truncate(11);
    absl::SleepFor(absl::Seconds(4));
    SortingLogsLoader loader("/tmp", prefix,
                             [](const Log::Message& a, const Log::Message& b) {
                               return a.paxos().idx() < b.paxos().idx();
                             });

    for (const Log::Message& msg : loader) {
      if (max_idx == kIdxSentinelValue) {
        max_idx = msg.paxos().idx();
        min_idx = msg.paxos().idx();
      } else {
        if (max_idx < msg.paxos().idx()) {
          max_idx = msg.paxos().idx();
        }
        if (min_idx > msg.paxos().idx()) {
          min_idx = msg.paxos().idx();
        }
      }
      indexes.push_back(msg.paxos().idx());
    }
  }
  EXPECT_EQ(min_idx, 11);
  EXPECT_EQ(max_idx, 14);
  EXPECT_THAT(indexes, ElementsAre(11, 12, 13, 14));
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(first_file)));
  CleanupFiles(cleanup_files);
}

}  // namespace witnesskvs::log
