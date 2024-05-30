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
  }
  uint64_t max_idx = kIdxSentinelValue;
  uint64_t min_idx = kIdxSentinelValue;
  std::vector<uint64_t> indexes;
  {
    LogsTruncator logs_truncator("/tmp", prefix, [](const Log::Message& msg) {
      return msg.paxos().idx();
    });
    logs_truncator.Truncate(3);
    absl::SleepFor(absl::Seconds(5));
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

}  // namespace witnesskvs::log
