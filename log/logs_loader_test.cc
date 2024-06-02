#include "logs_loader.h"

#include <gmock/gmock.h>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "log.pb.h"
#include "log_writer.h"
#include "tests/protobuf_matchers.h"
#include "tests/test_macros.h"
#include "tests/test_util.h"

using ::protobuf_matchers::EqualsProto;
using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::UnorderedElementsAre;

ABSL_DECLARE_FLAG(uint64_t, logs_loader_max_memory_for_sorting);
ABSL_DECLARE_FLAG(uint64_t, log_writer_max_msg_size);
ABSL_DECLARE_FLAG(uint64_t, log_writer_max_file_size);
ABSL_DECLARE_FLAG(std::string, tests_test_util_temp_dir);

MATCHER(IsError, "") { return (!arg.ok()); }

namespace witnesskvs::log {
class LogWriterTestPeer {
 public:
  LogWriterTestPeer() = delete;
  // Disable copy (and move) semantics.
  LogWriterTestPeer(const LogWriterTestPeer&) = delete;
  LogWriterTestPeer& operator=(const LogWriterTestPeer&) = delete;
  LogWriterTestPeer(std::string dir, std::string prefix)
      : log_writer_(dir, prefix) {
    absl::MutexLock l(&log_writer_.lock_);
    log_writer_.InitFileWriterLocked();
  }
  std::string filename() { return log_writer_.filename(); }

 private:
  LogWriter log_writer_;
};

namespace {

TEST(LogsLoader, Basic) {
  std::vector<std::string> cleanup_files;
  std::string prefix = witnesskvs::test::GetTempPrefix("logs_loader_");
  Log::Message log_message;
  log_message.mutable_paxos()->set_idx(0);
  log_message.mutable_paxos()->set_min_proposal(4);
  log_message.mutable_paxos()->set_accepted_proposal(9);
  log_message.mutable_paxos()->set_accepted_value("test1234");
  log_message.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix);
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, MultiTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(0);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(1);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    EXPECT_THAT(log_writer.Log(log_message2), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix);
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, MultiFileTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(0);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(1);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_idx(2);
  log_message3.mutable_paxos()->set_min_proposal(6);
  log_message3.mutable_paxos()->set_accepted_proposal(11);
  log_message3.mutable_paxos()->set_accepted_value("test1234124");
  log_message3.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
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
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix);
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, MultiFileTestWithBlank) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(0);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(1);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_idx(2);
  log_message3.mutable_paxos()->set_min_proposal(6);
  log_message3.mutable_paxos()->set_accepted_proposal(11);
  log_message3.mutable_paxos()->set_accepted_value("test1234124");
  log_message3.mutable_paxos()->set_is_chosen(true);
  {
    // Fake blank file.
    // TODO(mmucklo): Figure out a way to add a blank header file.
    std::string filename =
        absl::StrFormat("%s.%d", prefix, absl::ToUnixMicros(absl::Now()));
    std::FILE* f = fopen(filename.c_str(), "w+");
    std::fclose(f);
    cleanup_files.push_back(filename);
    absl::SleepFor(absl::Milliseconds(1));
  }
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message1), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    for (const auto& filename : log_writer.filenames()) {
      cleanup_files.push_back(filename);
    }
  }
  {
    // Another fake blank file.
    std::string filename =
        absl::StrFormat("%s.%d", prefix, absl::ToUnixMicros(absl::Now()));
    std::FILE* f = fopen(filename.c_str(), "w+");
    std::fclose(f);
    cleanup_files.push_back(filename);
    absl::SleepFor(absl::Milliseconds(1));
  }
  {
    // A blank file with a header only.
    LogWriterTestPeer log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                                 prefix);
    cleanup_files.push_back(log_writer.filename());
    absl::SleepFor(absl::Milliseconds(1));
  }
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
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
    // Fake blank file.
    // TODO(mmucklo): Figure out a way to add a blank header file.
    std::string filename =
        absl::StrFormat("%s.%d", prefix, absl::ToUnixMicros(absl::Now()));
    std::FILE* f = fopen(filename.c_str(), "w+");
    std::fclose(f);
    cleanup_files.push_back(filename);
  }
  {
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix);
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, SortingTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(0);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(1);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_idx(2);
  log_message3.mutable_paxos()->set_min_proposal(6);
  log_message3.mutable_paxos()->set_accepted_proposal(11);
  log_message3.mutable_paxos()->set_accepted_value("test1234124");
  log_message3.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
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
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix);
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      msgs.push_back(log_msg);
    }
    EXPECT_THAT(
        msgs, ElementsAre(EqualsProto(log_message3), EqualsProto(log_message2),
                          EqualsProto(log_message1)));
  }
  {
    // Try a logs loader that sorts by idx.
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix,
                           [](const Log::Message& a, const Log::Message& b) {
                             return a.paxos().idx() < b.paxos().idx();
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(LogsLoaderTest, SortingMultiFileTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(2);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(3);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_idx(4);
  log_message3.mutable_paxos()->set_min_proposal(6);
  log_message3.mutable_paxos()->set_accepted_proposal(11);
  log_message3.mutable_paxos()->set_accepted_value("test1234124");
  log_message3.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message4;
  Log::Message log_message5;
  log_message5.mutable_paxos()->set_idx(1);
  log_message5.mutable_paxos()->set_min_proposal(7);
  log_message5.mutable_paxos()->set_accepted_proposal(12);
  log_message5.mutable_paxos()->set_accepted_value("test1234124");
  log_message5.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message6;
  log_message6.mutable_paxos()->set_idx(0);
  log_message6.mutable_paxos()->set_min_proposal(8);
  log_message6.mutable_paxos()->set_accepted_proposal(13);
  log_message6.mutable_paxos()->set_accepted_value("test1234124");
  log_message6.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
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
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message4), IsOk());
    EXPECT_THAT(log_writer.Log(log_message5), IsOk());
    EXPECT_THAT(log_writer.Log(log_message6), IsOk());
    for (const auto& filename : log_writer.filenames()) {
      cleanup_files.push_back(filename);
    }
  }
  {
    // Unsorted should return 3, 2, 1, 4, 6, 5 in terms of message order.
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix);
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      msgs.push_back(log_msg);
    }
    EXPECT_THAT(
        msgs,
        ElementsAre(EqualsProto(log_message3), EqualsProto(log_message2),
                    EqualsProto(log_message1), EqualsProto(log_message4),
                    EqualsProto(log_message5), EqualsProto(log_message6)));
  }
  {
    // Try a logs loader that sorts by idx.
    LogsLoader logs_loader(absl::GetFlag(FLAGS_tests_test_util_temp_dir),
                           prefix,
                           [](const Log::Message& a, const Log::Message& b) {
                             return a.paxos().idx() < b.paxos().idx();
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
    EXPECT_EQ(count, 6);
    EXPECT_THAT(
        msgs,
        ElementsAre(EqualsProto(log_message1), EqualsProto(log_message2),
                    EqualsProto(log_message3), EqualsProto(log_message4),
                    EqualsProto(log_message6), EqualsProto(log_message5)));
  }
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(SortingLogsLoaderTest, Basic) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message;
  log_message.mutable_paxos()->set_idx(0);
  log_message.mutable_paxos()->set_min_proposal(4);
  log_message.mutable_paxos()->set_accepted_proposal(9);
  log_message.mutable_paxos()->set_accepted_value("test1234");
  log_message.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message), IsOk());
    EXPECT_GT(std::filesystem::file_size(log_writer.filename()), 1);
    cleanup_files = log_writer.filenames();
  }
  {
    SortingLogsLoader logs_loader(
        absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix,
        [](const Log::Message& a, const Log::Message& b) {
          return a.paxos().idx() < b.paxos().idx();
        });
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(SortingLogsLoaderTest, SortingTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(0);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(1);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_idx(2);
  log_message3.mutable_paxos()->set_min_proposal(6);
  log_message3.mutable_paxos()->set_accepted_proposal(11);
  log_message3.mutable_paxos()->set_accepted_value("test1234124");
  log_message3.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
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
    // Try a logs loader that sorts by idx.
    SortingLogsLoader logs_loader(
        absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix,
        [](const Log::Message& a, const Log::Message& b) {
          return a.paxos().idx() < b.paxos().idx();
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
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(SortingLogsLoaderTest, SortingMultiFileTest) {
  std::vector<std::string> cleanup_files;
  std::string prefix = test::GetTempPrefix("logs_loader_");
  Log::Message log_message1;
  log_message1.mutable_paxos()->set_idx(3);
  log_message1.mutable_paxos()->set_min_proposal(4);
  log_message1.mutable_paxos()->set_accepted_proposal(9);
  log_message1.mutable_paxos()->set_accepted_value("test1234");
  log_message1.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message2;
  log_message2.mutable_paxos()->set_idx(4);
  log_message2.mutable_paxos()->set_min_proposal(5);
  log_message2.mutable_paxos()->set_accepted_proposal(10);
  log_message2.mutable_paxos()->set_accepted_value("test12344");
  log_message2.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message3;
  log_message3.mutable_paxos()->set_idx(5);
  log_message3.mutable_paxos()->set_min_proposal(6);
  log_message3.mutable_paxos()->set_accepted_proposal(11);
  log_message3.mutable_paxos()->set_accepted_value("test1234124");
  log_message3.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message4;
  Log::Message log_message5;
  log_message5.mutable_paxos()->set_idx(2);
  log_message5.mutable_paxos()->set_min_proposal(7);
  log_message5.mutable_paxos()->set_accepted_proposal(12);
  log_message5.mutable_paxos()->set_accepted_value("test1234124");
  log_message5.mutable_paxos()->set_is_chosen(true);
  Log::Message log_message6;
  log_message6.mutable_paxos()->set_idx(1);
  log_message6.mutable_paxos()->set_min_proposal(8);
  log_message6.mutable_paxos()->set_accepted_proposal(13);
  log_message6.mutable_paxos()->set_accepted_value("test1234124");
  log_message6.mutable_paxos()->set_is_chosen(true);
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
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
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    EXPECT_THAT(log_writer.Log(log_message4), IsOk());
    EXPECT_THAT(log_writer.Log(log_message5), IsOk());
    EXPECT_THAT(log_writer.Log(log_message6), IsOk());
    for (const auto& filename : log_writer.filenames()) {
      cleanup_files.push_back(filename);
    }
  }
  {
    // Try a logs loader that sorts by idx.
    SortingLogsLoader logs_loader(
        absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix,
        [](const Log::Message& a, const Log::Message& b) {
          return a.paxos().idx() < b.paxos().idx();
        });
    auto it = logs_loader.begin();
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message4));
    ++it;
    ASSERT_NE(it, logs_loader.end());
    EXPECT_THAT(*it, EqualsProto(log_message6));
    it++;
    ASSERT_NE(it, logs_loader.end());
    int count = 0;
    std::vector<Log::Message> msgs;
    for (auto& log_msg : logs_loader) {
      count++;
      msgs.push_back(log_msg);
    }
    EXPECT_EQ(count, 6);
    EXPECT_THAT(
        msgs,
        ElementsAre(EqualsProto(log_message4), EqualsProto(log_message6),
                    EqualsProto(log_message5), EqualsProto(log_message1),
                    EqualsProto(log_message2), EqualsProto(log_message3)));
  }
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

TEST(SortingLogsLoaderTest, Large) {
  Log::Message msg_base;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(R"(
      paxos {
        idx: 5
        accepted_value: "testing1245"
      }
    )",
                                                            &msg_base));
  std::vector<Log::Message> msgs;

  absl::BitGen gen;
  absl::flat_hash_set<uint32_t> seen;
  for (int i = 0; i < 1024; i++) {
    uint32_t idx = absl::Uniform(absl::IntervalClosed, gen, 1, 1 << 20);
    if (seen.insert(idx).second) {
      Log::Message msg(msg_base);
      msg.mutable_paxos()->set_idx(idx);
      msgs.push_back(msg);
    }
  }
  const std::string prefix = test::GetTempPrefix("logs_loader_");
  absl::SetFlag(&FLAGS_log_writer_max_file_size, 256);
  std::vector<std::string> cleanup_files;
  {
    LogWriter log_writer(absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix);
    for (size_t i = 0; i < msgs.size(); i++) {
      ASSERT_THAT(log_writer.Log(msgs[i]), IsOk()) << i;
    }
    cleanup_files = log_writer.filenames();
  }

  {
    absl::SetFlag(&FLAGS_logs_loader_max_memory_for_sorting,
                  absl::GetFlag(FLAGS_log_writer_max_msg_size) * 5);

    // Try a logs loader that sorts by idx.
    SortingLogsLoader logs_loader(
        absl::GetFlag(FLAGS_tests_test_util_temp_dir), prefix,
        [](const Log::Message& a, const Log::Message& b) {
          return a.paxos().idx() < b.paxos().idx();
        });
    int prev_idx = -1;
    for (const Log::Message& msg : logs_loader) {
      EXPECT_GT(msg.paxos().idx(), prev_idx)
          << prev_idx << " " << msg.paxos().idx();
      prev_idx = msg.paxos().idx();
    }
  }
  ASSERT_THAT(witnesskvs::test::Cleanup(cleanup_files), IsOk());
}

}  // namespace
}  // namespace witnesskvs::log
