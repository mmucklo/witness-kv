#include "rocksdb_container.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include "absl/flags/declare.h"
#include "rocksdb/db.h"
#include "tests/test_util.h"
#include "third_party/absl_local/test_macros.h"

ABSL_DECLARE_FLAG(std::string, tests_test_util_temp_dir);

namespace witnesskvs::server {
namespace {

TEST(GetShardId, Basic) {
  EXPECT_NE(GetShardId("a", 2), GetShardId("b", 2));
  char a = 0;
  char b = 1;
  std::string str1;
  str1 += a;
  std::string str2;
  str2 += b;
  EXPECT_NE(GetShardId(str1, 2), GetShardId(str2, 2));
  EXPECT_EQ(GetShardId(str1, 2), 0);
  EXPECT_EQ(GetShardId(str2, 2), 1);
  EXPECT_EQ(GetShardId(str2, 4), 2);
  EXPECT_EQ(GetShardId(str2, 64), 32);
  std::string str3;
  str3 += (char)3;
  EXPECT_EQ(GetShardId(str3, 4), 3);
  std::string str4;
  str4 += (char)2;
  EXPECT_EQ(GetShardId(str4, 4), 1);
  EXPECT_EQ(GetShardId(str2, 8), 4);
  std::string str5;
  str5 += (char)7;
  EXPECT_EQ(GetShardId(str5, 8), 7);
  str2 += (char)7;
  EXPECT_EQ(GetShardId(str2, 8), 7);
}

TEST(RocksDBContainer, Basic) {
  RocksDBContainer r(absl::GetFlag(FLAGS_tests_test_util_temp_dir), 4);
  std::string str1;
  str1 += (char)0;
  std::string str2;
  str2 += (char)1;
  std::string str3;
  str3 += (char)2;
  std::string str4;
  str4 += (char)3;

  rocksdb::Status status;
  
  status = r.GetDB(str1)->Put(
          rocksdb::WriteOptions(), "test", "testval1");
  EXPECT_TRUE(status.ok());
  status = r.GetDB(str2)->Put(
          rocksdb::WriteOptions(), "test", "testval2");
  EXPECT_TRUE(status.ok());
  status = r.GetDB(str3)->Put(
          rocksdb::WriteOptions(), "test", "testval3");
  EXPECT_TRUE(status.ok());
  status = r.GetDB(str4)->Put(
          rocksdb::WriteOptions(), "test", "testval4");
  EXPECT_TRUE(status.ok());

  std::string value;
  status =
      r.GetDB(str1)->Get(rocksdb::ReadOptions(), "test", &value);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(value, "testval1");
  status =
      r.GetDB(str2)->Get(rocksdb::ReadOptions(), "test", &value);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(value, "testval2");
  status =
      r.GetDB(str3)->Get(rocksdb::ReadOptions(), "test", &value);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(value, "testval3");
  status =
      r.GetDB(str4)->Get(rocksdb::ReadOptions(), "test", &value);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(value, "testval4");
}


}  // namespace
}  // namespace witnesskvs::server
