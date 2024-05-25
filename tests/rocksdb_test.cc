#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <system_error>

#include "rocksdb/db.h"
#include "tests/test_macros.h"
#include "tests/test_util.h"

namespace witnesskvs::test {
namespace {

TEST(RocksDb, Basic) {
  rocksdb::DB *db;
  rocksdb::Options options;
  options.create_if_missing = true;
  std::string dirname = "/tmp/testrocksdb";
  rocksdb::Status status = rocksdb::DB::Open(options, dirname, &db);
  ASSERT_TRUE(status.ok());
  status = db->Put(rocksdb::WriteOptions(), "key1", "value1");
  EXPECT_TRUE(status.ok());
  std::string value;
  status = db->Get(rocksdb::ReadOptions(), "key1", &value);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(value, "value1");
  status = db->Delete(rocksdb::WriteOptions(), "key1");
  EXPECT_TRUE(status.ok());
  status = db->Get(rocksdb::ReadOptions(), "key1", &value);
  EXPECT_FALSE(status.ok());
  // TODO(mmucklo): verify Not Found status.
  delete db;

  std::error_code ec;
  ASSERT_GT(std::filesystem::remove_all(std::filesystem::path(dirname), ec), 0);
  ASSERT_FALSE(ec);
}

}  // namespace
}  // namespace witnesskv::test
