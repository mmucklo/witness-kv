#include "rocksdb_container.h"

#include <strings.h>

#include <bit>
#include <cstdint>
#include <string>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

namespace witnesskvs::server {

RocksDBContainer::RocksDBContainer(absl::string_view db_path, uint16_t shards)
    : shards_(shards) {
  CHECK_EQ(std::popcount(shards), 1) << shards << " not a power of 2";
  for (uint16_t i = 0; i < shards; i++) {
    rocksdb::Options options;
    options.create_if_missing = true;
    std::string path =
        absl::StrFormat("%s/kvs_server_shards%d_shard%d", db_path, shards, i);
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
    if (!status.ok()) {
      LOG(FATAL) << "[KVS]: Failed to open RocksDB: " << status.ToString();
    }
    Shard shard{.lower = i, .db = db};
    shard_set_.insert(shard);
  }
}

RocksDBContainer::~RocksDBContainer() {
  for (const Shard& shard : shard_set_) {
    delete shard.db;
  }
}

rocksdb::DB* RocksDBContainer::GetDB(const std::string& key) const {
  Shard shard_key{.lower = GetShardId(key, shards_)};
  auto it = shard_set_.lower_bound(shard_key);
  if (it == shard_set_.end()) {
    LOG(FATAL) << "empty shard set?";
  }
  return it->db;
}

// bit-reversed shard id.
uint16_t GetShardId(const std::string& key, uint16_t shards) {
  if (key.length() == 0 || shards == 1) {
    return 0;
  }

  int f = ffs(shards);
  CHECK_GT(f, 1);

  // uint64_t for future expansion.
  uint64_t shard_id = 0;
  char x;
  ;
  size_t round = 1;
  for (int i = 0; i < f - 1; i++) {
    if (i % 8 == 0) {
      x = key[key.length() - round];
      ++round;
    }
    shard_id = shard_id << 1;
    if (x & 0x01) {
      shard_id = shard_id | 0x01;
    }
    x = x >> 1;
  }
  return shard_id;
}

}  // namespace witnesskvs::server
