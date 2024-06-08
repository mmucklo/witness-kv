#ifndef SERVER_ROCKSDB_CONTAINER_H
#define SERVER_ROCKSDB_CONTAINER_H

#include <cstdint>
#include <set>
#include <string>

#include "absl/strings/string_view.h"
#include "rocksdb/db.h"

namespace witnesskvs::server {

class RocksDBContainer {
  public:
    // Creates a RocksDBContainer with num shards.
    // Requirement: shards must be a power of 2.
    RocksDBContainer(absl::string_view db_path, uint16_t shards);
    ~RocksDBContainer();

    rocksdb::DB* GetDB(const std::string& key) const;

  private:
    struct Shard {
        uint16_t lower;
        rocksdb::DB* db;
        bool operator() (const Shard& a, const Shard& b) const { return a.lower < b.lower; }
    };

    // This is is initialized in the constructor, but never written to again, although
    // on destruction delete is called on all db pointers.
    std::set<Shard, Shard> shard_set_;

    const uint16_t shards_;
};

// Returns a bit-reversed shard-id.
//
// Uses bit-reversal to attempt a more even distribution of the key space.
uint16_t GetShardId(const std::string& key, uint16_t shards);

}  // namespace witnesskvs::server

#endif // SERVER_ROCKSDB_CONTAINER_H
