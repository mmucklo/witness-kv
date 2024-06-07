#include <google/protobuf/message.h>
#include <grpcpp/grpcpp.h>
#include <rocksdb/status.h>

#include <csignal>
#include <iostream>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "kvs.grpc.pb.h"
#include "paxos/paxos.hh"
#include "paxos/utils.hh"
#include "rocksdb/db.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using KeyValueStore::DeleteRequest;
using KeyValueStore::DeleteResponse;
using KeyValueStore::GetRequest;
using KeyValueStore::GetResponse;
using KeyValueStore::Kvs;
using KeyValueStore::PutRequest;
using KeyValueStore::PutResponse;

using json = nlohmann::json;

class LinearizabilityChecker {
 public:
  LinearizabilityChecker() = default;
  ~LinearizabilityChecker();

  struct JSONLogEntry {
    std::vector<std::string> value;
    long long start;
    long long end;
  };

  JSONLogEntry LogBegin(const std::vector<std::string>& value);
  void LogEnd(JSONLogEntry entry, const std::vector<std::string>& value);

 private:
  /*struct JSONLogEntry {
    std::string type;  // "invoke" or "ok"
    std::vector<std::string> value;
    long long time;
  };*/

  long long current_time_millis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  std::vector<JSONLogEntry> json_log_;
};

class KvsServiceImpl final : public Kvs::Service {
 public:
  KvsServiceImpl(std::vector<std::unique_ptr<Node>> nodes);
  ~KvsServiceImpl();

  void InitPaxos(void);
  void InitRocksDb(const std::string& db_path);

  // GRPC routines mapping directly to rocksdb operations.
  Status Get(ServerContext* context, const GetRequest* request,
             GetResponse* response) override;
  Status Put(ServerContext* context, const PutRequest* request,
             PutResponse* response) override;
  Status Delete(ServerContext* context, const DeleteRequest* request,
                DeleteResponse* response) override;
  Status LinearizabilityCheckerInit(ServerContext* context,
                                    const google::protobuf::Empty* request,
                                    google::protobuf::Empty* response) override;
  Status LinearizabilityCheckerDeinit(
      ServerContext* context, const google::protobuf::Empty* request,
      google::protobuf::Empty* response) override;

 private:
  rocksdb::DB* db_;
  std::unique_ptr<witnesskvs::paxos::Paxos> paxos_;

  absl::Mutex dblock_;

  std::vector<std::unique_ptr<Node>> nodes_;

  absl::Mutex lock_;
  std::unique_ptr<LinearizabilityChecker> checker_ ABSL_GUARDED_BY(lock_);

  Status PaxosProposeWrapper(const std::string& value, bool is_read);
  void KvsPaxosCommitCallback(std::string value);

  /*void LinearizabilityLog(const std::string& type,
                          const std::vector<std::string>& value) {
    absl::MutexLock l(&lock_);
    if (checker_) {
      checker_->Log(type, value);
    }
  }*/

  LinearizabilityChecker::JSONLogEntry LinearizabilityLogBegin(
      const std::vector<std::string>& value) {
    absl::MutexLock l(&lock_);
    if (checker_) {
      return checker_->LogBegin(value);
    }

    return {};
  }

  void LinearizabilityLogEnd(LinearizabilityChecker::JSONLogEntry entry,
                             const std::vector<std::string>& value) {
    absl::MutexLock l(&lock_);
    if (checker_) {
      return checker_->LogEnd(entry, value);
    }
  }
};