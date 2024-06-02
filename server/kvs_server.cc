#include <google/protobuf/message.h>
#include <grpcpp/grpcpp.h>
#include <rocksdb/status.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>

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

// TODO: Maybe parse these from config file.
ABSL_FLAG(std::string, kvs_node_config_file, "server/kvs_nodes_cfg.txt",
          "KVS config file for nodes ip addresses and ports");
ABSL_FLAG(uint64_t, kvs_node_id, 0, "kvs_node_id");
ABSL_FLAG(std::string, kvs_db_path, "/tmp/kvs_rocksdb", "kvs_db_path");

class KvsServiceImpl final : public Kvs::Service {
 public:
  KvsServiceImpl(const std::string& db_path, std::vector<Node> nodes);
  ~KvsServiceImpl() { delete db_; }

  void InitPaxos(void);

  // GRPC routines mapping directly to rocksdb operations.
  Status Get(ServerContext* context, const GetRequest* request,
             GetResponse* response) override;
  Status Put(ServerContext* context, const PutRequest* request,
             PutResponse* response) override;
  Status Delete(ServerContext* context, const DeleteRequest* request,
                DeleteResponse* response) override;

 private:
  rocksdb::DB* db_;
  std::unique_ptr<witnesskvs::paxos::Paxos> paxos_;

  std::vector<Node> nodes_;

  Status PaxosProposeWrapper(const std::string& value, bool is_read);

  void KvsPaxosCommitCallback(std::string value);
};

KvsServiceImpl::KvsServiceImpl(const std::string& db_path,
                               std::vector<Node> nodes)
    : nodes_{nodes} {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
  if (!status.ok()) {
    LOG(FATAL) << "[KVS]: Failed to open RocksDB: " << status.ToString();
  }
}

void KvsServiceImpl::InitPaxos(void) {
  auto callback = std::bind(&KvsServiceImpl::KvsPaxosCommitCallback, this,
                            std::placeholders::_1);

  paxos_ = std::make_unique<witnesskvs::paxos::Paxos>(
      absl::GetFlag(FLAGS_kvs_node_id), callback);
}

void KvsServiceImpl::KvsPaxosCommitCallback(std::string value) {
  KeyValueStore::OperationType op;
  if (!op.ParseFromString(value)) {
    LOG(FATAL) << "[KVS]: Parse from string failed!";
  }

  switch (op.type()) {
    case KeyValueStore::OperationType_Type_PUT: {
      rocksdb::Status status = this->db_->Put(
          rocksdb::WriteOptions(), op.put_data().key(), op.put_data().value());
      if (!status.ok()) {
        LOG(WARNING) << "[KVS]: Put operation failed with error : "
                     << status.ToString();
      }
      break;
    }
    case KeyValueStore::OperationType_Type_DELETE: {
      rocksdb::Status status =
          this->db_->Delete(rocksdb::WriteOptions(), op.del_data().key());
      if (!status.ok()) {
        LOG(WARNING) << "[KVS]: Delete operation failed with error : "
                     << status.ToString();
      }
      break;
    }
    default:
      LOG(FATAL) << "[KVS]: Unknown operation type requested on rocks db";
      break;
  }
}

// Helper function to convert rocks db errors to grpc errors
static Status convertRocksDbErrorToGrpcError(const rocksdb::Status& status) {
  switch (status.code()) {
    case rocksdb::IOStatus::kNotFound:
      return Status(grpc::NOT_FOUND, status.ToString());
    case rocksdb::IOStatus::kCorruption:
      return Status(grpc::INTERNAL, "[KVS]: RocksDB internal corruption");
    case rocksdb::IOStatus::kInvalidArgument:
      return Status(grpc::INVALID_ARGUMENT, status.ToString());
    default:
      return Status(grpc::UNKNOWN, status.ToString());
  }
}

Status KvsServiceImpl::PaxosProposeWrapper(const std::string& value,
                                           bool is_read = false) {
  using witnesskvs::paxos::PaxosResult;
  using witnesskvs::paxos::PaxosResult::PAXOS_ERROR_NOT_PERMITTED;
  using witnesskvs::paxos::PaxosResult::PAXOS_OK;

  uint8_t leader_node_id = INVALID_NODE_ID;

  PaxosResult result = paxos_->Propose(value, &leader_node_id, is_read);
  if (PAXOS_OK == result) {
    return Status::OK;
  }

  if (PAXOS_ERROR_NOT_PERMITTED == result) {
    CHECK_NE(leader_node_id, INVALID_NODE_ID)
        << "[KVS]: Paxos library returned us an invalid leader node id";

    LOG(INFO) << "[KVS]: Leader address returned: "
              << nodes_[leader_node_id].GetAddressPortStr();
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        nodes_[leader_node_id].GetAddressPortStr());
  } else {
    // Either there is no-leader or more likely there are not enough nodes up
    // and running.
    return grpc::Status(
        grpc::StatusCode::UNAVAILABLE,
        "[KVS]: Cluster is not in a state to serve requests right now");
  }
}

Status KvsServiceImpl::Put(ServerContext* context, const PutRequest* request,
                           PutResponse* response) {
  KeyValueStore::OperationType op;
  op.set_type(KeyValueStore::OperationType_Type_PUT);
  KeyValueStore::PutRequest* kv_put = op.mutable_put_data();
  kv_put->set_key(request->key());
  kv_put->set_value(request->value());

  std::string serialized_request;
  if (!op.SerializeToString(&serialized_request)) {
    LOG(WARNING)
        << "[KVS]: SerializeToString to string failed in Put operation.";
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "[KVS]: Failed to serialize Put request.");
  }

  Status status = PaxosProposeWrapper(serialized_request);
  if (status.ok()) {
    response->set_status("[KVS]: Put operation Successful");
  }
  return status;
}

Status KvsServiceImpl::Get(ServerContext* context, const GetRequest* request,
                           GetResponse* response) {
  Status statusGrpc = PaxosProposeWrapper("", true);
  if (!statusGrpc.ok()) {
    return statusGrpc;
  }

  std::string value;
  rocksdb::Status status =
      db_->Get(rocksdb::ReadOptions(), request->key(), &value);
  if (!status.ok()) {
    LOG(WARNING) << "[KVS]: Get operation for key: " << request->key()
                 << " failed with error: " << status.ToString();
    return convertRocksDbErrorToGrpcError(status);
  }

  response->set_value(value);
  return Status::OK;
}

Status KvsServiceImpl::Delete(ServerContext* context,
                              const DeleteRequest* request,
                              DeleteResponse* response) {
  KeyValueStore::OperationType op;
  op.set_type(KeyValueStore::OperationType_Type_DELETE);
  KeyValueStore::DeleteRequest* kv_del = op.mutable_del_data();
  kv_del->set_key(request->key());

  std::string serialized_request;
  if (!op.SerializeToString(&serialized_request)) {
    LOG(WARNING)
        << "[KVS]: SerializeToString to string failed in Delete operation.";
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Failed to serialize Delete request.");
  }

  Status status = PaxosProposeWrapper(serialized_request);
  if (status.ok()) {
    response->set_status("[KVS]: Delete operation Successful");
  }
  return status;
}

void RunKvsServer(const std::string& db_path, std::vector<Node> nodes,
                  uint8_t node_id) {
  std::string server_address = nodes[node_id].GetAddressPortStr();

  KvsServiceImpl service(db_path, nodes);
  service.InitPaxos();

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // absl::InitializeLog();

  auto nodes = ParseNodesConfig(absl::GetFlag(FLAGS_kvs_node_config_file));
  CHECK_NE(nodes.size(), 0);

  uint8_t node_id = absl::GetFlag(FLAGS_kvs_node_id);

  std::string database_path =
      absl::StrCat(absl::GetFlag(FLAGS_kvs_db_path), node_id, ".",
                   absl::ToUnixMicros(absl::Now()));

  RunKvsServer(database_path, nodes, node_id);
}