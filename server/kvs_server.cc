
#include "kvs_server.h"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "rocksdb_container.h"

// TODO: Maybe parse these from config file.
ABSL_FLAG(std::vector<std::string>, kvs_node_list, {},
          "Comma separated list of ip addresses and ports");
ABSL_FLAG(std::string, kvs_node_config_file, "server/kvs_nodes_cfg.txt",
          "KVS config file for nodes ip addresses and ports");
ABSL_FLAG(uint64_t, kvs_node_id, 0, "kvs_node_id");
ABSL_FLAG(std::string, kvs_db_path, "/var/tmp/kvs_rocksdb", "kvs_db_path");

ABSL_FLAG(bool, kvs_enable_linearizability_checks, false, "");
ABSL_FLAG(std::string, kvs_linearizability_json_file, "history.json",
          "kvs_linearizability_json_file");

ABSL_FLAG(uint16_t, kvs_num_shards, 1, "Number of (rocksdb for now) shards.");

KvsServiceImpl::KvsServiceImpl(std::vector<std::unique_ptr<Node>> nodes)
    : nodes_{std::move(nodes)} {}

void KvsServiceImpl::InitRocksDb(const std::string& db_path) {
  CHECK(this->paxos_ != nullptr)
      << "[KVS]: Attempting to initialize Rocks DB before PaxosNode";

  if (!this->paxos_->IsWitness()) {
    if (!std::filesystem::exists(std::filesystem::path{db_path})) {
      CHECK(std::filesystem::create_directory(std::filesystem::path{db_path}));
    }
    rocksdb_container_ = std::make_unique<witnesskvs::server::RocksDBContainer>(
        db_path, absl::GetFlag(FLAGS_kvs_num_shards));
    auto callback = std::bind(&KvsServiceImpl::KvsPaxosCommitCallback, this,
                              std::placeholders::_1);
    if (witnesskvs::paxos::PAXOS_OK !=
        this->paxos_->RegisterAppCallback(callback)) {
      LOG(FATAL) << "[KVS]: Failed Regsiter DB callback with paxos";
    }
    LOG(INFO) << "[KVS]: Registered Database callback.";
  }
}

KvsServiceImpl::~KvsServiceImpl() {}

void KvsServiceImpl::InitPaxos(void) {
  paxos_ = std::make_unique<witnesskvs::paxos::Paxos>(
      absl::GetFlag(FLAGS_kvs_node_id));
}

void KvsServiceImpl::KvsPaxosCommitCallback(std::string value) {
  KeyValueStore::OperationType op;
  if (!op.ParseFromString(value)) {
    LOG(FATAL) << "[KVS]: Parse from string failed!";
  }

  switch (op.type()) {
    case KeyValueStore::OperationType_Type_PUT: {
      rocksdb::Status status =
          this->rocksdb_container_->GetDB(op.put_data().key())
              ->Put(rocksdb::WriteOptions(), op.put_data().key(),
                    op.put_data().value());
      if (!status.ok()) {
        LOG(WARNING) << "[KVS]: Put operation failed with error : "
                     << status.ToString();
      }
      break;
    }
    case KeyValueStore::OperationType_Type_DELETE: {
      rocksdb::Status status =
          this->rocksdb_container_->GetDB(op.del_data().key())
              ->Delete(rocksdb::WriteOptions(), op.del_data().key());
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
              << nodes_[leader_node_id]->GetAddressPortStr();

    KeyValueStore::KvsStatus kvs_status;
    kvs_status.set_type(KeyValueStore::KvsStatus_Type_REDIRECT);
    kvs_status.mutable_redirect_details()->set_node_id(leader_node_id);
    kvs_status.mutable_redirect_details()->set_ip_address_with_port(
        nodes_[leader_node_id]->GetAddressPortStr());
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "REDIRECT",
                        kvs_status.SerializeAsString());
  } else {
    // Either there is no-leader or more likely there are not enough nodes up
    // and running.
    KeyValueStore::KvsStatus kvs_status;
    kvs_status.set_type(KeyValueStore::KvsStatus_Type_UNAVAILABLE);
    return grpc::Status(
        grpc::StatusCode::UNAVAILABLE,
        "[KVS]: Cluster is not in a state to serve requests right now",
        kvs_status.SerializeAsString());
  }
}

Status KvsServiceImpl::Put(ServerContext* context, const PutRequest* request,
                           PutResponse* response) {
  auto entry = LinearizabilityLogBegin();

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

  LinearizabilityLogEnd(entry, {"PUT", request->key(), request->value()});

  return status;
}

Status KvsServiceImpl::Get(ServerContext* context, const GetRequest* request,
                           GetResponse* response) {
  auto entry = LinearizabilityLogBegin();

  Status statusGrpc = PaxosProposeWrapper("", true);
  if (!statusGrpc.ok()) {
    return statusGrpc;
  }

  std::string value;
  rocksdb::Status status =
      rocksdb_container_->GetDB(request->key())
          ->Get(rocksdb::ReadOptions(), request->key(), &value);
  if (!status.ok()) {
    LOG(WARNING) << "[KVS]: Get operation for key: " << request->key()
                 << " failed with error: " << status.ToString();
    return convertRocksDbErrorToGrpcError(status);
  }

  response->set_value(value);

  LinearizabilityLogEnd(entry, {"GET", request->key(), value});

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

LinearizabilityChecker::~LinearizabilityChecker() {
  std::ofstream file(absl::GetFlag(FLAGS_kvs_linearizability_json_file));
  CHECK(file.is_open())
      << "[KVS]: Failed to open file to logging checker information";

  json json_vector;
  for (const auto& entry : json_log_) {
    json_vector += nlohmann::json({
        {"value", entry.value},
        {"start", entry.start},
        {"end", entry.end},
    });
  }

  file << json_vector.dump(4) << std::endl;
  file.close();
}

Status KvsServiceImpl::LinearizabilityCheckerInit(
    ServerContext* context, const google::protobuf::Empty* request,
    google::protobuf::Empty* response) {
  if (absl::GetFlag(FLAGS_kvs_enable_linearizability_checks)) {
    LOG(INFO) << "[KVS]: Initialized Linearizability checker.";
    absl::MutexLock l(&lock_);
    checker_ = std::make_unique<LinearizabilityChecker>();
  }
  return Status::OK;
}

Status KvsServiceImpl::LinearizabilityCheckerDeinit(
    ServerContext* context, const google::protobuf::Empty* request,
    google::protobuf::Empty* response) {
  absl::MutexLock l(&lock_);
  if (checker_) {
    LOG(INFO) << "[KVS]: Deinitialized Linearizability checker.";
    checker_.reset();
  }
  return Status::OK;
}

void RunKvsServer(const std::string& db_path,
                  std::vector<std::unique_ptr<Node>> nodes, uint8_t node_id) {
  const std::string address_port_str = nodes[node_id]->GetAddressPortStr();
  KvsServiceImpl service(std::move(nodes));
  service.InitPaxos();
  service.InitRocksDb(db_path);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::channelz::experimental::InitChannelzService();
  ServerBuilder builder;
  builder.AddListeningPort(address_port_str, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server = builder.BuildAndStart();

  server->Wait();
}

int main(int argc, char** argv) {
  std::vector<char*> positional_args;
  std::vector<absl::UnrecognizedFlag> unrecognized_flags;
  absl::ParseAbseilFlagsOnly(argc, argv, positional_args, unrecognized_flags);
  if (positional_args.size() != 1) {
    for (size_t i = 1; i < positional_args.size(); i++) {
      LOG(ERROR) << "Unknown arg " << positional_args[i];
    }
    return -1;
  }
  if (!unrecognized_flags.empty()) {
    for (const auto& unrecognized_flag : unrecognized_flags) {
      std::string from =
          unrecognized_flag.kFromArgv == absl::UnrecognizedFlag::kFromArgv
              ? "ARGV"
              : "FLAG_FILE";
      LOG(ERROR) << "Unknown " << from
                 << " flag: " << unrecognized_flag.flag_name;
    }
    return -1;
  }

  std::vector<std::unique_ptr<Node>> nodes;
  if (!absl::GetFlag(FLAGS_kvs_node_list).empty()) {
    nodes = ParseNodesList(absl::GetFlag(FLAGS_kvs_node_list));
  } else {
    nodes = ParseNodesConfig(absl::GetFlag(FLAGS_kvs_node_config_file));
  }
  CHECK_NE(nodes.size(), 0);

  uint8_t node_id = absl::GetFlag(FLAGS_kvs_node_id);

  std::string database_path =
      absl::StrCat(absl::GetFlag(FLAGS_kvs_db_path), node_id);

  RunKvsServer(database_path, std::move(nodes), node_id);
}