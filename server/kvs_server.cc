#include <google/protobuf/message.h>
#include <grpcpp/grpcpp.h>

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

using KeyValueStore::OperationType;

// TODO: Maybe parse these from config file.
ABSL_FLAG(std::string, kvs_ip_address, "localhost", "kvs_ip_address");
ABSL_FLAG(uint64_t, kvs_port, 40051, "kvs_port");
ABSL_FLAG(uint64_t, kvs_node_id, 0, "kvs_node_id");
ABSL_FLAG(std::string, kvs_db_path, "/tmp/kvs_rocksdb", "kvs_db_path");

class KvsServiceImpl final : public Kvs::Service {
 public:
  KvsServiceImpl(const std::string& db_path /*, bool isLeader*/);
  ~KvsServiceImpl() { delete db_; }

  void KvsPaxosCommitCb(uint64_t idx, std::string value);

  Status Get(ServerContext* context, const GetRequest* request,
             GetResponse* response) override;

  Status Put(ServerContext* context, const PutRequest* request,
             PutResponse* response) override;

  Status Delete(ServerContext* context, const DeleteRequest* request,
                DeleteResponse* response) override;

  // bool IsLeader() { return isLeader_; }
  // std::string GetLeader() { return "localhost:50062"; }

  void InitPaxos(void);

 private:
  rocksdb::DB* db_;
  std::unique_ptr<witnesskvs::paxos::Paxos> paxos_;
  bool isLeader_;

  uint64_t first_unapplied_idx_;
};

KvsServiceImpl::KvsServiceImpl(const std::string& db_path /*, bool isLeader*/)
    : first_unapplied_idx_{0} {
  // isLeader_ = isLeader;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
  }
}

void KvsServiceImpl::InitPaxos(void) {
  auto commitCb = std::bind(&KvsServiceImpl::KvsPaxosCommitCb, this,
                            std::placeholders::_1, std::placeholders::_2);

  paxos_ = std::make_unique<witnesskvs::paxos::Paxos>(
      absl::GetFlag(FLAGS_kvs_node_id), commitCb);
}

#if 0
Status KvsServiceImpl::Put(ServerContext* context, const PutRequest* request,
                           PutResponse* response) {
  /*if (!IsLeader()) {
    std::string leader = GetLeader();
    LOG(INFO) << "Not a leader, forwarding leader info to client: " << leader
              << "\n";
    return grpc::Status(grpc::StatusCode::NOT_FOUND, leader);
  }*/

  // Serialize user request
  std::string serialized_request;
  if (request->SerializeToString(&serialized_request)) {
    LOG(INFO) << "Serialize successful.";
    serialized_request = "PUT:" + serialized_request;
    LOG(INFO) << "After serialization: serialized_request: "
              << serialized_request;

    PutRequest request1;
    // LOG(INFO) << "In Put "           <<
    //           request1.ParseFromString(serialized_request.substr(4)).key();
  }

  // Paxos propose
  paxos_->Propose(serialized_request);

  response->set_status("OK");
  return Status::OK;

  /*rocksdb::Status status =
      db_->Put(rocksdb::WriteOptions(), request->key(), request->value());
  if (status.ok()) {
    response->set_status("OK");
    return Status::OK;
  } else {
    return Status::CANCELLED;
  }*/
}
#endif

Status KvsServiceImpl::Put(ServerContext* context, const PutRequest* request,
                           PutResponse* response) {
  // KeyValueStore::OperationType op;
  KeyValueStore::OperationType op;
  // op.set_type(KeyValueStore::OperationTypeEnum::PUT);
  // op.mutable_put_data()->CopyFrom(*request);
  //  op.mutable_put_data()->set_key(request->key());
  //  op.mutable_put_data()->set_value(request->value());

  op.set_type(KeyValueStore::OperationType_Type_PUT);
  KeyValueStore::PutRequest* kv = op.mutable_message();
  kv->set_key(request->key());
  kv->set_value(request->value());

  std::string serialized_request;
  if (!op.SerializeToString(&serialized_request)) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Failed to serialize Put request.");
  }

  paxos_->Propose(serialized_request);

  response->set_status("OK");
  return Status::OK;
}

// using google::protobuf::MessageLite::ParseFromString;

void KvsServiceImpl::KvsPaxosCommitCb(uint64_t idx, std::string value) {
  KeyValueStore::OperationType op;
  if (!op.ParseFromString(value)) {
    LOG(FATAL) << "Parse from string failed!";
  }

  LOG(INFO) << "In KvsPaxosCommitCb value string: " << value;
  LOG(INFO) << "In KvsPaxosCommitCb value serialized: " << op;

  switch (op.type()) {
    case KeyValueStore::OperationType_Type_PUT: {
      LOG(INFO) << "Put request: Key: " << op.message().key()
                << " and value: " << op.message().value();
      break;
    }
    case KeyValueStore::OperationType_Type_DELETE:
    default:
      LOG(FATAL) << "Not implemented";
      break;
  }

  // LOG(INFO) << "request.key(): " << op.data.
  //<< " and request.value(): " << request.value();
}

#if 0
void KvsServiceImpl::KvsPaxosCommitCb(uint64_t idx, std::string value) {
  // std::cerr << "Get Callback from paxos\n";
  LOG(INFO) << "Get Callback from paxos\n";

  LOG(INFO) << "In callback: value  received: " << value;

  std::string op = std ::string(value.substr(0, 4));
  if (op == "PUT:") {
    PutRequest request;
    request.ParseFromString(value.substr(4));

    // std::cerr << "request.key(): " << request.key()
    //           << " and request.value(): " << request.value() << "\n";
    LOG(INFO) << "request.key(): " << request.key()
              << " and request.value(): " << request.value();

    rocksdb::Status status =
        this->db_->Put(rocksdb::WriteOptions(), request.key(), request.value());
  } else if (op == "DEL:") {
    LOG(FATAL) << "Not implemented";
  } else {
    LOG(FATAL) << "Something went wrong...";
  }
}
#endif

Status KvsServiceImpl::Get(ServerContext* context, const GetRequest* request,
                           GetResponse* response) {
  /*if (!IsLeader()) {
    std::string leader = GetLeader();
    LOG(INFO) << "Not a leader, forwarding leader info to client: " << leader
              << "\n";
    return grpc::Status(grpc::StatusCode::NOT_FOUND, leader);
  }*/
  std::string value;
  rocksdb::Status status =
      db_->Get(rocksdb::ReadOptions(), request->key(), &value);
  if (status.ok()) {
    response->set_value(value);
    return Status::OK;
  } else {
    return Status::CANCELLED;
  }
}

Status KvsServiceImpl::Delete(ServerContext* context,
                              const DeleteRequest* request,
                              DeleteResponse* response) {
  /*if (!IsLeader()) {
    std::string leader = GetLeader();
    LOG(INFO) << "Not a leader, forwarding leader info to client: " << leader
              << "\n";
    return grpc::Status(grpc::StatusCode::NOT_FOUND, leader);
  }*/

  rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), request->key());
  if (status.ok()) {
    response->set_status("OK");
    return Status::OK;
  } else {
    return Status::CANCELLED;
  }
}

void RunKvsServer(const std::string& db_path, uint16_t port
                  /*bool isLeader = false*/) {
  std::string port_str = std::to_string(port);
  std::string server_address =
      absl::GetFlag(FLAGS_kvs_ip_address) + ":" + port_str;

  KvsServiceImpl service(db_path /*, isLeader*/);
  service.InitPaxos();

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  LOG(INFO) << "KVS Server exited!\n";
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // absl::InitializeLog();

  uint16_t server_port = absl::GetFlag(FLAGS_kvs_port);

  std::string database_path =
      absl::StrCat(absl::GetFlag(FLAGS_kvs_db_path), server_port, ".",
                   absl::ToUnixMicros(absl::Now()));

  RunKvsServer(database_path, server_port /*, true*/);
  /*KeyValueStore::OperationType op;
  op.set_type(KeyValueStore::OperationType_Type_PUT);
  KeyValueStore::PutRequest* kv = op.mutable_message();
  kv->set_key("key");
  kv->set_value("value");*/

  // op.mutable_put_data()->CopyFrom(*request);
  //  op.mutable_put_data()->set_key(request->key());
  //  op.mutable_put_data()->set_value(request->value());
}