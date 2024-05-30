#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "kvs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using KeyValueStore::DeleteRequest;
using KeyValueStore::DeleteResponse;
using KeyValueStore::GetRequest;
using KeyValueStore::GetResponse;
using KeyValueStore::Kvs;
using KeyValueStore::PutRequest;
using KeyValueStore::PutResponse;

ABSL_FLAG(std::string, server_address, "",
          "Address of the key-value server (host:port)");

ABSL_FLAG(std::string, key, "", "Key for the operation");
ABSL_FLAG(std::string, value, "", "Value for put operation (optional)");
ABSL_FLAG(std::string, op, "", "Operation to perform (put, get, delete)");

class KvsClient {
 private:
  std::unique_ptr<Kvs::Stub> stub_;

 public:
  KvsClient(std::shared_ptr<Channel> channel);
  ~KvsClient() = default;

  Status PutGrpc(const std::string& key, const std::string& value,
                 PutResponse* response) const;

  Status GetGrpc(const std::string& key, GetResponse* response) const;

  Status DeleteGrpc(const std::string& key, DeleteResponse* response) const;
};

KvsClient::KvsClient(std::shared_ptr<Channel> channel)
    : stub_{Kvs::NewStub(channel)} {}

Status KvsClient::PutGrpc(const std::string& key, const std::string& value,
                          PutResponse* response) const {
  ClientContext context;

  PutRequest request;
  request.set_key(key);
  request.set_value(value);

  return stub_->Put(&context, request, response);
}

Status KvsClient::GetGrpc(const std::string& key, GetResponse* response) const {
  ClientContext context;

  GetRequest request;
  request.set_key(key);

  return stub_->Get(&context, request, response);
}

Status KvsClient::DeleteGrpc(const std::string& key,
                             DeleteResponse* response) const {
  ClientContext context;

  DeleteRequest request;
  request.set_key(key);

  return stub_->Delete(&context, request, response);
}

// TODO [V]: Should these helper functions do these operations in a fixed retry
// count loop ?
static int PutHelper(const KvsClient& client, const std::string& key,
                     const std::string& value) {
  PutResponse response;
  Status status = client.PutGrpc(key, value, &response);
  if (status.ok()) {
    LOG(INFO) << response.status();
    return 0;
  }

  if (status.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
    LOG(INFO) << "[Client]: Error from KVS server: " << status.error_code();
    return -1;
  }

  if (status.error_message().empty()) {
    return -1;
  }

  // Retry this opeartion now with the leader address returned by the
  KvsClient leader_client(grpc::CreateChannel(
      status.error_message(), grpc::InsecureChannelCredentials()));

  return PutHelper(leader_client, key, value);
}

static std::string GetHelper(const KvsClient& client, const std::string& key,
                             int* return_code) {
  GetResponse response;
  Status status = client.GetGrpc(key, &response);
  if (status.ok()) {
    LOG(INFO) << "[Client]: Get operation successful";
    *return_code = 0;
    return response.value();
  }

  if (status.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
    LOG(INFO) << "[Client]: Error from KVS server: " << status.error_code();
    *return_code = -1;
    return response.value();
  }

  if (status.error_message().empty()) {
    *return_code = -1;
    return response.value();
  }

  // Retry this opeartion now with the leader address returned by the
  KvsClient leader_client(grpc::CreateChannel(
      status.error_message(), grpc::InsecureChannelCredentials()));

  return GetHelper(leader_client, key, return_code);
}

static int DeleteHelper(const KvsClient& client, const std::string& key) {
  DeleteResponse response;
  Status status = client.DeleteGrpc(key, &response);
  if (status.ok()) {
    LOG(INFO) << "[Client]: Delete successful";
    return 0;
  }

  if (status.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
    LOG(INFO) << "[Client]: Error from KVS server: " << status.error_code();
    return -1;
  }

  if (status.error_message().empty()) {
    return -1;
  }

  // Retry this opeartion now with the leader address returned by the
  KvsClient leader_client(grpc::CreateChannel(
      status.error_message(), grpc::InsecureChannelCredentials()));

  return DeleteHelper(leader_client, key);
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // absl::InitializeLog();

  if (absl::GetFlag(FLAGS_server_address).empty()) {
    LOG(ERROR) << "[Client]: Usage: --server_address is required.";
    return 1;
  }

  if (absl::GetFlag(FLAGS_op).empty()) {
    LOG(ERROR) << "[Client]: Usage: --op is required (put, get, delete).";
    return 1;
  }

  if (absl::GetFlag(FLAGS_key).empty()) {
    LOG(ERROR)
        << "[Client]: Usage: --key is required for any op (put, get, delete).";
    return 1;
  }

  if ((absl::GetFlag(FLAGS_op) == "put") &&
      absl::GetFlag(FLAGS_value).empty()) {
    LOG(ERROR) << "[Client]: Usage: --value is required for put operation.";
    return 1;
  }

  std::string op = absl::GetFlag(FLAGS_op);

  KvsClient client(grpc::CreateChannel(absl::GetFlag(FLAGS_server_address),
                                       grpc::InsecureChannelCredentials()));

  std::transform(op.begin(), op.end(), op.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (op == "put") {
    if (PutHelper(client, absl::GetFlag(FLAGS_key),
                  absl::GetFlag(FLAGS_value)) != 0) {
      LOG(WARNING) << "[Client]: Put operation failed!";
      std::cout << "[Client]: Put operation failed!";
    }
  } else if (op == "get") {
    int ret = 0;
    std::string value = GetHelper(client, absl::GetFlag(FLAGS_key), &ret);
    if (ret != 0) {
      LOG(WARNING) << "[Client]: Get operation failed!";
    } else {
      LOG(INFO) << "[Client]: Get response: " << value;
    }
  } else if (op == "delete") {
    if (DeleteHelper(client, absl::GetFlag(FLAGS_key)) != 0) {
      LOG(WARNING) << "[Client]: Delete operation failed!";
    }
  } else {
    LOG(WARNING) << "[Client]: Unsupported operation type";
  }

  return 0;
}