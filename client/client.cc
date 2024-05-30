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
ABSL_FLAG(std::string, op, "", "Operation to perform (put, get, delete)");
ABSL_FLAG(std::string, key, "", "Key for the operation");
ABSL_FLAG(std::string, value, "", "Value for put operation (optional)");

class KvsClient {
 public:
  KvsClient(std::shared_ptr<Channel> channel) : stub_(Kvs::NewStub(channel)) {}

  std::string Get(const std::string& key) {
    GetRequest request;
    request.set_key(key);

    GetResponse response;
    ClientContext context;

    Status status = stub_->Get(&context, request, &response);

    if (status.ok()) {
      return response.value();
    } else if (status.error_code() == grpc::StatusCode::PERMISSION_DENIED) {
      if (!status.error_message().empty()) {
        return GetFromLeader(status.error_message(), key);
      }
    }
    return "Error: " + status.error_message();
  }

  std::string Put(const std::string& key, const std::string& value) {
    PutRequest request;
    request.set_key(key);
    request.set_value(value);

    PutResponse response;
    ClientContext context;

    Status status = stub_->Put(&context, request, &response);

    if (status.ok()) {
      return response.status();
    } else if (status.error_code() == grpc::StatusCode::PERMISSION_DENIED) {
      LOG(INFO) << status.error_message();
      if (!status.error_message().empty()) {
        LOG(INFO) << "Forward to leader";
        return PutToLeader(status.error_message(), key, value);
      }
    }
    return "Error: " + status.error_message();
  }

  std::string Delete(const std::string& key) {
    DeleteRequest request;
    request.set_key(key);

    DeleteResponse response;
    ClientContext context;

    Status status = stub_->Delete(&context, request, &response);

    if (status.ok()) {
      return response.status();
    } else if (status.error_code() == grpc::StatusCode::PERMISSION_DENIED) {
      if (!status.error_message().empty()) {
        return DeleteFromLeader(status.error_message(), key);
      }
    }
    return "Error: " + status.error_message();
  }

 private:
  std::unique_ptr<Kvs::Stub> stub_;

  std::string GetFromLeader(const std::string& leader_address,
                            const std::string& key) {
    KvsClient leader_client(grpc::CreateChannel(
        leader_address, grpc::InsecureChannelCredentials()));
    return leader_client.Get(key);
  }

  std::string PutToLeader(const std::string& leader_address,
                          const std::string& key, const std::string& value) {
    LOG(INFO) << "Put to leader! " << leader_address;
    KvsClient leader_client(grpc::CreateChannel(
        leader_address, grpc::InsecureChannelCredentials()));
    return leader_client.Put(key, value);
  }

  std::string DeleteFromLeader(const std::string& leader_address,
                               const std::string& key) {
    KvsClient leader_client(grpc::CreateChannel(
        leader_address, grpc::InsecureChannelCredentials()));
    return leader_client.Delete(key);
  }
};

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  absl::InitializeLog();

  if (absl::GetFlag(FLAGS_server_address).empty()) {
    LOG(ERROR) << "Usage: --server_address is required.";
    return 1;
  }

  if (absl::GetFlag(FLAGS_op).empty()) {
    LOG(ERROR) << "Usage: --op is required (put, get, delete).";
    return 1;
  }

  if (absl::GetFlag(FLAGS_key).empty()) {
    LOG(ERROR) << "Usage: --key is required for any op (put, get, delete).";
    return 1;
  }

  if ((absl::GetFlag(FLAGS_op) == "put") &&
      absl::GetFlag(FLAGS_value).empty()) {
    LOG(ERROR) << "Usage: --value is required for put operation.";
    return 1;
  }

  KvsClient client(grpc::CreateChannel(absl::GetFlag(FLAGS_server_address),
                                       grpc::InsecureChannelCredentials()));

  if (absl::GetFlag(FLAGS_op) == "get") {
    std::string get_response = client.Get(absl::GetFlag(FLAGS_key));
    std::cout << "Get response: " << get_response << std::endl;
    LOG(INFO) << "Get response: " << get_response;
  } else if (absl::GetFlag(FLAGS_op) == "put") {
    std::string put_response =
        client.Put(absl::GetFlag(FLAGS_key), absl::GetFlag(FLAGS_value));
    std::cout << "Put response: " << put_response << std::endl;
    LOG(INFO) << "Put response: " << put_response;
  } else if (absl::GetFlag(FLAGS_op) == "delete") {
    std::string delete_response = client.Delete(absl::GetFlag(FLAGS_key));
    std::cout << "Delete response: " << delete_response << std::endl;
    LOG(INFO) << "Delete response: " << delete_response;
  } else {
    std::cerr << "Invalid operation. Use get, put, or delete." << std::endl;
    return 1;
  }

  return 0;
}