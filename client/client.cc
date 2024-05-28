#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>

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
    } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
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
    } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
      if (!status.error_message().empty()) {
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
    } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
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
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <target_address> <operation> <key> [value]" << std::endl;
    return 1;
  }

  std::string target_address = argv[1];
  std::string operation = argv[2];
  std::string key = argv[3];
  std::string value;

  if (operation == "put" && argc < 5) {
    std::cerr << "Usage for put operation: " << argv[0]
              << " <target_address> put <key> <value>" << std::endl;
    return 1;
  }

  if (operation == "put") {
    value = argv[4];
  }

  KvsClient client(
      grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials()));

  if (operation == "get") {
    std::string get_response = client.Get(key);
    std::cout << "Get response: " << get_response << std::endl;
  } else if (operation == "put") {
    std::string put_response = client.Put(key, value);
    std::cout << "Put response: " << put_response << std::endl;
  } else if (operation == "delete") {
    std::string delete_response = client.Delete(key);
    std::cout << "Delete response: " << delete_response << std::endl;
  } else {
    std::cerr << "Invalid operation. Use get, put, or delete." << std::endl;
    return 1;
  }

  return 0;
}