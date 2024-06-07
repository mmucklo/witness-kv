#include "client_util.h"

#include <grpcpp/grpcpp.h>

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

int PutHelper(const std::vector<std::unique_ptr<Node>>& nodes,
              const std::string& key, const std::string& value) {
  uint8_t node_id = 0;

  PutRequest request;
  request.set_key(key);
  request.set_value(value);

  PutResponse response;
  Status status;

  while (true) {
    auto channel = grpc::CreateChannel(nodes[node_id]->GetAddressPortStr(),
                                       grpc::InsecureChannelCredentials());

    std::unique_ptr<Kvs::Stub> stub = Kvs::NewStub(channel);

    ClientContext context;
    status = stub->Put(&context, request, &response);
    if (status.ok()) {
      LOG(INFO) << response.status();
      return 0;
    }
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      node_id = (++node_id) % nodes.size();
      continue;
    }

    if (status.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
      LOG(INFO) << "[Client]: Error from KVS server: " << status.error_code();
      return -1;
    }

    if (status.error_message().empty()) {
      LOG(INFO) << "[Client]: Error from KVS server***";
      return -1;
    }

    KeyValueStore::KvsStatus kvs_status;
    if (!kvs_status.ParseFromString(status.error_details())) {
      LOG(FATAL) << "Could not parse error details.";
    }
    LOG(INFO) << kvs_status.DebugString();
    CHECK_EQ(kvs_status.type(), KeyValueStore::KvsStatus_Type_REDIRECT);

    auto ch = grpc::CreateChannel(
        kvs_status.redirect_details().ip_address_with_port(),
        grpc::InsecureChannelCredentials());

    std::unique_ptr<Kvs::Stub> st = Kvs::NewStub(ch);

    ClientContext ctx;
    status = st->Put(&ctx, request, &response);
    if (status.ok()) {
      return 0;
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      absl::SleepFor(absl::Seconds(1));
    } else {
      LOG(WARNING) << "[Client]: Error from KVS server leader node: "
                   << status.error_code();
      return -1;
    }
  }

  return -1;
}

std::string GetHelper(const std::vector<std::unique_ptr<Node>>& nodes,
                      const std::string& key, int* return_code) {
  uint8_t node_id = 0;

  GetRequest request;
  request.set_key(key);

  GetResponse response;
  Status status;
  while (true) {
    auto channel = grpc::CreateChannel(nodes[node_id]->GetAddressPortStr(),
                                       grpc::InsecureChannelCredentials());

    std::unique_ptr<Kvs::Stub> stub = Kvs::NewStub(channel);

    ClientContext context;
    status = stub->Get(&context, request, &response);
    if (status.ok()) {
      return response.value();
    }
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      node_id = (++node_id) % nodes.size();
      continue;
    }

    if (status.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
      LOG(INFO) << "[Client]: Error from KVS server: " << status.error_code();
      *return_code = -1;
      return response.value();
    }

    if (status.error_message().empty()) {
      LOG(INFO) << "[Client]: Error from KVS server***";
      *return_code = -1;
      return response.value();
    }

    KeyValueStore::KvsStatus kvs_status;
    if (!kvs_status.ParseFromString(status.error_details())) {
      LOG(FATAL) << "Could not parse error details.";
    }
    LOG(INFO) << kvs_status.DebugString();
    CHECK_EQ(kvs_status.type(), KeyValueStore::KvsStatus_Type_REDIRECT);

    auto ch = grpc::CreateChannel(
        kvs_status.redirect_details().ip_address_with_port(),
        grpc::InsecureChannelCredentials());

    std::unique_ptr<Kvs::Stub> st = Kvs::NewStub(ch);

    ClientContext ctx;
    status = st->Get(&ctx, request, &response);
    if (status.ok()) {
      *return_code = 0;
      return response.value();
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      absl::SleepFor(absl::Seconds(2));
    } else {
      LOG(WARNING) << "[Client]: Error from KVS server leader node: "
                   << status.error_code();
      *return_code = 0;
      return response.value();
    }
  }
  *return_code = -1;
  return "";
}

int DeleteHelper(const std::vector<std::unique_ptr<Node>>& nodes,
                 const std::string& key) {
  uint8_t node_id = 0;

  DeleteRequest request;
  request.set_key(key);

  DeleteResponse response;
  Status status;

  while (true) {
    auto channel = grpc::CreateChannel(nodes[node_id]->GetAddressPortStr(),
                                       grpc::InsecureChannelCredentials());

    std::unique_ptr<Kvs::Stub> stub = Kvs::NewStub(channel);

    ClientContext context;
    status = stub->Delete(&context, request, &response);
    if (status.ok()) {
      LOG(INFO) << response.status();
      return 0;
    }
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      node_id = (++node_id) % nodes.size();
      continue;
    }

    if (status.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
      LOG(INFO) << "[Client]: Error from KVS server: " << status.error_code();
      return -1;
    }

    if (status.error_message().empty()) {
      LOG(INFO) << "[Client]: Error from KVS server***";
      return -1;
    }

    // Retry this opeartion now with the leader address

    KeyValueStore::KvsStatus kvs_status;
    if (!kvs_status.ParseFromString(status.error_details())) {
      LOG(FATAL) << "Could not parse error details.";
    }
    LOG(INFO) << kvs_status.DebugString();
    CHECK_EQ(kvs_status.type(), KeyValueStore::KvsStatus_Type_REDIRECT);

    auto ch = grpc::CreateChannel(
        kvs_status.redirect_details().ip_address_with_port(),
        grpc::InsecureChannelCredentials());

    std::unique_ptr<Kvs::Stub> st = Kvs::NewStub(ch);

    ClientContext ctx;
    status = st->Delete(&ctx, request, &response);
    if (status.ok()) {
      return 0;
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      absl::SleepFor(absl::Seconds(1));
    } else {
      LOG(WARNING) << "[Client]: Error from KVS server leader node: "
                   << status.error_code();
      return -1;
    }
  }

  return -1;
}

int LinearizabilityCheckerInitHelper(
    const std::vector<std::unique_ptr<Node>>& nodes) {
  auto channel = grpc::CreateChannel(nodes[1]->GetAddressPortStr(),
                                     grpc::InsecureChannelCredentials());

  std::unique_ptr<Kvs::Stub> stub = Kvs::NewStub(channel);

  ClientContext context;
  google::protobuf::Empty empty;
  google::protobuf::Empty empty_response;

  Status status =
      stub->LinearizabilityCheckerInit(&context, empty, &empty_response);
  if (!status.ok()) {
    return -1;
  }
  return 0;
}

int LinearizabilityCheckerDeinitHelper(
    const std::vector<std::unique_ptr<Node>>& nodes) {
  auto channel = grpc::CreateChannel(nodes[1]->GetAddressPortStr(),
                                     grpc::InsecureChannelCredentials());

  std::unique_ptr<Kvs::Stub> stub = Kvs::NewStub(channel);

  ClientContext context;
  google::protobuf::Empty empty;
  google::protobuf::Empty empty_response;

  Status status =
      stub->LinearizabilityCheckerDeinit(&context, empty, &empty_response);
  if (!status.ok()) {
    return -1;
  }
  return 0;
}