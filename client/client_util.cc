#include "client_util.hh"

#include "client_util_common.hh"

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

Status KvsClient::LinearizabilityCheckerInitGrpc() const {
  ClientContext context;
  google::protobuf::Empty empty;
  return stub_->LinearizabilityCheckerInit(&context, empty, nullptr);
}

Status KvsClient::LinearizabilityCheckerDeInitGrpc() const {
  ClientContext context;
  google::protobuf::Empty empty;
  return stub_->LinearizabilityCheckerDeinit(&context, empty, nullptr);
}

// TODO [V]: Should these helper functions do these operations in a fixed retry
// count loop ?
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

    // Retry this opeartion now with the leader address
    auto ch = grpc::CreateChannel(status.error_message(),
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

    // Retry this opeartion now with the leader address
    auto ch = grpc::CreateChannel(status.error_message(),
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
    auto ch = grpc::CreateChannel(status.error_message(),
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
  // for (size_t i = 0; i < nodes.size(); i++) {
  auto channel = grpc::CreateChannel(nodes[1]->GetAddressPortStr(),
                                     grpc::InsecureChannelCredentials());

  std::unique_ptr<Kvs::Stub> stub = Kvs::NewStub(channel);

  ClientContext context;
  google::protobuf::Empty empty;
  google::protobuf::Empty empty_response;

  // LOG(INFO) << "[Client]: Enabling Linearizability checker for node: " << i;
  Status status =
      stub->LinearizabilityCheckerInit(&context, empty, &empty_response);
  if (!status.ok()) {
    return -1;
  }
  // LOG(INFO) << "[Client]: Enabled Linearizability checker for node: " << i;
  //}
  return 0;
}

int LinearizabilityCheckerDeinitHelper(
    const std::vector<std::unique_ptr<Node>>& nodes) {
  // for (size_t i = 0; i < nodes.size(); i++) {
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
  //}
  return 0;
}