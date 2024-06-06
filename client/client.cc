#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <random>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "kvs.grpc.pb.h"
#include "util/node.h"

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

ABSL_FLAG(bool, interactive, false, "Mode for running the client test");

ABSL_FLAG(std::string, server_address, "",
          "Address of the key-value server (host:port)");

ABSL_FLAG(std::string, key, "", "Key for the operation");
ABSL_FLAG(std::string, value, "", "Value for put operation (optional)");
ABSL_FLAG(std::string, op, "", "Operation to perform (put, get, delete)");

ABSL_FLAG(std::string, kvs_node_config_file, "server/kvs_nodes_cfg.txt",
          "KVS config file for nodes ip addresses and ports");

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
static int PutHelper(const std::vector<std::unique_ptr<Node>>& nodes, const std::string& key,
                     const std::string& value) {
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

static std::string GetHelper(const std::vector<std::unique_ptr<Node>>& nodes,
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

static int DeleteHelper(const std::vector<std::unique_ptr<Node>>& nodes,
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

int NonInteractiveClientTest(void) {
  const std::vector<std::unique_ptr<Node>> nodes =
      ParseNodesConfig(absl::GetFlag(FLAGS_kvs_node_config_file));

  std::map<std::string, std::string> kv;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(1, 3);

  constexpr size_t num_tests = 500;
  for (size_t i = 0; i < num_tests; i++) {
    int step = dis(gen);

    switch (step) {
      case 1: {
        std::string k = "key" + std::to_string(i);
        std::string v = "value" + std::to_string(i);
        if (PutHelper(nodes, k, v) != 0) {
          LOG(FATAL) << "[Client]: Put operation failed for test number: " << i;
        } else {
          LOG(INFO) << "[Client]: Put operation successful for: " << i;
        }
        kv[k] = v;
        break;
      }
      case 2: {
        std::uniform_int_distribution<> key_dis(0, static_cast<int>(i));
        int random_key = key_dis(gen);
        std::string k = "key" + std::to_string((size_t)random_key);
        auto it = kv.find(k);
        if (it == kv.end()) {
          // Random key for get was a previously deleted key.
          break;
        }

        std::string expected_value = it->second;

        LOG(INFO) << "Performing Get operation for key: " << k;
        int ret = 0;
        std::string v = GetHelper(nodes, k, &ret);
        if (ret != 0) {
          LOG(FATAL) << "[Client]: Get operation failed! ";
        } else {
          LOG(INFO) << "[Client]: Get response: " << v;
          CHECK_EQ(v, it->second)
              << "Returned value: " << v
              << " does not match expectation: " << it->second;
        }
        break;
      }
      case 3: {
        std::uniform_int_distribution<> key_dis(0, static_cast<int>(i));
        int random_key = key_dis(gen);
        std::string k = "key" + std::to_string((size_t)random_key);
        auto it = kv.find(k);
        if (it == kv.end()) {
          // Random key for get was a previously deleted key.
          break;
        }

        if (DeleteHelper(nodes, k) != 0) {
          LOG(WARNING) << "[Client]: Delete operation failed for key: " << k;
        } else {
          LOG(INFO) << "[Client]: Delete operation successful for: " << i;
          kv.erase(it);
        }
        break;
      }
      default:
        LOG(FATAL) << "Invalid test step";
    }

    absl::SleepFor(absl::Milliseconds(500));
  }

  return 0;
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // absl::InitializeLog();

  if (!absl::GetFlag(FLAGS_interactive)) {
    return NonInteractiveClientTest();
  }

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

  // KvsClient client(grpc::CreateChannel(absl::GetFlag(FLAGS_server_address),
  //                                      grpc::InsecureChannelCredentials()));
  const std::vector<std::unique_ptr<Node>> nodes =
      ParseNodesConfig(absl::GetFlag(FLAGS_kvs_node_config_file));

  std::transform(op.begin(), op.end(), op.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (op == "put") {
    if (PutHelper(nodes, absl::GetFlag(FLAGS_key),
                  absl::GetFlag(FLAGS_value)) != 0) {
      LOG(WARNING) << "[Client]: Put operation failed!";
      std::cout << "[Client]: Put operation failed!";
    }
  } else if (op == "get") {
    int ret = 0;
    std::string value = GetHelper(nodes, absl::GetFlag(FLAGS_key), &ret);
    if (ret != 0) {
      LOG(WARNING) << "[Client]: Get operation failed!";
    } else {
      LOG(INFO) << "[Client]: Get response: " << value;
    }
  } else if (op == "delete") {
    if (DeleteHelper(nodes, absl::GetFlag(FLAGS_key)) != 0) {
      LOG(WARNING) << "[Client]: Delete operation failed!";
    }
  } else {
    LOG(WARNING) << "[Client]: Unsupported operation type";
  }

  return 0;
}