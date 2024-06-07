#include "client_util.h"

ABSL_FLAG(bool, interactive, false, "Mode for running the client test");
ABSL_FLAG(bool, linearizability, false,
          "Test will perform linearizability checks");

ABSL_FLAG(std::string, server_address, "",
          "Address of the key-value server (host:port)");

ABSL_FLAG(std::string, key, "", "Key for the operation");
ABSL_FLAG(std::string, value, "", "Value for put operation (optional)");
ABSL_FLAG(std::string, op, "", "Operation to perform (put, get, delete)");

ABSL_FLAG(std::string, kvs_node_config_file, "server/kvs_nodes_cfg.txt",
          "KVS config file for nodes ip addresses and ports");
ABSL_FLAG(std::vector<std::string>, kvs_node_list, {},
          "Comma separated list of ip addresses and ports");

int NonInteractiveClientTest(const std::vector<std::unique_ptr<Node>>& nodes) {
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

int InteractiveTest(const std::vector<std::unique_ptr<Node>>& nodes) {
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

int LinearizabilityTest(const std::vector<std::unique_ptr<Node>>& nodes) {
  CHECK_EQ(LinearizabilityCheckerInitHelper(nodes), 0);

  const size_t num_entries = 10;
  for (size_t i = 0; i < num_entries; i++) {
    CHECK_EQ(PutHelper(nodes, std::to_string(i), "0"), 0);
  }

  int magic = 100;
  std::thread thread1([num_entries, magic, &nodes]() {
    for (size_t i = 0; i < num_entries; i++) {
      CHECK_EQ(PutHelper(nodes, std::to_string(i), std::to_string(magic + 1)),
               0);
      int ret;
      std::string value = GetHelper(nodes, std::to_string(i), &ret);
      CHECK_EQ(ret, 0);
    }
  });

  magic = 200;
  std::thread thread2([num_entries, magic, &nodes]() {
    for (size_t i = 0; i < num_entries; i++) {
      CHECK_EQ(PutHelper(nodes, std::to_string(i), std::to_string(magic + 2)),
               0);
      int ret;
      std::string value = GetHelper(nodes, std::to_string(i), &ret);
      CHECK_EQ(ret, 0);
    }
  });

  // Wait for both threads to finish
  thread1.join();
  thread2.join();

  CHECK_EQ(LinearizabilityCheckerDeinitHelper(nodes), 0);
  return 0;
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // absl::InitializeLog();

  const std::vector<std::unique_ptr<Node>> nodes =
      absl::GetFlag(FLAGS_kvs_node_list).empty()
          ? ParseNodesConfig(absl::GetFlag(FLAGS_kvs_node_config_file))
          : ParseNodesList(absl::GetFlag(FLAGS_kvs_node_list));

  if (absl::GetFlag(FLAGS_interactive)) {
    return InteractiveTest(nodes);
  } else if (absl::GetFlag(FLAGS_linearizability)) {
    return LinearizabilityTest(nodes);
  } else {
    return NonInteractiveClientTest(nodes);
  }
  // return 0;
}