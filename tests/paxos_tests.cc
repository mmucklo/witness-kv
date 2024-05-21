// Gtest header
// #include <absl/flags.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>

#include "paxos.hh"

// ABSL_DECLARE_FLAG( int, absl_log_min_level, 3,  // Defaults to FATAL (3)
//                    "Minimum log level for Abseil logging." );

// Simple test to sanity check file parsing logic.
TEST(PaxosSanity, ConfigFileParseTest) {
  std::vector<std::string> addrs = {"0.0.0.0", "0.1.2.3", "8.7.6.5",
                                    "10.10.10.10"};
  std::vector<std::string> ports = {"10", "20", "30", "40"};
  ASSERT_EQ(addrs.size(), ports.size());

  char filename[] = "/tmp/paxos_config_file_test";
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < addrs.size(); i++) {
    temp_file << addrs[i] << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;

  auto nodes = ParseNodesConfig(filename);
  ASSERT_EQ(addrs.size(), nodes.size());

  for (size_t i = 0; i < addrs.size(); i++) {
    ASSERT_EQ(addrs[i], nodes[i].ip_address_);
    ASSERT_EQ(std::stoi(ports[i]), nodes[i].port_);
  }

  temp_file.close();
  ASSERT_EQ(remove(filename), 0);
}

// Helper function to check replicated log on all nodes.
void VerifyLogIntegrity(const std::vector<std::unique_ptr<Paxos>> &nodes,
                        size_t total_proposals) {
  std::vector<std::map<uint64_t, ReplicatedLogEntry>> logs(nodes.size());
  for (size_t i = 0; i < nodes.size(); i++) {
    logs[i] = nodes[i]->GetReplicatedLog()->GetLogEntries();
    CHECK_EQ(logs[i].size(), total_proposals);
  }

  // Ensure all the nodes see the same value in their log positions.
  for (size_t i = 0; i < total_proposals; i++) {
    for (size_t n = 1; n < nodes.size() - 1; n++) {
      auto left = logs[0].find(i)->second;
      auto right = logs[n].find(i)->second;
      CHECK_EQ(left.accepted_value_, right.accepted_value_)
          << "Log values do not match at index: " << i << " on nodes 0("
          << left.accepted_value_ << ") and " << n << "("
          << right.accepted_value_ << ")";
    }
  }
}

TEST(PaxosSanity, ReplicatedLogSanity) {
  const size_t num_nodes = 3;
  const int sleep_timer = 4;
  std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>("paxos/nodes_config.txt", i);
  }

  sleep(sleep_timer);

  const size_t num_proposals = 10;
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[0]->Propose(std::to_string(i));
  }

  VerifyLogIntegrity(nodes, num_proposals);
}

TEST(PaxosSanity, ReplicatedLogAfterNodeReconnection) {
  const size_t num_nodes = 3;
  const int sleep_timer = 4;
  std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>("paxos/nodes_config.txt", i);
  }

  sleep(sleep_timer);

  const size_t num_proposals = 5;
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[num_nodes - 1]->Propose(std::to_string(i));
  }

  // Mimic node 0 going away and coming back up.
  nodes[0].reset();
  nodes[0] = std::make_unique<Paxos>("paxos/nodes_config.txt", 0);
  sleep(sleep_timer);

  for (size_t i = 0; i < num_proposals; i++) {
    nodes[num_nodes - 1]->Propose(std::to_string(num_proposals + i));
  }

  VerifyLogIntegrity(nodes, 2 * num_proposals);
}