// Gtest header
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>

#include "absl/flags/flag.h"
#include "paxos.hh"

ABSL_DECLARE_FLAG(uint64_t, absl_log_min_level);

ABSL_DECLARE_FLAG(std::string, paxos_log_directory);
ABSL_DECLARE_FLAG(std::string, paxos_log_file_prefix);

ABSL_DECLARE_FLAG(std::string, paxos_node_config_file);
ABSL_DECLARE_FLAG(absl::Duration, paxos_node_heartbeat);
ABSL_DECLARE_FLAG(bool, lower_node_witness);

// Simple test to sanity check file parsing logic.
TEST(FileParseTest, ConfigFileParseTest) {
  std::vector<std::string> addrs = {"0.0.0.0", "0.1.2.3", "8.7.6.5",
                                    "10.10.10.10"};
  std::vector<std::string> ports = {"10", "20", "30", "40"};
  ASSERT_EQ(addrs.size(), ports.size());

  char filename[] = "/tmp/paxos_config_file_test";
  absl::SetFlag(&FLAGS_paxos_node_config_file, "/tmp/paxos_config_file_test");
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < addrs.size(); i++) {
    temp_file << addrs[i] << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;

  auto nodes = witnesskvs::paxoslibrary::ParseNodesConfig();
  ASSERT_EQ(addrs.size(), nodes.size());

  for (size_t i = 0; i < addrs.size(); i++) {
    ASSERT_EQ(addrs[i], nodes[i].ip_address_);
    ASSERT_EQ(std::stoi(ports[i]), nodes[i].port_);
  }

  temp_file.close();
  ASSERT_EQ(remove(filename), 0);
}

// Helper function to check replicated log on all nodes.
void VerifyLogIntegrity(
    const std::vector<std::unique_ptr<witnesskvs::paxoslibrary::Paxos>> &nodes,
    size_t total_proposals) {
  std::vector<std::map<uint64_t, witnesskvs::paxoslibrary::ReplicatedLogEntry>>
      logs(nodes.size());
  for (size_t i = 0; i < nodes.size(); i++) {
    logs[i] = nodes[i]->GetReplicatedLog()->GetLogEntries();
    ASSERT_EQ(logs[i].size(), total_proposals);
  }

  // Ensure all the nodes see the same value in their log positions.
  for (size_t i = 0; i < total_proposals; i++) {
    for (size_t n = 1; n < nodes.size() - 1; n++) {
      auto left = logs[0].find(i)->second;
      auto right = logs[n].find(i)->second;
      ASSERT_EQ(left.accepted_value_, right.accepted_value_)
          << "Log values do not match at index: " << i << " on nodes 0("
          << left.accepted_value_ << ") and " << n << "("
          << right.accepted_value_ << ")";
    }
  }
}

struct PaxosSanity : public ::testing::Test {
  static constexpr uint64_t heartbeat_timer = 300;  // ms
  // Setup up abseil flags for paxos library to suit these test.
  // Log directory and prefix are chosen such that they do not collide with
  // existing default logs.
  // Heartbeat timer is set so that the tests can run faster.
  virtual void SetUp() override {
    absl::SetFlag(&FLAGS_paxos_node_heartbeat,
                  absl::Milliseconds(heartbeat_timer));
    absl::SetFlag(&FLAGS_paxos_log_directory, "/tmp");
    absl::SetFlag(&FLAGS_paxos_log_file_prefix, "paxos_sanity_test");
  }

  // Cleanup all log files we may have created.
  virtual void TearDown() override {
    std::string log_files = absl::GetFlag(FLAGS_paxos_log_directory) + "/" +
                            absl::GetFlag(FLAGS_paxos_log_file_prefix) + "*";

    // TODO [V]: Use a more portable std::filesystem (or something) to remove
    // these files
    std::string command = "rm -f " + log_files;
    CHECK_EQ(system(command.c_str()), 0);
  }
};

TEST_F(PaxosSanity, ReplicatedLogSanity) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxoslibrary::Paxos>> nodes(
      num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxoslibrary::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 10;
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[0]->Propose(std::to_string(i));
  }

  VerifyLogIntegrity(nodes, num_proposals);
}

TEST_F(PaxosSanity, BasicStableLogSanity) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxoslibrary::Paxos>> nodes(
      num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxoslibrary::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[num_nodes - 1]->Propose(std::to_string(i));
  }

  // Mimic node 0 going away and coming back up.
  nodes[0].reset();
  nodes[0] = std::make_unique<witnesskvs::paxoslibrary::Paxos>(0);
  absl::SleepFor(sleep_timer);

  // After node 0 comes back up it should have all the committed entires intact.
  std::map<uint64_t, witnesskvs::paxoslibrary::ReplicatedLogEntry> log =
      nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), num_proposals);

  for (size_t i = 0; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_value_, std::to_string(i))
        << "Log values do not match at index: " << i << " observed: ("
        << entry.accepted_value_ << ") and expected: (" << std::to_string(i)
        << ")";
    CHECK(entry.is_chosen_);
  }

  // Make node 0 propose some values and verify that all nodes see the same log.
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[0]->Propose(std::to_string(num_proposals + i));
  }

  VerifyLogIntegrity(nodes, 2 * num_proposals);
}

TEST_F(PaxosSanity, ReplicatedLogAfterNodeReconnection) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxoslibrary::Paxos>> nodes(
      num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxoslibrary::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[num_nodes - 1]->Propose(std::to_string(i));
  }

  // Mimic node 0 going away and coming back up.
  nodes[0].reset();
  nodes[0] = std::make_unique<witnesskvs::paxoslibrary::Paxos>(0);
  absl::SleepFor(sleep_timer);

  for (size_t i = 0; i < num_proposals; i++) {
    nodes[num_nodes - 1]->Propose(std::to_string(num_proposals + i));
  }

  VerifyLogIntegrity(nodes, 2 * num_proposals);
}

TEST_F(PaxosSanity, ReplicatedLogWhenOneNodeIsDown) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxoslibrary::Paxos>> nodes(
      num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxoslibrary::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;

  // First batch of proposals, all nodes are up.
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[0]->Propose(std::to_string(i));
  }

  // Mimic node going away and some operations happening while node is down and
  // then node comes back up.
  nodes[num_nodes - 1].reset();
  absl::SleepFor(sleep_timer);

  // Second batch of proposals, one node is not up.
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[0]->Propose(std::to_string(num_proposals + i));
  }

  nodes[num_nodes - 1] =
      std::make_unique<witnesskvs::paxoslibrary::Paxos>((num_nodes - 1));
  absl::SleepFor(sleep_timer);

  // Third batch of proposals, all nodes are up again.
  for (size_t i = 0; i < num_proposals; i++) {
    nodes[0]->Propose(std::to_string(2 * num_proposals + i));
  }

  // Log must reflect the operations from all three batch of proposals.
  VerifyLogIntegrity(nodes, 3 * num_proposals);
}

TEST_F(PaxosSanity, WitnessNotLeader) {
  using witnesskvs::paxoslibrary::Paxos;

  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(0), false);
  ASSERT_EQ(nodes[1]->IsWitness(1), false);
  ASSERT_EQ(nodes[1]->IsLeader(1), true);
  ASSERT_EQ(nodes[2]->IsWitness(2), true);
  ASSERT_EQ(nodes[2]->IsLeader(2), false);

  absl::SetFlag(&FLAGS_lower_node_witness, true);
  nodes.clear();
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(0), true);
  ASSERT_EQ(nodes[1]->IsLeader(1), false);
  ASSERT_EQ(nodes[2]->IsWitness(2), false);
  ASSERT_EQ(nodes[2]->IsLeader(2), true);
}

TEST_F(PaxosSanity, WitnessCount) {
  using witnesskvs::paxoslibrary::Paxos;

  std::string addr = "localhost";
  std::vector<std::string> ports = {"50051", "50052", "50053", "50054",
                                    "50055"};

  char filename[] = "/tmp/nodes_config_5nodes";
  absl::SetFlag(&FLAGS_paxos_node_config_file, "/tmp/nodes_config_5nodes");
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < ports.size(); i++) {
    temp_file << addr << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;
  absl::SetFlag(&FLAGS_paxos_node_config_file, "/tmp/nodes_config_5nodes");
  const size_t num_nodes = 5;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(0), false);
  ASSERT_EQ(nodes[1]->IsWitness(1), false);
  ASSERT_EQ(nodes[2]->IsWitness(2), false);
  ASSERT_EQ(nodes[2]->IsLeader(2), true);
  ASSERT_EQ(nodes[3]->IsWitness(3), true);
  ASSERT_EQ(nodes[4]->IsWitness(4), true);
  ASSERT_EQ(nodes[4]->IsLeader(4), false);

  nodes.clear();

  absl::SleepFor(sleep_timer);

  absl::SetFlag(&FLAGS_lower_node_witness, true);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(0), true);
  ASSERT_EQ(nodes[1]->IsWitness(1), true);
  ASSERT_EQ(nodes[2]->IsWitness(2), false);
  ASSERT_EQ(nodes[3]->IsWitness(3), false);
  ASSERT_EQ(nodes[4]->IsWitness(4), false);
  ASSERT_EQ(nodes[4]->IsLeader(4), true);
}