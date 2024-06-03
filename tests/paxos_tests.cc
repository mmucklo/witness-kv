// Gtest header
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>

#include "absl/flags/flag.h"
#include "paxos.hh"
#include "utils.hh"

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
  // absl::SetFlag(&FLAGS_paxos_node_config_file,
  // "/tmp/paxos_config_file_test");
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < addrs.size(); i++) {
    temp_file << addrs[i] << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;

  std::vector<Node> nodes = ParseNodesConfig(filename);
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
    const std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>> &nodes,
    size_t total_proposals) {
  std::vector<std::map<uint64_t, witnesskvs::paxos::ReplicatedLogEntry>> logs(
      nodes.size());
  for (size_t i = 0; i < nodes.size(); i++) {
    if (nodes[i]) {
      logs[i] = nodes[i]->GetReplicatedLog()->GetLogEntries();
      ASSERT_EQ(logs[i].size(), total_proposals);
    }
  }

  // Ensure all the nodes see the same value in their log positions.
  for (size_t i = 0; i < total_proposals; i++) {
    for (size_t n = 1; n < nodes.size() - 1; n++) {
      if (!nodes[i]) continue;
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
  std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxos::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 10;
  uint8_t leader_id;
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[0]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[leader_id]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }

  VerifyLogIntegrity(nodes, num_proposals);
}

TEST_F(PaxosSanity, BasicStableLogSanity) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>> nodes(num_nodes);
  uint8_t leader_id;
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxos::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;
  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[num_nodes - 1]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[leader_id]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  // Mimic node 0 going away and coming back up.
  nodes[0].reset();
  nodes[0] = std::make_unique<witnesskvs::paxos::Paxos>(0);
  absl::SleepFor(sleep_timer);

  // After node 0 comes back up it should have all the committed entires intact.
  std::map<uint64_t, witnesskvs::paxos::ReplicatedLogEntry> log =
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
    auto status =
        nodes[0]->Propose(std::to_string(num_proposals + i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    nodes[leader_id]->Propose(std::to_string(num_proposals + i), &leader_id,
                              false);
  }

  VerifyLogIntegrity(nodes, 2 * num_proposals);
}

TEST_F(PaxosSanity, ReplicatedLogAfterNodeReconnection) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>> nodes(num_nodes);
  uint8_t leader_id;
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxos::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;
  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[num_nodes - 1]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[leader_id]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }

  // Mimic node 0 going away and coming back up.
  nodes[0].reset();
  nodes[0] = std::make_unique<witnesskvs::paxos::Paxos>(0);
  absl::SleepFor(sleep_timer);

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[num_nodes - 1]->Propose(
        std::to_string(num_proposals + i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[leader_id]->Propose(std::to_string(num_proposals + i),
                                            &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }

  VerifyLogIntegrity(nodes, 2 * num_proposals);
}

TEST_F(PaxosSanity, ReplicatedLogWhenOneNodeIsDown) {
  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>> nodes(num_nodes);
  uint8_t leader_id;
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxos::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;

  // First batch of proposals, all nodes are up.
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[0]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[leader_id]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }

  // Mimic node going away and some operations happening while node is down and
  // then node comes back up.
  nodes[num_nodes - 1].reset();
  absl::SleepFor(sleep_timer);

  // Second batch of proposals, one node is not up.
  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[0]->Propose(std::to_string(num_proposals + i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[leader_id]->Propose(std::to_string(num_proposals + i),
                                            &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }

  nodes[num_nodes - 1] =
      std::make_unique<witnesskvs::paxos::Paxos>((num_nodes - 1));
  absl::SleepFor(sleep_timer);

  // Third batch of proposals, all nodes are up again.
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[0]->Propose(std::to_string(2 * num_proposals + i),
                                    &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[leader_id]->Propose(
        std::to_string(2 * num_proposals + i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }

  // Log must reflect the operations from all three batch of proposals.
  VerifyLogIntegrity(nodes, 3 * num_proposals);
}

TEST_F(PaxosSanity, BasicMultiPaxosSanity) {
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  const size_t num_nodes = 3;
  std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>> nodes(num_nodes);
  uint8_t leader_id;
  uint64_t first_proposal_number = 0;
  uint64_t second_proposal_number = 0;
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<witnesskvs::paxos::Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  const size_t num_proposals = 5;
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[num_nodes - 1]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for ( size_t i = 0; i < num_proposals; i++ ) {
    auto status = nodes[leader_id]->Propose(std::to_string( i ), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  std::map<uint64_t, witnesskvs::paxos::ReplicatedLogEntry> log =
      nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), num_proposals);

  first_proposal_number = nodes[0]->GetReplicatedLog()->GetLogEntries().find(0)->second.accepted_proposal_;
  for (size_t i = 0; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_proposal_, first_proposal_number)
        << "Prposal number for log entry mistmatch at index: " << i << " observed: ("
        << entry.accepted_proposal_ << ") and expected: (" << first_proposal_number
        << ")";
  }

  // Mimic leader going away 
  nodes[1].reset();
  absl::SleepFor(sleep_timer);
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[num_nodes - 1]->Propose(std::to_string((num_proposals + i) * 2), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for ( size_t i = 0; i < num_proposals; i++ ) {
    auto status = nodes[leader_id]->Propose(std::to_string((num_proposals + i) * 2), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  log = nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), 2 * num_proposals);

  /*we want to check proposal numbers of second set after leader change
  they should be different than original one*/
  second_proposal_number = nodes[0]->GetReplicatedLog()->GetLogEntries().find(5)->second.accepted_proposal_;
  for (size_t i = 5; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_proposal_, second_proposal_number)
        << "Prposal number for log entry mistmatch at index: " << i << " observed: ("
        << entry.accepted_proposal_ << ") and expected: (" << second_proposal_number
        << ")";
  }
  ASSERT_NE( first_proposal_number, second_proposal_number )
      << "Prposal number for same after leader change "
      << " observed: (" << second_proposal_number << ") and previous proposal number ("
      << first_proposal_number << ")";
}

TEST_F(PaxosSanity, WitnessNotLeader) {
  using witnesskvs::paxos::Paxos;

  const size_t num_nodes = 3;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(), false);
  ASSERT_EQ(nodes[1]->IsWitness(), false);
  ASSERT_EQ(nodes[1]->IsLeader(), true);
  ASSERT_EQ(nodes[2]->IsWitness(), true);
  ASSERT_EQ(nodes[2]->IsLeader(), false);

  absl::SetFlag(&FLAGS_lower_node_witness, true);
  nodes.clear();
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(), true);
  ASSERT_EQ(nodes[1]->IsLeader(), false);
  ASSERT_EQ(nodes[2]->IsWitness(), false);
  ASSERT_EQ(nodes[2]->IsLeader(), true);
}

TEST_F(PaxosSanity, WitnessCount) {
  using witnesskvs::paxos::Paxos;

  std::string addr = "localhost";
  std::vector<std::string> ports = {"50051", "50052", "50053", "50054",
                                    "50055"};

  char filename[] = "/tmp/nodes_config_5nodes";
  absl::SetFlag(&FLAGS_paxos_node_config_file, filename);
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < ports.size(); i++) {
    temp_file << addr << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;
  // absl::SetFlag(&FLAGS_paxos_node_config_file, "/tmp/nodes_config_5nodes");
  const size_t num_nodes = 5;
  absl::Duration sleep_timer = absl::Milliseconds(2 * heartbeat_timer);
  std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(), false);
  ASSERT_EQ(nodes[1]->IsWitness(), false);
  ASSERT_EQ(nodes[2]->IsWitness(), false);
  ASSERT_EQ(nodes[2]->IsLeader(), true);
  ASSERT_EQ(nodes[3]->IsWitness(), true);
  ASSERT_EQ(nodes[4]->IsWitness(), true);
  ASSERT_EQ(nodes[4]->IsLeader(), false);

  nodes.clear();

  absl::SleepFor(sleep_timer);

  absl::SetFlag(&FLAGS_lower_node_witness, true);
  for (size_t i = 0; i < num_nodes; i++) {
    nodes[i] = std::make_unique<Paxos>(i);
  }

  absl::SleepFor(sleep_timer);

  ASSERT_EQ(nodes[0]->IsWitness(), true);
  ASSERT_EQ(nodes[1]->IsWitness(), true);
  ASSERT_EQ(nodes[2]->IsWitness(), false);
  ASSERT_EQ(nodes[3]->IsWitness(), false);
  ASSERT_EQ(nodes[4]->IsWitness(), false);
  ASSERT_EQ(nodes[4]->IsLeader(), true);

  temp_file.close();
  ASSERT_EQ(remove(filename), 0);
}
