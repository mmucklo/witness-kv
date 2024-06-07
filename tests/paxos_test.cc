// Gtest header
#include "paxos/paxos.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "log.pb.h"
#include "log/logs_loader.h"
#include "paxos/replicated_log.h"
#include "tests/test_util.h"
#include "util/node.h"

ABSL_DECLARE_FLAG(uint64_t, absl_log_min_level);

ABSL_DECLARE_FLAG(std::string, paxos_log_directory);
ABSL_DECLARE_FLAG(std::string, paxos_log_file_prefix);

ABSL_DECLARE_FLAG(std::string, paxos_node_config_file);
ABSL_DECLARE_FLAG(absl::Duration, paxos_node_heartbeat_interval);
ABSL_DECLARE_FLAG(absl::Duration, paxos_node_truncation_interval);
ABSL_DECLARE_FLAG(bool, paxos_node_truncation_enabled);
ABSL_DECLARE_FLAG(bool, lower_node_witness);

// Simple test to sanity check file parsing logic.
TEST(FileParseTest, ConfigFileParseTest) {
  std::vector<std::string> addrs = {"0.0.0.0", "0.1.2.3", "8.7.6.5",
                                    "10.10.10.10"};
  std::vector<std::string> ports = {"10", "20", "30", "40"};
  ASSERT_EQ(addrs.size(), ports.size());

  char filename[] = "/var/tmp/paxos_config_file_test";
  // absl::SetFlag(&FLAGS_paxos_node_config_file,
  // "/var/tmp/paxos_config_file_test");
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < addrs.size(); i++) {
    temp_file << addrs[i] << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;

  std::vector<std::unique_ptr<Node>> nodes = ParseNodesConfig(filename);
  ASSERT_EQ(addrs.size(), nodes.size());

  for (size_t i = 0; i < addrs.size(); i++) {
    ASSERT_EQ(addrs[i], nodes[i]->ip_address());
    ASSERT_EQ(std::stoi(ports[i]), nodes[i]->port());
  }

  temp_file.close();
  ASSERT_EQ(remove(filename), 0);
}

TEST(FileParseTest, ConfigStrTest) {
  std::vector<std::string> addrs = {"0.0.0.0", "0.1.2.3", "8.7.6.5",
                                    "10.10.10.10"};
  std::vector<std::string> ports = {"10", "20", "30", "40"};
  ASSERT_EQ(addrs.size(), ports.size());

  std::vector<std::string> addr_ports;
  for (size_t i = 0; i < addrs.size(); i++) {
    addr_ports.push_back(absl::StrCat(addrs[i], ":", ports[i]));
  }

  std::vector<std::unique_ptr<Node>> nodes = ParseNodesList(addr_ports);
  ASSERT_EQ(addrs.size(), nodes.size());
  for (size_t i = 0; i < addrs.size(); i++) {
    ASSERT_EQ(addrs[i], nodes[i]->ip_address());
    ASSERT_EQ(std::stoi(ports[i]), nodes[i]->port());
  }
}

class PaxosSanity : public ::testing::Test {
 public:
  static constexpr uint64_t heartbeat_timer = 300;  // ms
  // Setup up abseil flags for paxos library to suit these tests.
  // Log directory and prefix are chosen such that they do not collide with
  // existing default logs.
  // Heartbeat timer is set so that the tests can run faster.
  virtual void SetUp() override {
    absl::SetFlag(&FLAGS_paxos_node_heartbeat_interval,
                  absl::Milliseconds(heartbeat_timer));
    absl::SetFlag(&FLAGS_paxos_log_directory, "/var/tmp");
    absl::SetFlag(&FLAGS_paxos_log_file_prefix, "paxos_sanity_test");
    witnesskvs::test::Cleanup(absl::GetFlag(FLAGS_paxos_log_directory),
                              absl::GetFlag(FLAGS_paxos_log_file_prefix));
    // Effectively disable the background truncation loop.
    absl::SetFlag(&FLAGS_paxos_node_truncation_interval, absl::Seconds(86400));
  }

  // Cleanup all log files we may have created.
  virtual void TearDown() override {
    witnesskvs::test::Cleanup(absl::GetFlag(FLAGS_paxos_log_directory),
                              absl::GetFlag(FLAGS_paxos_log_file_prefix));
  }

  // Helper function to check replicated log on all nodes.
  void VerifyLogIntegrity(
      const std::vector<std::unique_ptr<witnesskvs::paxos::Paxos>>& nodes,
      size_t total_proposals) {
    std::vector<std::map<uint64_t, witnesskvs::paxos::ReplicatedLogEntry>> logs(
        nodes.size());
    for (size_t i = 0; i < nodes.size(); i++) {
      if (nodes[i]) {
        logs[i] = nodes[i]->GetReplicatedLog()->GetLogEntries();
        ASSERT_EQ(logs[i].size(), total_proposals) << " node: " << i;
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

    // Logs should be complete.
    for (size_t i = 0; i < nodes.size(); i++) {
      witnesskvs::log::SortingLogsLoader logs_loader(
          absl::GetFlag(FLAGS_paxos_log_directory),
          absl::StrCat(absl::GetFlag(FLAGS_paxos_log_file_prefix), i),
          witnesskvs::paxos::GetLogSortFn());
      // Make sure there's one accept per proposal.
      absl::flat_hash_set<uint64_t> accepts;
      for (const Log::Message& msg : logs_loader) {
        if (msg.paxos().accepted_proposal() > 0 || msg.paxos().is_chosen()) {
          accepts.insert(msg.paxos().idx());
        }
      }
      // Make sure a log message exists for every proposal.
      CHECK_EQ(accepts.size(), total_proposals) << "node: " << i;
    }
    if (absl::GetFlag(FLAGS_paxos_node_truncation_enabled)) {
      for (size_t i = 0; i < nodes.size(); i++) {
        nodes[i]->RunTruncationOnce();
        // TODO(mmucklo): somehow instrument logs_truncator so we can know in
        // tests when a truncation has taken place.
      }
      absl::SleepFor(absl::Seconds(1));
    }

    for (size_t i = 0; i < nodes.size(); i++) {
      witnesskvs::log::SortingLogsLoader logs_loader(
          absl::GetFlag(FLAGS_paxos_log_directory),
          absl::StrCat(absl::GetFlag(FLAGS_paxos_log_file_prefix), i),
          witnesskvs::paxos::GetLogSortFn());
      // Make sure there's one accept per proposal.
      absl::flat_hash_set<uint64_t> accepts;
      for (const Log::Message& msg : logs_loader) {
        if (msg.paxos().accepted_proposal() > 0 || msg.paxos().is_chosen()) {
          accepts.insert(msg.paxos().idx());
        }
      }
      // Make sure a log message exists for every proposal.
      CHECK_EQ(accepts.size(), 1) << "node: " << i;
    }

    // Make sure the log was truncated as well.
    for (size_t i = 0; i < nodes.size(); i++) {
      if (nodes[i]) {
        logs[i] = nodes[i]->GetReplicatedLog()->GetLogEntries();
        ASSERT_EQ(logs[i].size(), 1) << " node: " << i;
      }
    }
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
    auto status =
        nodes[num_nodes - 1]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status =
        nodes[leader_id]->Propose(std::to_string(i), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  std::map<uint64_t, witnesskvs::paxos::ReplicatedLogEntry> log =
      nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), num_proposals);

  first_proposal_number = nodes[0]
                              ->GetReplicatedLog()
                              ->GetLogEntries()
                              .find(0)
                              ->second.accepted_proposal_;
  for (size_t i = 0; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_proposal_, first_proposal_number)
        << "Prposal number for log entry mistmatch at index: " << i
        << " observed: (" << entry.accepted_proposal_ << ") and expected: ("
        << first_proposal_number << ")";
  }

  // Mimic leader going away
  nodes[1].reset();
  absl::SleepFor(sleep_timer);
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[num_nodes - 1]->Propose(
        std::to_string((num_proposals + i) * 2), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[leader_id]->Propose(
        std::to_string((num_proposals + i) * 2), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  log = nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), 2 * num_proposals);

  /*we want to check proposal numbers of second set after leader change
  they should be different than original one*/
  second_proposal_number = nodes[0]
                               ->GetReplicatedLog()
                               ->GetLogEntries()
                               .find(5)
                               ->second.accepted_proposal_;
  for (size_t i = 5; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_proposal_, second_proposal_number)
        << "Prposal number for log entry mistmatch at index: " << i
        << " observed: (" << entry.accepted_proposal_ << ") and expected: ("
        << second_proposal_number << ")";
  }
  ASSERT_NE(first_proposal_number, second_proposal_number)
      << "Prposal number for same after leader change " << " observed: ("
      << second_proposal_number << ") and previous proposal number ("
      << first_proposal_number << ")";
  nodes[1] = std::make_unique<witnesskvs::paxos::Paxos>(1);
  absl::SleepFor(sleep_timer);
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[num_nodes - 1]->Propose(
        std::to_string((num_proposals + i) * 2), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[leader_id]->Propose(
        std::to_string((num_proposals + i) * 3), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  log = nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), 3 * num_proposals);

  /*we want to check proposal numbers of second set after leader change
  they should be different than original one*/
  auto third_proposal_number = nodes[0]
                                   ->GetReplicatedLog()
                                   ->GetLogEntries()
                                   .find(10)
                                   ->second.accepted_proposal_;
  for (size_t i = 10; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_proposal_, third_proposal_number)
        << "Prposal number for log entry mistmatch at index: " << i
        << " observed: (" << entry.accepted_proposal_ << ") and expected: ("
        << second_proposal_number << ")";
  }

  nodes[1].reset();
  absl::SleepFor(sleep_timer);
  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[num_nodes - 1]->Propose(
        std::to_string((num_proposals + i) * 4), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_ERROR_NOT_PERMITTED, status);
  }

  for (size_t i = 0; i < num_proposals; i++) {
    auto status = nodes[leader_id]->Propose(
        std::to_string((num_proposals + i) * 4), &leader_id, false);
    ASSERT_EQ(witnesskvs::paxos::PAXOS_OK, status);
  }
  log = nodes[0]->GetReplicatedLog()->GetLogEntries();
  ASSERT_EQ(log.size(), 4 * num_proposals);

  /*we want to check proposal numbers of second set after leader change
  they should be different than original one*/
  auto fourth_proposal_number = nodes[0]
                                    ->GetReplicatedLog()
                                    ->GetLogEntries()
                                    .find(15)
                                    ->second.accepted_proposal_;
  for (size_t i = 15; i < log.size(); i++) {
    auto entry = log.find(i)->second;
    ASSERT_EQ(entry.accepted_proposal_, fourth_proposal_number)
        << "Prposal number for log entry mistmatch at index: " << i
        << " observed: (" << entry.accepted_proposal_ << ") and expected: ("
        << second_proposal_number << ")";
  }
  ASSERT_NE(second_proposal_number, third_proposal_number)
      << "Third Prposal number for same after leader change " << " observed: ("
      << third_proposal_number << ") and previous(second) proposal number ("
      << third_proposal_number << ")";

  ASSERT_NE(third_proposal_number, fourth_proposal_number)
      << "Fourth Prposal number for same after leader change " << " observed: ("
      << third_proposal_number << ") and previous(third) proposal number ("
      << third_proposal_number << ")";
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

  char filename[] = "/var/tmp/nodes_config_5nodes";
  absl::SetFlag(&FLAGS_paxos_node_config_file, filename);
  std::ofstream temp_file(filename);
  ASSERT_TRUE(temp_file.is_open()) << "Failed to create temporary file\n";

  for (size_t i = 0; i < ports.size(); i++) {
    temp_file << addr << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;
  // absl::SetFlag(&FLAGS_paxos_node_config_file,
  // "/var/tmp/nodes_config_5nodes");
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

struct ReplicatedLogTest : public ::testing::Test {
  std::unique_ptr<witnesskvs::paxos::ReplicatedLog> log;

  virtual void SetUp() override {
    absl::SetFlag(&FLAGS_paxos_log_directory, "/var/tmp");
    absl::SetFlag(&FLAGS_paxos_log_file_prefix, "paxos_sanity_test");

    log = std::make_unique<witnesskvs::paxos::ReplicatedLog>(0);
  }

  virtual void TearDown() override {}
};

TEST_F(PaxosSanity, BasicLogTest) {
  std::unique_ptr<witnesskvs::paxos::ReplicatedLog> log =
      std::make_unique<witnesskvs::paxos::ReplicatedLog>(0);
  uint64_t proposal_number = 0;
  uint64_t num_idx = 10;

  for (uint64_t i = 0; i < num_idx; i++) {
    uint64_t proposal = log->GetNextProposalNumber();
    ASSERT_GT(proposal, proposal_number);
    proposal_number = proposal;

    witnesskvs::paxos::ReplicatedLogEntry entry = {};
    entry.idx_ = i;
    entry.min_proposal_ = proposal;
    entry.accepted_proposal_ = proposal;
    entry.accepted_value_ = std::to_string(i);
    entry.is_chosen_ = false;

    ASSERT_EQ(log->UpdateLogEntry(entry), proposal);
  }

  ASSERT_EQ(log->GetFirstUnchosenIdx(), 0);

  for (uint64_t i = 0; i < num_idx; i += 2) {
    log->MarkLogEntryChosen(i);
  }

  ASSERT_EQ(log->GetFirstUnchosenIdx(), 1);

  for (uint64_t i = 1; i < num_idx; i += 2) {
    log->MarkLogEntryChosen(i);

    uint64_t expected_idx = (i == (num_idx - 1)) ? num_idx : (i + 2);
    ASSERT_EQ(log->GetFirstUnchosenIdx(), expected_idx);
  }
}

TEST(ProposalNumberTest, BasicProposalNumberTest) {
  std::unique_ptr<witnesskvs::paxos::ReplicatedLog> log =
      std::make_unique<witnesskvs::paxos::ReplicatedLog>(0);

  uint64_t proposal_number = 10;
  uint64_t num_proposals = 10;

  for (uint64_t i = 0; i < num_proposals; i++) {
    log->UpdateProposalNumber(proposal_number);
    uint64_t proposal = log->GetNextProposalNumber();
    ASSERT_GT(proposal, proposal_number)
        << "The Next proposal number should never be less than the previously "
           "updated proposal number";

    proposal_number = proposal * num_proposals;
  }
}