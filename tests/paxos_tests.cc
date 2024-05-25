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
ABSL_DECLARE_FLAG(uint64_t, paxos_node_heartbeat);

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
    for (size_t i = 0; i < addrs.size(); i++) {
      temp_file << addrs[i] << ":" << ports[i] << "\n";
    }
    temp_file << std::endl;

    auto nodes = ParseNodesConfig();
    ASSERT_EQ(addrs.size(), nodes.size());

    for (size_t i = 0; i < addrs.size(); i++) {
      ASSERT_EQ(addrs[i], nodes[i].ip_address_);
      ASSERT_EQ(std::stoi(ports[i]), nodes[i].port_);
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

    static constexpr uint64_t heartbeat_timer = 1;

    struct PaxosSanity : public ::testing::Test {
      virtual void SetUp() override {
        absl::SetFlag(&FLAGS_paxos_node_heartbeat, heartbeat_timer);
        absl::SetFlag(&FLAGS_paxos_log_directory, "/tmp");
        absl::SetFlag(&FLAGS_paxos_log_file_prefix, "paxos_sanity_test");
      }

      virtual void TearDown() override {
        std::string log_files = absl::GetFlag(FLAGS_paxos_log_directory) + "/" +
                                absl::GetFlag(FLAGS_paxos_log_file_prefix) +
                                "*";

        std::string command = "rm -f " + log_files;
        CHECK_EQ(system(command.c_str()), 0);
      }
    };

    TEST_F(PaxosSanity, ReplicatedLogSanity) {
      const size_t num_nodes = 3;
      const int sleep_timer = 2 * heartbeat_timer;
      std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
      for (size_t i = 0; i < num_nodes; i++) {
        nodes[i] = std::make_unique<Paxos>(i);
      }

      sleep(sleep_timer);

      const size_t num_proposals = 10;
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[0]->Propose(std::to_string(i));
      }

      VerifyLogIntegrity(nodes, num_proposals);
    }

    TEST_F(PaxosSanity, BasicStableLogSanity) {
      const size_t num_nodes = 3;
      const int sleep_timer = 2 * heartbeat_timer;
      std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
      for (size_t i = 0; i < num_nodes; i++) {
        nodes[i] = std::make_unique<Paxos>(i);
      }

      sleep(sleep_timer);

      const size_t num_proposals = 5;
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[num_nodes - 1]->Propose(std::to_string(i));
      }

      // Mimic node 0 going away and coming back up.
      nodes[0].reset();
      nodes[0] = std::make_unique<Paxos>(0);
      sleep(sleep_timer);

      // After node 0 comes back up it should have all the committed entires
      // intact.
      std::map<uint64_t, ReplicatedLogEntry> log =
          nodes[0]->GetReplicatedLog()->GetLogEntries();
      CHECK_EQ(log.size(), num_proposals);

      for (size_t i = 0; i < log.size(); i++) {
        auto entry = log.find(i)->second;
        CHECK_EQ(entry.accepted_value_, std::to_string(i))
            << "Log values do not match at index: " << i << " observed: ("
            << entry.accepted_value_ << ") and expected: (" << std::to_string(i)
            << ")";
        CHECK(entry.is_chosen_);
      }

      // Make node 0 propose some values and verify that all nodes see the same
      // log.
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[0]->Propose(std::to_string(num_proposals + i));
      }

      VerifyLogIntegrity(nodes, 2 * num_proposals);
    }

    TEST_F(PaxosSanity, ReplicatedLogAfterNodeReconnection) {
      const size_t num_nodes = 3;
      const int sleep_timer = 2 * heartbeat_timer;
      std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
      for (size_t i = 0; i < num_nodes; i++) {
        nodes[i] = std::make_unique<Paxos>(i);
      }

      sleep(sleep_timer);

      const size_t num_proposals = 5;
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[num_nodes - 1]->Propose(std::to_string(i));
      }

      // Mimic node 0 going away and coming back up.
      nodes[0].reset();
      nodes[0] = std::make_unique<Paxos>(0);
      sleep(sleep_timer);

      for (size_t i = 0; i < num_proposals; i++) {
        nodes[num_nodes - 1]->Propose(std::to_string(num_proposals + i));
      }

      VerifyLogIntegrity(nodes, 2 * num_proposals);
    }

    TEST_F(PaxosSanity, ReplicatedLogWhenOneNodeIsDown) {
      const size_t num_nodes = 3;
      const int sleep_timer = 2 * heartbeat_timer;
      std::vector<std::unique_ptr<Paxos>> nodes(num_nodes);
      for (size_t i = 0; i < num_nodes; i++) {
        nodes[i] = std::make_unique<Paxos>(i);
      }

      sleep(sleep_timer);

      const size_t num_proposals = 5;

      // First batch of proposals, all nodes are up.
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[0]->Propose(std::to_string(i));
      }

      // Mimic node going away and some operations happening while node is down
      // and then node comes back up.
      nodes[num_nodes - 1].reset();
      sleep(sleep_timer);

      // Second batch of proposals, one node is not up.
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[0]->Propose(std::to_string(num_proposals + i));
      }

      nodes[num_nodes - 1] = std::make_unique<Paxos>((num_nodes - 1));
      sleep(sleep_timer);

      // Third batch of proposals, all nodes are up again.
      for (size_t i = 0; i < num_proposals; i++) {
        nodes[0]->Propose(std::to_string(2 * num_proposals + i));
      }

      // Log must reflect the operations from all three batch of proposals.
      VerifyLogIntegrity(nodes, 3 * num_proposals);
    }