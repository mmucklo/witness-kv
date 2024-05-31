#ifndef UTILS_H_
#define UTILS_H_

#include "common.hh"

#define INVALID_NODE_ID UINT8_MAX

struct Node {
  std::string ip_address_;
  int port_;

  bool is_witness_;
  bool is_leader_;
  bool IsWitness() const { return is_witness_; }
  bool IsLeader() const { return is_leader_; }

  std::string GetAddressPortStr() const {
    return this->ip_address_ + ":" + std::to_string(this->port_);
  }
};

bool IsValidNodeId(uint8_t node_id);

// This helper function will parse the node config file specified
// by `config_file_name`.
std::vector<Node> ParseNodesConfig(std::string config_file_name);

#endif  // UTILS_H_