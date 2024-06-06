#ifndef UTIL_NODE_H_
#define UTIL_NODE_H_

#include <memory>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

#define INVALID_NODE_ID UINT8_MAX

class Node {
 public:
  Node(std::string ip_address, int port)
      : ip_address_(std::move(ip_address)), port_(port) {}

  bool IsWitness() const {
    absl::MutexLock l(&lock_);
    return is_witness_;
  }
  bool IsLeader() const {
    absl::MutexLock l(&lock_);
    return is_leader_;
  }
  void SetIsWitness(bool is_witness) {
    absl::MutexLock l(&lock_);
    is_witness_ = is_witness;
  }
  void SetIsLeader(bool is_leader) {
    absl::MutexLock l(&lock_);
    is_leader_ = is_leader;
  }

  std::string ip_address() { return ip_address_; }

  int port() { return port_; }

  std::string GetAddressPortStr() const {
    return this->ip_address_ + ":" + std::to_string(this->port_);
  }

 private:
  mutable absl::Mutex lock_;
  bool is_witness_ ABSL_GUARDED_BY(lock_);
  bool is_leader_ ABSL_GUARDED_BY(lock_);
  const std::string ip_address_;
  const int port_;
};

bool IsValidNodeId(uint8_t node_id);

// This helper function will parse the node config file specified
// by `config_file_name`.
std::vector<std::unique_ptr<Node>> ParseNodesConfig(
    std::string config_file_name);

std::vector<std::unique_ptr<Node>> ParseNodesList(
    const std::vector<std::string>& node_list);

#endif  // UTIL_NODE_H_
