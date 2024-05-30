#ifndef PAXOS_HH_
#define PAXOS_HH_

#include "acceptor.hh"
#include "common.hh"
#include "node.hh"
#include "proposer.hh"
#include "replicated_log.hh"

namespace witnesskvs::paxos {
enum PaxosResult {
  PAXOS_OK = 1,
  PAXOS_ERROR_NOT_PERMITTED = 2,
  PAXOS_ERROR_LEADER_NOT_READY = 3,
  PAXOS_ERROR_NO_QUORUM = 4,
};

std::ostream& operator<<(std::ostream& os, PaxosResult error);

class Paxos {
 private:
  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;
  std::unique_ptr<AcceptorService> acceptor_;
  std::unique_ptr<Proposer> proposer_;
  uint8_t node_id_;

 public:
  Paxos(uint8_t node_id, std::function<void(std::string)> callback = nullptr);
  ~Paxos();

  // Function to add a command to the replicated state machine across all alive
  // Paxos nodes. If value is empty this will trigger a NOP paxos round as
  // described in section 3 in
  // https://lamport.azurewebsites.net/pubs/paxos-simple.pdf
  PaxosResult Propose(const std::string& value,
                      uint8_t* leader_node_id = nullptr, bool is_read = false);

  // Helper functions for unit testing.
  std::shared_ptr<ReplicatedLog>& GetReplicatedLog() { return replicated_log_; }
  bool IsLeader() { return paxos_node_->IsLeader(); }
  bool IsWitness() { return paxos_node_->IsWitness(); }
};
}  // namespace witnesskvs::paxos
#endif  // PAXOS_HH_