#ifndef PAXOS_HH_
#define PAXOS_HH_

#include "acceptor.hh"
#include "common.hh"
#include "node.hh"
#include "proposer.hh"
#include "replicated_log.hh"

namespace witnesskvs::paxoslibrary {

class Paxos {
 private:
  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;
  std::unique_ptr<AcceptorService> acceptor_;
  std::unique_ptr<ProposerService> proposer_;

 public:
  Paxos(uint8_t node_id);
  ~Paxos();

  // Function to add a command to the replicated state machine across all alive
  // Paxos nodes. If value is empty this will trigger a NOP paxos round as
  // described in section 3 in
  // https://lamport.azurewebsites.net/pubs/paxos-simple.pdf
  void Propose(const std::string& value);

  // Helper functions for unit testing.
  std::shared_ptr<ReplicatedLog>& GetReplicatedLog() { return replicated_log_; }
  bool IsLeader() { return paxos_node_->IsLeader(); }
  bool IsWitness() { return paxos_node_->IsWitness(); }
};

}  // namespace witnesskvs::paxoslibrary
#endif  // PAXOS_HH_