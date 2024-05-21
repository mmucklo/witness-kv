#ifndef PAXOS_HH_
#define PAXOS_HH_

#include "acceptor.hh"
#include "common.hh"
#include "node.hh"
#include "proposer.hh"
#include "replicated_log.hh"

class Paxos
{
 private:
  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;
  std::unique_ptr<Proposer> proposer_;
  std::unique_ptr<AcceptorService> acceptor_;

 public:
  Paxos( const std::string& config_file_name, uint8_t node_id );
  ~Paxos();

  void Propose( const std::string& value );

  // Helper functions for unit testing.
  std::shared_ptr<ReplicatedLog>& GetReplicatedLog() { return replicated_log_; }
};

#endif  // PAXOS_HH_