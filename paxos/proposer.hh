#ifndef PROPOSER_HH_
#define PROPOSER_HH_

#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "replicated_log.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/server_builder.h>

#include "node.hh"

namespace witnesskvs::paxos {

class Proposer {
 private:
  uint8_t node_id_;
  int majority_threshold_;

  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;

 public:
  Proposer(int num_acceptors, uint8_t nodeId,
           std::shared_ptr<ReplicatedLog> rlog,
           std::shared_ptr<PaxosNode> paxos_node)
      : majority_threshold_{num_acceptors / 2 + 1},
        node_id_{nodeId},
        replicated_log_{rlog},
        paxos_node_{paxos_node} {}
  ~Proposer() = default;

  void Propose(const std::string& value);
};

}  // namespace witnesskvs::paxos
#endif  // PROPOSER_HH_