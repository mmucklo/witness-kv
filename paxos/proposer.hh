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
  uint64_t proposal_number_;
  uint8_t node_id_;
  int majority_threshold_;

  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;
  std::vector<bool> is_prepare_needed_;

 public:
  Proposer(int num_acceptors, uint8_t nodeId,
           std::shared_ptr<ReplicatedLog> rlog,
           std::shared_ptr<PaxosNode> paxos_node)
      : majority_threshold_{num_acceptors / 2 + 1},
        node_id_{nodeId},
        proposal_number_{0},
        replicated_log_{rlog},
        paxos_node_{paxos_node},
        is_prepare_needed_(num_acceptors, false) {}
  ~Proposer() = default;

  void Propose(const std::string& value);
  void PreparePhase(paxos_rpc::PrepareRequest& request,
                    std::string& value_for_accept_phase);
  bool AcceptPhase(paxos_rpc::PrepareRequest& request,
                   std::string& value_for_accept_phase,
                   bool is_nop_paxos_round,
                   const std::string& value);
  bool DoPreparePhase() {return std::any_of(is_prepare_needed_.begin(), is_prepare_needed_.end(), [](bool v) { return v; });};
};

}  // namespace witnesskvs::paxos
#endif  // PROPOSER_HH_
