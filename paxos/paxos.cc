#include "paxos.hh"

Paxos::Paxos(uint8_t node_id) {
  replicated_log_ = std::make_shared<ReplicatedLog>(node_id);
  paxos_node_ = std::make_shared<PaxosNode>(node_id, replicated_log_);

  acceptor_ = std::make_unique<AcceptorService>(
      paxos_node_->GetNodeAddress(node_id), node_id, replicated_log_);
  CHECK_NE(acceptor_, nullptr);
  
  proposer_ = std::make_unique<ProposerService>( paxos_node_->GetLeaderAddress(node_id), 
                                             node_id, replicated_log_, paxos_node_);
  CHECK_NE(proposer_, nullptr);

  paxos_node_->MakeReady();
}

Paxos::~Paxos() {
  acceptor_.reset();
  proposer_.reset();
  replicated_log_.reset();
}

void Paxos::Propose(const std::string& value) {
  CHECK_NE(this->proposer_, nullptr) << "Proposer should not be NULL.";

  if (!paxos_node_->ClusterHasEnoughNodesUp()) {
    // TODO [V]: Fix this with a user specified timeout/deadline for request.
    LOG(WARNING)
        << "Replication not possible, majority of the nodes are not reachable.";
  } else {
    paxos::ProposeRequest request;
    google::protobuf::Empty response;
    request.set_value(value);
    paxos_node_->SendProposeGrpc(request, &response);
  }
}