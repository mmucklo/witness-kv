#include "paxos.hh"

Paxos::Paxos(const std::string& config_file_name, uint8_t node_id) {
  replicated_log_ = std::make_shared<ReplicatedLog>(node_id);
  paxos_node_ =
      std::make_shared<PaxosNode>(config_file_name, node_id, replicated_log_);

  proposer_ = std::make_unique<Proposer>(paxos_node_->GetNumNodes(), node_id,
                                         replicated_log_, paxos_node_);
  CHECK_NE(proposer_, nullptr);

  acceptor_ = std::make_unique<AcceptorService>(
      paxos_node_->GetNodeAddress(node_id), node_id, replicated_log_);
  CHECK_NE(acceptor_, nullptr);

  paxos_node_->MakeReady();
}

Paxos::~Paxos() { acceptor_.reset(); }

void Paxos::Propose(const std::string& value) {
  CHECK_NE(this->proposer_, nullptr) << "Proposer should not be NULL.";

  // absl::MutexLock l( &paxos_mutex_ );
  // if ( this->num_active_acceptors_conns_ < quorum_ ) {
  //  TODO [V]: Fix this with a user specified timeout/deadline for request.
  // LOG( WARNING )
  //    << "Replication not possible, majority of the nodes are not reachable.";
  //}
  // else {
  this->proposer_->Propose(value);
}