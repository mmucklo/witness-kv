#include "paxos.hh"

namespace witnesskvs::paxos {

Paxos::Paxos(uint8_t node_id, std::function<void(std::string)> callback)
    : node_id_{node_id} {
  replicated_log_ = std::make_shared<ReplicatedLog>(node_id, callback);
  paxos_node_ = std::make_shared<PaxosNode>(node_id, replicated_log_);

  acceptor_ = std::make_unique<AcceptorService>(
      paxos_node_->GetNodeAddress(node_id), node_id, replicated_log_);
  CHECK_NE(acceptor_, nullptr);

  proposer_ = std::make_unique<Proposer>(paxos_node_->GetNumNodes(), node_id,
                                         replicated_log_, paxos_node_);
  CHECK_NE(proposer_, nullptr);

  paxos_node_->MakeReady();
}

Paxos::~Paxos() {
  acceptor_.reset();
  proposer_.reset();
  replicated_log_.reset();
}

PaxosResult Paxos::Propose(const std::string& value, uint8_t* leader_node_id,
                           bool is_read) {
  CHECK_NE(this->proposer_, nullptr) << "Proposer should not be NULL.";

  if (!paxos_node_->ClusterHasEnoughNodesUp()) {
    // TODO [V]: Fix this with a user specified timeout/deadline for request.
    LOG(WARNING)
        << "Replication not possible, majority of the nodes are not reachable.";
    return PAXOS_ERROR_NO_QUORUM;
  }

  if (!IsLeader()) {
    if (!leader_node_id) {
      return PAXOS_ERROR_NOT_PERMITTED;
    }
    *leader_node_id = paxos_node_->GetLeaderNodeId();
    if (!IsValidNodeId(*leader_node_id)) {
      LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                   << "] Leader is not ready.";
      return PAXOS_ERROR_LEADER_NOT_READY;
    } else {
      LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] is not the leader.";
      return PAXOS_ERROR_NOT_PERMITTED;
    }
  }

  if (!is_read) {
    proposer_->Propose(value);
  }

  return PAXOS_OK;
}

std::ostream& operator<<(std::ostream& os, PaxosResult error) {
  switch (error) {
    case PaxosResult::PAXOS_OK:
      os << "PAXOS_OK";
      break;
    case PaxosResult::PAXOS_ERROR_NOT_PERMITTED:
      os << "PAXOS_ERROR_NOT_PERMITTED";
      break;
    case PaxosResult::PAXOS_ERROR_LEADER_NOT_READY:
      os << "PAXOS_ERROR_LEADER_NOT_READY";
      break;
    case PaxosResult::PAXOS_ERROR_NO_QUORUM:
      os << "PAXOS_ERROR_NO_QUORUM";
      break;
    default:
      LOG(FATAL) << "Unrecognized error returned by paxos";
  }
  return os;
}

}  // namespace witnesskvs::paxos
