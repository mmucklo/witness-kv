#include "proposer.hh"

#include "absl/flags/flag.h"
#include "absl/log/check.h"

ABSL_FLAG(bool, disable_multi_paxos, false, "Disable multi-paxos optimization");

namespace witnesskvs::paxos {

void Proposer::Propose(const std::string& value) {
  bool is_nop = value.empty();
  bool done = false;
  while (!done) {
    std::string value_for_accept_phase = value;
    paxos_rpc::PrepareRequest request;
    request.set_index(this->replicated_log_->GetFirstUnchosenIdx());
    request.set_proposal_number( proposal_number_ );
    LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
              << "] Attempting replication at index " << request.index();
    if (proposal_number_ == 0 || DoPreparePhase() || absl::GetFlag(FLAGS_disable_multi_paxos)) {
      PreparePhase(request, value_for_accept_phase);
    }

    const bool nop_paxos_round = (is_nop and (value == value_for_accept_phase));
    AcceptPhase(request, value_for_accept_phase, nop_paxos_round, done, value);
  }

}

void Proposer::PreparePhase(paxos_rpc::PrepareRequest& request,
                            std::string& value_for_accept_phase) {
  // Perform phase 1 of the paxos operation i.e. find a proposal that will be
  // accepted by a quorum of acceptors.
  uint32_t num_promises = 0;
  proposal_number_ = this->replicated_log_->GetNextProposalNumber();
  request.set_proposal_number(proposal_number_);
  do {
    uint64_t max_proposal_id = 0;
    for (size_t i = 0; i < this->paxos_node_->GetNumNodes(); i++) {
      paxos_rpc::PrepareResponse response;
      grpc::Status status = this->paxos_node_->PrepareGrpc(
          static_cast<uint8_t>( i ), request, &response);
      if (!status.ok()) {
        LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                     << "] Prepare grpc failed for node: " << i
                     << " with error code: " << status.error_code()
                     << " and error message: " << status.error_message();
        continue;
      }
      if (response.max_chosen_index() > request.index()) {
        LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                  << "] Received max chosen index: " << response.max_chosen_index()
                  << " for index: " << request.index();
        is_prepare_needed_[i] = true;
      }
      else {
        is_prepare_needed_[i] = false;
      }

      if (response.min_proposal() > request.proposal_number()) {
        this->replicated_log_->UpdateProposalNumber(response.min_proposal());
        num_promises = 0;
        LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                  << "] Saw a proposal number larger than what we sent, "
                     "retry Propose operation with a bigger proposal.";
        break;
      }
      if (response.has_accepted_value()) {
        if (max_proposal_id < response.accepted_proposal()) {
          max_proposal_id = response.accepted_proposal();
          value_for_accept_phase = response.accepted_value();
          LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                    << "] Current index already has value: "
                    << value_for_accept_phase;
        }
      }
      ++num_promises;
    }
  } while (num_promises < majority_threshold_);

}

void Proposer::AcceptPhase(paxos_rpc::PrepareRequest& request,
                           std::string& value_for_accept_phase,
                           bool nop_paxos_round, bool& done, const std::string& value) {
  // Perform phase 2 of paxos operation i.e. try to get the value we
  // determined in phase 1 to be accepted by a quorum of acceptors.
  paxos_rpc::AcceptRequest accept_request;
  accept_request.set_proposal_number(request.proposal_number());
  accept_request.set_index(request.index());
  accept_request.set_value(value_for_accept_phase);
  paxos_rpc::AcceptResponse accept_response;
  uint32_t accept_majority_count = 0;
  std::vector<uint64_t> peer_unchosen_idx( this->paxos_node_->GetNumNodes() );

  for (size_t i = 0; i < this->paxos_node_->GetNumNodes(); i++) {
    grpc::Status status = this->paxos_node_->AcceptGrpc(
        static_cast<uint8_t>( i ), accept_request, &accept_response );
    if (!status.ok()) {
      LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                   << "] Accept grpc failed for node: " << i
                   << " with error code: " << status.error_code()
                   << " and error message: " << status.error_message();
      continue;
    }

    peer_unchosen_idx[i] = accept_response.first_unchosen_index();
    if (nop_paxos_round) {continue;}

    if (accept_response.min_proposal() > request.proposal_number()) {
      this->replicated_log_->UpdateProposalNumber(accept_response.min_proposal());
      accept_majority_count = 0;
      LOG(INFO) << "Returning from here:\n";
      break;
    }
    ++accept_majority_count;
    LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
              << "] Got accept from " << i
              << " for proposal number: " << accept_response.min_proposal()
              << " and accepted value: " << value_for_accept_phase;
  }

  // Since a quorum of acceptors responded with an accept for this value,
  // we can mark this entry as chosen.
  if (accept_majority_count >= majority_threshold_) {
    CHECK(!nop_paxos_round)
        << "NODE: [" << static_cast<uint32_t>(node_id_)
        << "] NOP round of paxos should never make log commits.";

    this->replicated_log_->MarkLogEntryChosen(request.index());
    LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] Accepted Proposal number: "
                << accept_response.min_proposal()
                << ", accepted value: " << value_for_accept_phase
                << ", at index: " << request.index() << "\n";

    if (value == value_for_accept_phase) {
      done = true;
      VLOG(1) << "NODE: [" << static_cast<uint32_t>(node_id_)
              << "] Got the value of interest committed: " << value;
    }
  }

  if (done || nop_paxos_round) {
    done = true;
    this->paxos_node_->CommitOnPeerNodes(peer_unchosen_idx);
  }
}
}  // namespace witnesskvs::paxos