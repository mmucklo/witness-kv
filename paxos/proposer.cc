#include "proposer.hh"

#include "absl/log/check.h"

void Proposer::Propose(const std::string& value) {
  bool done = false;
  int retry_count = 0;
  while (!done && retry_count < retry_count_) {
    std::string value_for_accept_phase = value;
    uint32_t num_promises = 0;

    // Perform phase 1 of the paxos operation i.e. find a proposal that will be
    // accepted by a quorum of acceptors.
    paxos::PrepareRequest request;
    request.set_index(this->replicated_log_->GetFirstUnchosenIdx());
    LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
              << "] Attempting replication at index " << request.index();
    do {
      num_promises = 0;
      request.set_proposal_number(
          this->replicated_log_->GetNextProposalNumber());

      uint64_t max_proposal_id = 0;
      for (size_t i = 0; i < this->node_grpc_->GetNumNodes(); i++) {
        paxos::PrepareResponse response;
        grpc::Status status = this->node_grpc_->PrepareGrpc(
            static_cast<uint8_t>(i), request, &response);
        if (!status.ok()) {
          LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                       << "] Prepare grpc failed for node: " << i
                       << " with error code: " << status.error_code()
                       << " and error message: " << status.error_message();
          continue;
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

    // Perform phase 2 of paxos operation i.e. try to get the value we
    // determined in phase 1 to be accepted by a quorum of acceptors.
    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number(request.proposal_number());
    accept_request.set_index(request.index());
    accept_request.set_value(value_for_accept_phase);
    paxos::AcceptResponse accept_response;
    uint32_t accept_majority_count = 0;
    std::vector<uint64_t> peer_unchosen_idx(this->node_grpc_->GetNumNodes());

    for (size_t i = 0; i < this->node_grpc_->GetNumNodes(); i++) {
      grpc::Status status = this->node_grpc_->AcceptGrpc(
          static_cast<uint8_t>(i), accept_request, &accept_response);
      if (!status.ok()) {
        LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                     << "] Accept grpc failed for node: " << i
                     << " with error code: " << status.error_code()
                     << " and error message: " << status.error_message();
        continue;
      }
      if (accept_response.min_proposal() > request.proposal_number()) {
        this->replicated_log_->UpdateProposalNumber(
            accept_response.min_proposal());
        accept_majority_count = 0;
        break;
      }
      ++accept_majority_count;
      LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] Got accept from " << i
                << " for proposal number: " << accept_response.min_proposal()
                << " and accepted value: " << value_for_accept_phase;

      peer_unchosen_idx[i] = accept_response.first_unchosen_index();
    }

    // Since a quorum of acceptors responded with an accept for this value,
    // we can mark this entry as chosen.
    if (accept_majority_count >= majority_threshold_) {
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
    } else if (retry_count > retry_count_) {
      LOG(ERROR) << "NODE: [" << static_cast<uint32_t>(node_id_)
                 << "] Failed to reach consensus\n";
    }

    if (done) {
      this->node_grpc_->CommitInBackground(peer_unchosen_idx);
    }
  }
}