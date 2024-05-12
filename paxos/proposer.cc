#include "proposer.hh"
#include "absl/log/check.h"


void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value )
{
  bool done = false;
  int retry_count = 0;
  uint64_t index = first_uncommited_index_;
  while (!done && retry_count < retry_count_) {
    uint32_t prep_majority_count = 0;
    uint32_t accept_majority_count = 0;
    paxos::PrepareRequest request;
    std::string max_proposal_value = "";
    while ( prep_majority_count < majority_threshold_ ) {
      prep_majority_count = 0;
      request.set_proposal_number( getNextProposalNumber() );
      request.set_index( index );

      uint64_t max_proposal_id = 0;

      for ( size_t i = 0; i < stubs.size(); i++ ) {
        paxos::PrepareResponse response;
        grpc::ClientContext context;
        grpc::Status status = stubs[i]->Prepare( &context, request, &response );
        if ( !status.ok() ) {
          LOG( WARNING ) << "Prepare grpc failed for node: " << i
                         << " with error code: " << status.error_code()
                         << " and error message: " << status.error_message();
          continue;
        }
        if ( response.has_accepted_value() ) {
          if ( max_proposal_id < response.accepted_proposal() ) {
            max_proposal_id = response.accepted_proposal();
            max_proposal_value = response.accepted_value();
          }
          ++prep_majority_count;
        } else if ( response.accepted_proposal() <= request.proposal_number()) {
          max_proposal_value = value;
          ++prep_majority_count;
        }
        
      }
    }

    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number( request.proposal_number() );
    accept_request.set_index( request.index() );
    accept_request.set_value( max_proposal_value );
    paxos::AcceptResponse accept_response;

    for ( size_t i = 0; i < stubs.size(); i++ ) {
      grpc::ClientContext context;
      grpc::Status status
          = stubs[i]->Accept( &context, accept_request, &accept_response );
      if ( !status.ok() ) {
        LOG( WARNING ) << "Accept grpc failed for node: " << i
                       << " with error code: " << status.error_code()
                       << " and error message: " << status.error_message();
        continue;
      } else {
        if ( accept_response.min_proposal() > request.proposal_number() ) {
          accept_majority_count = 0;
          break;
        }
        ++accept_majority_count;
        LOG(INFO) << "Got accept for proposal number: " << accept_response.min_proposal()
                  << " and accepted value: " << max_proposal_value;
      }
    }
    if ( accept_majority_count >= majority_threshold_ ) {
      accepted_proposals_[request.index()] = max_proposal_value;
      ++first_uncommited_index_;
      LOG(INFO) << "Accepted Proposal number: "
                << accept_response.min_proposal()
                << ", accepted value: " << max_proposal_value
                << ", at index: " << request.index() << "\n";
      index = first_uncommited_index_;
      if ( value == max_proposal_value) {
          done = true;
      }
    }
    else if ( retry_count > retry_count_ ) {
      LOG(ERROR) << "Failed to reach consensus\n";
    }
  }
}