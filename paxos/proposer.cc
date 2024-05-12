#include "proposer.hh"


void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value )
{
  bool done = false;
  int retry_count = 0;
  uint64_t index = GetIndex() + 1;
  while (!done )
  {
    uint32_t prep_majority_count = 0;
    uint32_t accept_majority_count = 0;
    paxos::PrepareRequest request;
    std::string max_proposal_value = "";
    while ( prep_majority_count < majority_threshold_ ) {
      prep_majority_count = 0;
      request.set_proposal_number( getNextProposalNumber() );
      request.set_index_number( index );

      uint64_t max_proposal_id = 0;

      for ( size_t i = 0; i < stubs.size(); i++ ) {
        paxos::PrepareResponse response;
        grpc::ClientContext context;
        grpc::Status status = stubs[i]->Prepare( &context, request, &response );
        if ( !status.ok() ) {
          std::cerr << "Prepare grpc failed -- Maybe other node is down ?\n";
          continue;
        }
        if ( response.has_accepted_value() ) {
          if ( max_proposal_id < response.accepted_proposal() ) {
            max_proposal_id = response.accepted_proposal();
            max_proposal_value = response.accepted_value();
          }
          ++prep_majority_count;
        } else {
          max_proposal_value = value;
          ++prep_majority_count;
        }
      }
    }

    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number( request.proposal_number() );
    accept_request.set_index_number( request.index_number() );
    accept_request.set_value( max_proposal_value );
    paxos::AcceptResponse accept_response;

    for ( size_t i = 0; i < stubs.size(); i++ ) {
      grpc::ClientContext context;
      grpc::Status status
          = stubs[i]->Accept( &context, accept_request, &accept_response );
      if ( !status.ok() ) {
        std::cerr << "Accept GRPC Failed\n";
        continue;
      } else {
        if ( accept_response.min_proposal() == request.proposal_number() ) {
          ++accept_majority_count;
        }
      }
    }
    if ( accept_majority_count >= majority_threshold_ ) {
      accepted_proposals_[request.index_number()] = max_proposal_value;
      std::cout << "Accepted Proposal number: "
                << accept_response.min_proposal()
                << ", accepted value: " << max_proposal_value
                << ", at index: " << request.index_number() << "\n";
      done = true;
    }
    else if ( retry_count > retry_count_ ) {
      std::cerr << "Failed to reach consensus\n";
    }
  }
}