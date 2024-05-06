#include "proposer.hh"

void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value )
{
  paxos::PrepareRequest request;
  request.set_proposal_number( ++m_roundNumber );
  paxos::PrepareResponse response;
  uint64_t maxProposalId = 0;
  std::string maxProposalValue = "";
  uint32_t majorityCount = 0;
  for ( size_t i = 0; i < stubs.size(); i++ ) {
    grpc::ClientContext context;
    grpc::Status status = stubs[i]->Prepare( &context, request, &response );
    if ( status.ok() && response.has_accepted_value() ) {
      std::cerr << "GRPC Prepare Accepeted...\n";
      if ( response.accepted_proposal() > m_roundNumber ) {
        std::cerr << "Proposal rejected go back to phase 1\n";
      } else if ( response.accepted_proposal() > maxProposalId ) {
        maxProposalValue = response.accepted_value();
        ++majorityCount;
      }
    } else if ( !response.has_accepted_value() ) {
      maxProposalValue = value;
      ++majorityCount;
    } else if ( !status.ok() ) {
      std::cerr << "Prepare GRPC Failed\n";
    }
  }

  if ( majorityCount >= majority_threshold ) {
    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number( request.proposal_number() );
    accept_request.set_value( maxProposalValue );
    paxos::AcceptResponse accept_response;
    for ( size_t i = 0; i < stubs.size(); i++ ) {
      grpc::ClientContext context;
      grpc::Status status = stubs[i]->Accept( &context, accept_request, &accept_response );
      if ( !status.ok() ) {
        std::cerr << "Accept GRPC Failed\n";
      } else {
        std::cerr << "Accepted Proposal number: " << accept_response.min_proposal() << ", accepted value: " << value
                  << "\n";
      }
    }
  }
}
