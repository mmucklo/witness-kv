#include "proposer.hh"


void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value, const uint64_t index )
{
Start:
  paxos::PrepareRequest request;
  request.set_proposal_number( getNextProposalNumber() );
  request.set_index_number( index );

  uint64_t maxProposalId = 0;
  std::string maxProposalValue = "";
  uint32_t majorityCount = 0;

  for ( size_t i = 0; i < stubs.size(); i++ ) {
    paxos::PrepareResponse response;
    grpc::ClientContext context;
    grpc::Status status = stubs[i]->Prepare( &context, request, &response );
    if (!status.ok()) {
      std::cerr << "Prepare grpc failed -- Maybe other node is down ?\n";
      continue;
    }

    if (response.has_accepted_value()) {
      maxProposalValue = response.accepted_value();
      ++majorityCount;
    }
    else {
      maxProposalValue = value;
      ++majorityCount;
    }
  }

  if ( majorityCount >= m_majorityThreshold ) {
    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number( request.proposal_number() );
    accept_request.set_index_number( request.index_number() );
    accept_request.set_value( maxProposalValue );

    for ( size_t i = 0; i < stubs.size(); i++ ) {
      grpc::ClientContext context;
      paxos::AcceptResponse accept_response;
      grpc::Status status = stubs[i]->Accept( &context, accept_request, &accept_response );
      if ( !status.ok() ) {
        std::cerr << "Accept GRPC Failed\n";
        continue;
      } 
      else {
        std::cout << "Accepted Proposal number: " << accept_response.min_proposal() << ", accepted value: " << maxProposalValue << ", at index: " << request.index_number()
                << "\n";
      }
    }
  }
  else
  {
      // FIXME: modularize it later
      goto Start;
  }
}
