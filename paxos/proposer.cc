#include "proposer.hh"


void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value )
{
  bool bDone = false;
  int retryCount = 0;
  uint64_t index = GetIndex() + 1;
  while (!bDone )
  {
    uint32_t prepMajorityCount = 0;
    uint32_t acceptMajorityCount = 0;
    paxos::PrepareRequest request;
    std::string maxProposalValue = "";
    while ( prepMajorityCount < m_majorityThreshold ) {
      prepMajorityCount = 0;
      request.set_proposal_number( getNextProposalNumber() );
      request.set_index_number( index );

      uint64_t maxProposalId = 0;

      for ( size_t i = 0; i < stubs.size(); i++ ) {
        paxos::PrepareResponse response;
        grpc::ClientContext context;
        grpc::Status status = stubs[i]->Prepare( &context, request, &response );
        if ( !status.ok() ) {
          std::cerr << "Prepare grpc failed -- Maybe other node is down ?\n";
          continue;
        }
        if ( response.has_accepted_value() ) {
          if ( maxProposalId < response.accepted_proposal() ) {
            maxProposalId = response.accepted_proposal();
            maxProposalValue = response.accepted_value();
          }
          ++prepMajorityCount;
        } else {
          maxProposalValue = value;
          ++prepMajorityCount;
        }
      }
    }

    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number( request.proposal_number() );
    accept_request.set_index_number( request.index_number() );
    accept_request.set_value( maxProposalValue );
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
          ++acceptMajorityCount;
        }
      }
    }
    if ( acceptMajorityCount >= m_majorityThreshold ) {
      m_acceptedProposals[request.index_number()] = maxProposalValue;
      std::cout << "Accepted Proposal number: "
                << accept_response.min_proposal()
                << ", accepted value: " << maxProposalValue
                << ", at index: " << request.index_number() << "\n";
      bDone = true;
    }
    else if ( retryCount > m_retryCount ) {
      std::cerr << "Failed to reach consensus\n";
    }
  }
}