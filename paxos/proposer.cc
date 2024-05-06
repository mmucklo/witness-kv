#include "proposer.hh"


void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value )
{
Start:
  paxos::PrepareRequest request;
  request.set_proposal_number( getNextProposalNumber() );

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
      std::cout << "[PROPOSER] : Got value from peer: " << response.accepted_value() << "\n";
      maxProposalValue = response.accepted_value();
      ++majorityCount;
    }
    else {
      std::cout << "[PROPOSER] : Will attempt value: " << response.accepted_value() << "\n";
      maxProposalValue = value;
      ++majorityCount;
    }

    /*if ( status.ok() && response.has_accepted_value() ) {
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
    }*/
  }

  if ( majorityCount >= m_majorityThreshold ) {
    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number( request.proposal_number() );
    accept_request.set_value( maxProposalValue );
    paxos::AcceptResponse accept_response;
    for ( size_t i = 0; i < stubs.size(); i++ ) {
      grpc::ClientContext context;
      grpc::Status status = stubs[i]->Accept( &context, accept_request, &accept_response );
      if ( !status.ok() ) {
        std::cerr << "Accept GRPC Failed\n";
      } 
      else {
        uint64_t roundNumber = accept_response.min_proposal() >> 8;
        std::cout << "Round number obtained.. " << roundNumber << "\n";
        if (roundNumber > m_roundNumber) {
          m_roundNumber = roundNumber;
          // FIXME: Ugly.
          goto Start;
        }
        else {
          std::cerr << "Accepted Proposal number: " << accept_response.min_proposal() << ", accepted value: " << maxProposalValue
                  << "\n";
        }
      }
    }
  }
}
