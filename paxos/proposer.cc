#include "proposer.hh"

void Proposer::Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& stubs, const std::string& value )
{
  bool accepted_by_quorum = false;
  while (!accepted_by_quorum) {
    paxos::PrepareRequest request;
    request.set_proposal_number( GetNextProposalNumber() );

    std::string value_for_accept_phase = value;
    uint64_t max_proposal_id = 0;
    uint32_t num_promises = 0;

    for ( size_t i = 0; i < stubs.size(); i++ ) {
      if (!stubs[i]) {
        continue;
      }
      paxos::PrepareResponse response;
      grpc::ClientContext context;
      grpc::Status status = stubs[i]->Prepare( &context, request, &response );
      if (!status.ok()) {
        LOG(WARNING) << "Prepare grpc failed for node: " << i
                     << " with error code: " << status.error_code()
                     << " and error message: " << status.error_message();
        continue;
      }

      num_promises++;
      if (response.has_accepted_value()) {
        if (response.accepted_proposal() > max_proposal_id) {
          max_proposal_id = response.accepted_proposal();
          value_for_accept_phase = response.accepted_value();
        }
      }
    }

    if ( num_promises >= quorum_ ) {
      paxos::AcceptRequest accept_request;
      accept_request.set_proposal_number( request.proposal_number() );
      accept_request.set_value( value_for_accept_phase );
      
      uint32_t num_accepts = 0;
      for ( size_t i = 0; i < stubs.size(); i++ ) {
        if (!stubs[i]) {
          continue;
        }
        grpc::ClientContext context;
        paxos::AcceptResponse accept_response;
        grpc::Status status = stubs[i]->Accept( &context, accept_request, &accept_response );
        if ( !status.ok() ) {
          LOG(WARNING) << "Accept grpc failed for node: " << i
                       << " with error code: " << status.error_code()
                       << " and error message: " << status.error_message();
          continue;
        }
        uint64_t round_number = accept_response.min_proposal() >> 8;
        if (round_number > round_number_) {
          round_number_ = round_number;
          break;
        }
        else {
          num_accepts++;
          LOG(INFO) << "Accepted proposal number: " << accept_response.min_proposal()
                    << " and accepted value: " << value_for_accept_phase;
        }
      }

      if (num_accepts >= quorum_) {
        accepted_by_quorum = true;
      }
    }
  }
}