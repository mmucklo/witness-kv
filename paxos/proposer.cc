#include "proposer.hh"

void
Proposer::Propose(const std::vector<std::unique_ptr<paxos::Acceptor::Stub>> &stubs, const std::string &value)
{
#if 0
    paxos::PrepareRequest request;
    request.set_proposal_number( m_roundNumber++ );
    paxos::PrepareResponse response;
    for (size_t i = 0; i < stubs.size(); i++) {
        grpc::ClientContext context;
        grpc::Status status = stubs[i]->Prepare(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "GRPC Prepare failed...\n";
        }
    }
#endif
}