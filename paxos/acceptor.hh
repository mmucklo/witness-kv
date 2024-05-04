#ifndef __acceptor_hh__
#define __acceptor_hh__

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <optional>
#include <mutex>

#include "paxos.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::ServerContext;
using grpc::Status;

using paxos::Acceptor;
using paxos::PrepareRequest;
using paxos::PrepareResponse;
using paxos::AcceptRequest;
using paxos::AcceptResponse;

class AcceptorService final : public Acceptor::Service {
private:
    uint64_t m_minProposal;
    std::optional<uint64_t> m_acceptedProposal;
    std::optional<std::string> m_acceptedValue;
    // TODO: FIXME - For now we can use a terminal mutex.
    std::mutex m_mutex;

public:
    AcceptorService() : m_minProposal{0}, m_mutex{} {    
    }
    ~AcceptorService() = default;

    Status Prepare(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) override;
    Status Accept(ServerContext* context, const AcceptRequest* request, AcceptResponse* response) override;
};

#endif // __acceptor_hh__