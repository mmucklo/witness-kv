#ifndef __proposer_hh__
#define __proposer_hh__

#include "common.hh"

#include "paxos.grpc.pb.h"
#include "paxos.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

class Proposer {
private:
    uint64_t m_roundNumber;
    int majority_threshold;

public:
    Proposer(int num_acceptors)
        : majority_threshold{num_acceptors / 2 + 1},
          m_roundNumber{0} { }
    ~Proposer() = default;

    void Propose(const std::vector<std::unique_ptr<paxos::Acceptor::Stub>> &m_acceptorStubs, const std::string &value);
};
#endif // __proposer_hh__