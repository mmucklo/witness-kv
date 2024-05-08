#ifndef PROPOSER_HH_
#define PROPOSER_HH_

#include "common.hh"

#include "paxos.grpc.pb.h"
#include "paxos.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

class Proposer
{
 private:
  // Round number should be stored to disk.
  uint64_t round_number_;
  uint8_t node_id_;
  int majority_threshold_;

  uint64_t getNextProposalNumber() {
    uint64_t propNum = ((++round_number_) << 8) | (uint64_t)node_id_;
    std::cout << "From Proposer prop number " << propNum << "\n";
    return propNum;
  }

 public:
  Proposer( int num_acceptors , uint8_t nodeId) : majority_threshold_ { num_acceptors / 2 + 1 },
                                                  round_number_ { 0 },
                                                  node_id_ { nodeId }
  { 
  }
  ~Proposer() = default;

  void Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& m_acceptorStubs,
                const std::string& value );
};

#endif // PROPOSER_HH_