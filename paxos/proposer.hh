#ifndef __proposer_hh__
#define __proposer_hh__

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
  uint64_t m_roundNumber;
  uint8_t m_nodeId;
  int m_majorityThreshold;
  int m_retryCount; // For testing so we can test nodes not reaching consensus
  std::map<uint64_t, std::string> m_acceptedProposals;  // This would come from log read, just war for now

  uint64_t getNextProposalNumber() {
    uint64_t propNum = ((++m_roundNumber) << 8) | (uint64_t)m_nodeId;
    std::cout << "From Proposer prop number " << propNum << "\n";
    return propNum;
  }

public:
  Proposer( int num_acceptors , uint8_t nodeId) : m_retryCount { 3 },
                                                  m_majorityThreshold { num_acceptors / 2 + 1 },
                                                  m_roundNumber { 0 },
                                                  m_nodeId { nodeId }
  { 
  }
  ~Proposer() = default;

  void Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& m_acceptorStubs,
                const std::string& value );
  std::string GetValue() {
    if (m_acceptedProposals.empty()) {
      return ""; // or throw an exception, depending on your requirements
    }
    return m_acceptedProposals.rbegin()->second;
  }
  std::uint64_t GetIndex() {
    if (m_acceptedProposals.empty()) {
      return 0; // or throw an exception, depending on your requirements
    }
    return m_acceptedProposals.rbegin()->first;
  }

  
};
#endif // __proposer_hh__