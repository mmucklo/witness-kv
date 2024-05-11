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
  // Proposal number should be stored to disk.
  uint64_t proposal_number_;
  uint8_t node_id_;
  uint32_t quorum_;

  static constexpr uint8_t num_bits_for_node_id_ = 3; 
  static constexpr uint64_t mask_ = ~((1ull << num_bits_for_node_id_) - 1);

  uint64_t GetNextProposalNumber() {
    proposal_number_ =
      ((proposal_number_ & mask_) + (1ull << num_bits_for_node_id_)) | (uint64_t)node_id_;
    LOG(INFO) << "Generated proposal number: " << proposal_number_;
    return proposal_number_;
  }

 public:
  Proposer( uint8_t num_acceptors , uint8_t node_id) : quorum_ { num_acceptors / 2u + 1u },
                                                      proposal_number_ { 0 },
                                                      node_id_ { node_id }
  { 
  }
  ~Proposer() = default;

  void Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& m_acceptorStubs,
                const std::string& value );
};

#endif // PROPOSER_HH_