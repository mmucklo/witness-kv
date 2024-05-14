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
  int majority_threshold_;
  int retry_count_; // For testing so we can test nodes not reaching consensus
  uint64_t first_uncommited_index_;

  static constexpr uint8_t num_bits_for_node_id_ = 3; 
  static constexpr uint64_t mask_ = ~((1ull << num_bits_for_node_id_) - 1);

  std::map<uint64_t, std::string> accepted_proposals_;  // This would come from log read, just war for now

  uint64_t GetNextProposalNumber() {
    proposal_number_ =
      ((proposal_number_ & mask_) + (1ull << num_bits_for_node_id_)) | (uint64_t)node_id_;
    LOG(INFO) << "Generated proposal number: " << proposal_number_;
    return proposal_number_;
  }

 public:
  Proposer( int num_acceptors , uint8_t nodeId)
    : first_uncommited_index_ { 0 }, retry_count_ { 3 },
      majority_threshold_ { num_acceptors / 2 + 1 },
      proposal_number_ { 0 }, node_id_ { nodeId }

  { 
  }
  ~Proposer() = default;

  void Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& m_acceptorStubs,
                const std::string& value );
  std::string GetValue() {
    if (accepted_proposals_.empty()) {
      return ""; // or throw an exception, depending on your requirements
    }
    return accepted_proposals_.rbegin()->second;
  }
  std::uint64_t GetIndex() {
    return first_uncommited_index_;
  }

  
};
#endif // PROPOSER_HH_