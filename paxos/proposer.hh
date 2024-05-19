#ifndef PROPOSER_HH_
#define PROPOSER_HH_

#include "common.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "replicated_log.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

#include <memory>

class Proposer
{
 private:
  uint8_t node_id_;
  int majority_threshold_;
  int retry_count_;  // For testing so we can test nodes not reaching consensus

  std::shared_ptr<ReplicatedLog> replicated_log_;

 public:
  Proposer( int num_acceptors, uint8_t nodeId,
            std::shared_ptr<ReplicatedLog> rlog )
      : retry_count_ { 3 },
        majority_threshold_ { num_acceptors / 2 + 1 },
        node_id_ { nodeId },
        replicated_log_ { rlog }
  {}
  ~Proposer() = default;

  void Propose( const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>&
                    m_acceptorStubs,
                const std::string& value );
  std::string GetValue() const { return ""; }
  std::uint64_t GetIndex() const { return 0; }
};
#endif  // PROPOSER_HH_