#ifndef PROPOSER_HH_
#define PROPOSER_HH_

#include "common.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "replicated_log.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/server_builder.h>


#include "node.hh"
#include "common.hh"

class ProposerService {
  private:
    std::jthread service_thread_;
    std::stop_source stop_source_ = {};
    uint8_t node_id_;
 
  public:
    ProposerService(const std::string& address, uint8_t node_id,
                    std::shared_ptr<ReplicatedLog> rlog,
                    std::shared_ptr<PaxosNode> paxos_node);
    ~ProposerService();
};
#endif  // PROPOSER_HH_