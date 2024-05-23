#ifndef LEADER_HH_
#define LEADER_HH_

#include "common.hh"
#include "replicated_log.hh"
#include "node.hh"

class LeaderService
{
 private:
  std::jthread service_thread_;
  std::stop_source stop_source_ = {};
  uint8_t node_id_;

 public:
  LeaderService( const std::string& address, uint8_t node_id,  std::shared_ptr<ReplicatedLog> replicated_log,
                       std::shared_ptr<PaxosNode> paxos_node);
  ~LeaderService();
};

#endif // LEADER_HH_