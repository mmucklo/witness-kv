#ifndef NODE_HH_
#define NODE_HH_

#include "acceptor.hh"
#include "common.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"

struct Node
{
  std::string ip_address_;
  int port_;
  std::string GetAddressPortStr() const
  {
    return this->ip_address_ + ":" + std::to_string( this->port_ );
  }
};

class PaxosNode
{
 private:
  std::vector<Node> nodes_;

  absl::Mutex node_mutex_;
  std::vector<std::unique_ptr<paxos::Acceptor::Stub>> acceptor_stubs_
      ABSL_GUARDED_BY( node_mutex_ );
  size_t num_active_acceptors_conns_ ABSL_GUARDED_BY( node_mutex_ );

  size_t quorum_;
  uint8_t node_id_;
  uint8_t leader_node_id_ ABSL_GUARDED_BY( node_mutex_ );

  std::jthread heartbeat_thread_;
  std::stop_source stop_source_ = {};
  std::chrono::seconds hb_timer_ { 3 };

  // Sends heartbeat/ping messages to all other nodes in `acceptor_stubs_`.
  // This thread then goes to sleep for `hb_timer_` number of seconds.
  // If it detects failure/timeout, it removes that stub from the vector,
  // and next time will attempt to establish a connection hoping the node is
  // back. If it detects a successful re-connection, reinstate the new stub in
  // the vector at the index corresponding to the node.
  void HeartbeatThread( const std::stop_source& stop_source );

  grpc::Status SendPingGrpc( uint8_t node_id, paxos::PingRequest request,
                             paxos::PingResponse* response );

 public:
  PaxosNode( const std::string& config_file_name, uint8_t node_id );
  ~PaxosNode();
  void CreateHeartbeatThread( void );
  size_t GetNumNodes() const;
  std::string GetNodeAddress( uint8_t node_id ) const;
  bool ClusterHasEnoughNodesUp();

  // TODO remove
  const std::vector<std::unique_ptr<paxos::Acceptor::Stub>>& GetStubs()
  {
    return acceptor_stubs_;
  }

  grpc::Status PrepareGrpc( uint8_t node_id, paxos::PrepareRequest request,
                            paxos::PrepareResponse* response );
  grpc::Status AcceptGrpc( uint8_t node_id, paxos::AcceptRequest request,
                           paxos::AcceptResponse* response );
  grpc::Status CommitGrpc( uint8_t node_id, paxos::CommitRequest request,
                           paxos::CommitResponse* response );
};

std::vector<Node> ParseNodesConfig( const std::string& config_file_name );
#endif  // NODE_HH_