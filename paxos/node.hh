#ifndef NODE_HH_
#define NODE_HH_

#include "acceptor.hh"
#include "common.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "replicated_log.hh"

namespace witnesskvs::paxos {

struct Node {
  std::string ip_address_;
  int port_;
  bool is_witness_;
  bool is_leader_;
  bool IsWitness() const { return is_witness_; }
  bool IsLeader() const { return is_leader_; }
  std::string GetAddressPortStr() const {
    return this->ip_address_ + ":" + std::to_string(this->port_);
  }

  std::string GetProposerServiceAddressPortStr(uint64_t nodes_size) const {
    int leader_port = this->port_ + nodes_size;
    return this->ip_address_ + ":" + std::to_string(leader_port);
  }
};

class PaxosNode {
 private:
  std::vector<Node> nodes_;

  std::shared_ptr<ReplicatedLog> replicated_log_;

  absl::Mutex node_mutex_;

  std::vector<std::unique_ptr<paxos_rpc::Acceptor::Stub>> acceptor_stubs_
      ABSL_GUARDED_BY(node_mutex_);
  size_t num_active_acceptors_conns_ ABSL_GUARDED_BY(node_mutex_);

  size_t quorum_;
  uint8_t node_id_;
  uint8_t leader_node_id_ ABSL_GUARDED_BY(node_mutex_);

  std::jthread heartbeat_thread_;
  std::stop_source hb_ss_ = {};

  // Sends heartbeat/ping messages to all other nodes in `acceptor_stubs_`.
  // This thread then goes to sleep for `paxos_node_heartbeat` number of
  // seconds. If it detects failure/timeout, it removes that stub from the
  // vector, and next time will attempt to establish a connection hoping the
  // node is back. If it detects a successful re-connection, reinstate the new
  // stub in the vector at the index corresponding to the node.
  void HeartbeatThread(const std::stop_source& ss);

  void CommitAsync(uint8_t node_id, uint64_t idx);
  std::vector<std::future<void>> commit_futures_;

  // Upon a new leader election, the newly elected leader will perform a NOP
  // paxos propose operation which will help fill up any holes/gaps in its log.
  // Till the leader is caught up, it is not ready to serve client requests.
  // The catchup operation happens in the background so that the leader can
  // continue to send heartbeats till it is caught up.
  void ProposeNopAsync(void);
  void PerformLeaderCatchUp(void);
  std::future<void> async_leader_catch_up_;
  std::atomic<bool> leader_caught_up_;

  grpc::Status SendPingGrpc(uint8_t node_id, paxos_rpc::PingRequest request,
                            paxos_rpc::PingResponse* response);

 public:
  PaxosNode(uint8_t node_id, std::shared_ptr<ReplicatedLog> rlog);
  ~PaxosNode();

  void MakeReady(void);
  void CommitOnPeerNodes(const std::vector<uint64_t>& commit_idxs);

  size_t GetNumNodes() const { return nodes_.size(); };
  std::string GetNodeAddress(uint8_t node_id) const;
  std::string GetProposerServiceAddress(uint8_t nodes_id) const;
  bool IsLeader() const;
  bool IsLeaderCaughtUp() const {
    return IsLeader() && this->leader_caught_up_;
  }
  bool IsWitness() const;
  bool ClusterHasEnoughNodesUp();

  grpc::Status PrepareGrpc(uint8_t node_id, paxos_rpc::PrepareRequest request,
                           paxos_rpc::PrepareResponse* response);
  grpc::Status AcceptGrpc(uint8_t node_id, paxos_rpc::AcceptRequest request,
                          paxos_rpc::AcceptResponse* response);
  grpc::Status CommitGrpc(uint8_t node_id, paxos_rpc::CommitRequest request,
                          paxos_rpc::CommitResponse* response);
  grpc::Status SendProposeGrpc(paxos_rpc::ProposeRequest request,
                               google::protobuf::Empty* response);
};

// This helper function will parse the node config file specified
// by flag `paxos_node_config_file`.
std::vector<Node> ParseNodesConfig();
}  // namespace witnesskvs::paxos
#endif  // NODE_HH_