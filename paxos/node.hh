#ifndef NODE_HH_
#define NODE_HH_

#include "acceptor.hh"
#include "common.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "replicated_log.hh"

namespace witnesskvs::paxoslibrary {

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

  std::string GetLeaderAddressPortStr(uint64_t nodes_size) const {
    int leader_port = this->port_ + nodes_size;
    return this->ip_address_ + ":" + std::to_string(leader_port);
  }
};

class PaxosNode {
 private:
  std::vector<Node> nodes_;

  std::shared_ptr<ReplicatedLog> replicated_log_;

  absl::Mutex node_mutex_;
  std::mutex proposer_stub_mutex_;

  std::vector<std::unique_ptr<paxos::Acceptor::Stub>> acceptor_stubs_
      ABSL_GUARDED_BY(node_mutex_);
  std::unique_ptr<paxos::Proposer::Stub> proposer_stub_;
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

  grpc::Status SendPingGrpc(uint8_t node_id, paxos::PingRequest request,
                            paxos::PingResponse* response);

 public:
  PaxosNode(uint8_t node_id, std::shared_ptr<ReplicatedLog> rlog);
  ~PaxosNode();

  void MakeReady(void);
  void CommitInBackground(const std::vector<uint64_t>& commit_idxs);

  size_t GetNumNodes() const { return nodes_.size(); };
  std::string GetNodeAddress(uint8_t node_id) const;
  std::string GetLeaderAddress(uint8_t nodes_id) const;
  bool IsLeader(uint8_t node_id) const;
  bool IsWitness(uint8_t node_id) const;
  bool ClusterHasEnoughNodesUp();

  grpc::Status PrepareGrpc(uint8_t node_id, paxos::PrepareRequest request,
                           paxos::PrepareResponse* response);
  grpc::Status AcceptGrpc(uint8_t node_id, paxos::AcceptRequest request,
                          paxos::AcceptResponse* response);
  grpc::Status CommitGrpc(uint8_t node_id, paxos::CommitRequest request,
                          paxos::CommitResponse* response);
  grpc::Status SendProposeGrpc(paxos::ProposeRequest request,
                               google::protobuf::Empty* response);
};

// This helper function will parse the node config file specified
// by flag `paxos_node_config_file`.
std::vector<Node> ParseNodesConfig();

}  // namespace witnesskvs::paxoslibrary
#endif  // NODE_HH_