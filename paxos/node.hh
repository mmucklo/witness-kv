#ifndef NODE_HH_
#define NODE_HH_

#include <memory>

#include "absl/base/optimization.h"
#include "acceptor.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "replicated_log.hh"
#include "utils.hh"

namespace witnesskvs::paxos {

class PaxosNode : public std::enable_shared_from_this<PaxosNode> {
 private:
  std::vector<Node> nodes_;

  std::shared_ptr<ReplicatedLog> replicated_log_;

  absl::Mutex lock_;
  std::vector<std::unique_ptr<paxos_rpc::Acceptor::Stub>> acceptor_stubs_
      ABSL_GUARDED_BY(lock_);

  size_t num_active_acceptors_conns_ ABSL_GUARDED_BY(lock_);

  size_t quorum_;
  uint8_t node_id_;
  uint8_t leader_node_id_ ABSL_GUARDED_BY(lock_);

  bool is_witness_;
  bool is_leader_;

  std::jthread heartbeat_thread_;
  std::jthread truncation_thread_;

  // Sends heartbeat/ping messages to all other nodes in `acceptor_stubs_`.
  // This thread then goes to sleep for `paxos_node_heartbeat` number of
  // seconds. If it detects failure/timeout, it removes that stub from the
  // vector, and next time will attempt to establish a connection hoping the
  // node is back. If it detects a successful re-connection, reinstate the new
  // stub in the vector at the index corresponding to the node.
  void HeartbeatThread(std::stop_token st);
  void TruncationLoop(std::stop_token st);

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

  std::unique_ptr<paxos_rpc::Acceptor::Stub>& GetAcceptorStub(uint8_t node_id)
      ABSL_LOCKS_EXCLUDED(lock_);

  // TODO(mmucklo): maybe use a template like this for the boilerplate in grpc
  // functions
  //
  // template<typename T, typename U, auto F>
  // grpc::Status Send(uint8_t node_id,
  //                   T request,
  //                   U& response) {
  //   std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub =
  //   GetAcceptorStub(node_id); grpc::ClientContext context;
  //   absl::ReaderMutexLock rl(&lock_);
  //   if (ABSL_PREDICT_FALSE(stub == nullptr)) {
  //     return grpc::Status(grpc::StatusCode::UNAVAILABLE,
  //                         "Acceptor not available right now.");
  //   }
  //   return (stub->*F)(&context, request, response);
  // }

 public:
  PaxosNode(uint8_t node_id, std::shared_ptr<ReplicatedLog> rlog);
  ~PaxosNode();
  void MakeReady(void);
  void CommitOnPeerNodes(const std::vector<uint64_t>& commit_idxs);

  size_t GetNumNodes() const { return nodes_.size(); };
  std::string GetNodeAddress(uint8_t node_id) const;

  bool IsLeader() const;
  bool IsLeaderCaughtUp() const {
    return IsLeader() && this->leader_caught_up_;
  }
  uint8_t GetLeaderNodeId() {
    absl::MutexLock l(&lock_);
    return leader_node_id_;
  }

  bool IsWitness() const;
  bool ClusterHasEnoughNodesUp();

  grpc::Status PrepareGrpc(uint8_t node_id, paxos_rpc::PrepareRequest request,
                           paxos_rpc::PrepareResponse* response);
  grpc::Status AcceptGrpc(uint8_t node_id, paxos_rpc::AcceptRequest request,
                          paxos_rpc::AcceptResponse* response);
  grpc::Status CommitGrpc(uint8_t node_id, paxos_rpc::CommitRequest request,
                          paxos_rpc::CommitResponse* response);
};
}  // namespace witnesskvs::paxos
#endif  // NODE_HH_
