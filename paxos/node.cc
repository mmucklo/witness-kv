#include "node.hh"

#include <functional>

#include <functional>

#include "proposer.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/synchronization/mutex.h"

#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/synchronization/mutex.h"

ABSL_FLAG(absl::Duration, paxos_node_heartbeat, absl::Seconds(3),
          "Heartbeat timeout for paxos node");

// Note: for demo purposes this should be set to be small.
ABSL_FLAG(absl::Duration, paxos_node_truncation_interval, absl::Seconds(60),
          "How often we should query for truncation.");

// Note: for demo purposes this should be set to be small.
ABSL_FLAG(absl::Duration, paxos_node_truncation_interval, absl::Seconds(60),
          "How often we should query for truncation.");

ABSL_FLAG(std::string, paxos_node_config_file, "paxos/nodes_config.txt",
          "Paxos config file for nodes ip addresses and ports");

ABSL_FLAG(bool, witness_support, true, "Enable witness support");
ABSL_FLAG(bool, lower_node_witness, false, "Lower nodes are witnesses");

namespace witnesskvs::paxos {

PaxosNode::PaxosNode(uint8_t node_id, std::shared_ptr<ReplicatedLog> rlog)
    : num_active_acceptors_conns_{},
      replicated_log_{rlog},
      leader_node_id_{INVALID_NODE_ID} {
  nodes_ = ParseNodesConfig(absl::GetFlag(FLAGS_paxos_node_config_file));
  CHECK_NE(nodes_.size(), 0);

  std::set<std::pair<std::string, int>> s;
  for (const auto& node : nodes_) {
    CHECK(s.insert({node.ip_address_, node.port_}).second)
        << "Invalid config file : Duplicate IP address and port found";
  }

  node_id_ = node_id;
  quorum_ = nodes_.size() / 2 + 1;

  // Determine the number of witnesses based on the total number of nodes
  size_t num_witnesses = floor(static_cast<double>(nodes_.size()) / 2);

  for (size_t i = 0; i < nodes_.size(); ++i) {
    nodes_[i].is_witness_ = false;
    nodes_[i].is_leader_ = false;
    if (absl::GetFlag(FLAGS_witness_support)) {
      if (i >= nodes_.size() - num_witnesses &&
          !absl::GetFlag(FLAGS_lower_node_witness)) {
        nodes_[i].is_witness_ = true;
      }
      if (i < num_witnesses && absl::GetFlag(FLAGS_lower_node_witness)) {
        nodes_[i].is_witness_ = true;
      }
    }
  }

  acceptor_stubs_.resize(nodes_.size());
}

PaxosNode::~PaxosNode() {
  absl::MutexLock l(&lock_);
  heartbeat_thread_.get_stop_source().request_stop();
  truncation_thread_.get_stop_source().request_stop();
}

void PaxosNode::TruncationLoop(std::stop_token st) {
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] TruncationLoop Interval "
            << absl::GetFlag(FLAGS_paxos_node_truncation_interval);

  while (!st.stop_requested()) {
    // TODO(mmucklo): fill with Truncation logic.
    size_t acceptor_stubs_size;
    {
      absl::ReaderMutexLock l(&lock_);
      acceptor_stubs_size = acceptor_stubs_.size();
    }
    for (size_t i = 0; i < acceptor_stubs_size; i++) {
      std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(i);
      paxos_rpc::TruncateProposeRequest request;
      paxos_rpc::TruncateProposeResponse response;
      grpc::ClientContext context;
      absl::ReaderMutexLock l(&lock_);
      if (stub == nullptr) {
        // We can't truncate without agreement. Since a node is down, we skip
        // this round.
        LOG(INFO) << "Skipping truncation because node: " << i
                  << " is offline.";
        break;
      }

      grpc::Status status = stub->TruncatePropose(&context, request, &response);
      LOG(INFO) << "Got Truncate Response for node: " << i << ": "
                << response.index();
    }

    absl::MutexLock l(&lock_);
    auto stopping = [&st]() { return st.stop_requested(); };
    lock_.AwaitWithTimeout(absl::Condition(&stopping),
                           absl::GetFlag(FLAGS_paxos_node_truncation_interval));
  }
}

void PaxosNode::HeartbeatThread(std::stop_token st) {
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Heartbeat Timeout " <<  absl::GetFlag(FLAGS_paxos_node_heartbeat);

  while (!st.stop_requested()) {
    uint8_t highest_node_id = 0;
    bool cluster_has_valid_leader = false;
    size_t acceptor_stubs_size;
    {
      absl::ReaderMutexLock l(&lock_);
      acceptor_stubs_size = acceptor_stubs_.size();
    }
    for (size_t i = 0; i < acceptor_stubs_size; i++) {
    size_t acceptor_stubs_size;
    {
      absl::ReaderMutexLock l(&lock_);
      acceptor_stubs_size = acceptor_stubs_.size();
    }
    for (size_t i = 0; i < acceptor_stubs_size; i++) {
      paxos_rpc::PingRequest request;
      paxos_rpc::PingResponse response;
      grpc::ClientContext context;
      grpc::Status status;
      std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(i);
      if (stub != nullptr) {
        // Don't need a lock around this call like the others do since we're the
        // only one that will change the stub.
        status = stub->Ping(&context, request, &response);
        if (!status.ok()) {
          LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                       << "] Connection lost with node: " << i;
          absl::MutexLock l(&lock_);
          absl::MutexLock l(&lock_);
          acceptor_stubs_[i].reset();
          CHECK(num_active_acceptors_conns_);
          num_active_acceptors_conns_--;
        }
      } else {
        auto channel = grpc::CreateChannel(nodes_[i].GetAddressPortStr(),
                                           grpc::InsecureChannelCredentials());
        auto new_stub = paxos_rpc::Acceptor::NewStub(channel);
        status = new_stub->Ping(&context, request, &response);
        if (status.ok()) {
          LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                    << "] [Witness: " << nodes_[node_id_].IsWitness()
                    << "] Connection established with node: " << i;
          absl::MutexLock l(&lock_);
          stub = std::move(new_stub);
          absl::MutexLock l(&lock_);
          stub = std::move(new_stub);
          num_active_acceptors_conns_++;
        }
      }

      if (status.ok()) {
        if (!nodes_[response.node_id()].is_witness_) {
          // If there is atleast one non-witness node, we have a valid leader
          // node.
          cluster_has_valid_leader = true;
          highest_node_id = std::max(highest_node_id,
                                     static_cast<uint8_t>(response.node_id()));
        }
      }
    }

    bool node_is_new_leader = false;
    {
      // TODO(vishnu/ritesh): the purpose behind this logic should be explained
      // probably in a not-so-short comment here.
      absl::MutexLock l(&lock_);
      // TODO(vishnu/ritesh): the purpose behind this logic should be explained
      // probably in a not-so-short comment here.
      absl::MutexLock l(&lock_);
      if (!(cluster_has_valid_leader && (num_active_acceptors_conns_ > 1))) {
        leader_node_id_ = INVALID_NODE_ID;
      } else if (leader_node_id_ != highest_node_id) {
        LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                  << "] New leader elected with node id: "
                  << static_cast<uint32_t>(highest_node_id);
        leader_node_id_ = highest_node_id;
        std::for_each(nodes_.begin(), nodes_.end(),
                      [](auto& e) { e.is_leader_ = false; });
        nodes_[leader_node_id_].is_leader_ = true;
        leader_caught_up_ = false;
        node_is_new_leader = (node_id_ == leader_node_id_);
      }
    }

    if (node_is_new_leader) {
      PerformLeaderCatchUp();
    }
    absl::MutexLock l(&lock_);
    auto stopping = [&st]() { return st.stop_requested(); };
    lock_.AwaitWithTimeout(absl::Condition(&stopping),  absl::GetFlag(FLAGS_paxos_node_heartbeat));
  }

  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Shutting down heartbeat thread";
}

void PaxosNode::ProposeNopAsync(void) {
  auto proposer = std::make_unique<Proposer>(
      this->GetNumNodes(), node_id_, replicated_log_, shared_from_this());
  proposer->Propose("");
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Performed a Paxos No-op round";
  leader_caught_up_ = true;
}

void PaxosNode::PerformLeaderCatchUp(void) {
  auto func = std::bind(&PaxosNode::ProposeNopAsync, this);
  async_leader_catch_up_ = std::async(std::launch::async, func);
}

void PaxosNode::CommitAsync(uint8_t node_id, uint64_t idx) {
  uint64_t commit_idx = idx;
  while (commit_idx < this->replicated_log_->GetFirstUnchosenIdx()) {
    paxos_rpc::CommitRequest commit_request;
    commit_request.set_index(commit_idx);
    commit_request.set_value(
        this->replicated_log_->GetLogEntryAtIdx(commit_idx).accepted_value_);
    paxos_rpc::CommitResponse commit_response;

    grpc::Status status;
    {
      // Limit the scope of this shared_ptr within these braces.
      std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub =
          GetAcceptorStub(node_id);
      grpc::ClientContext context;
      absl::ReaderMutexLock rl(&lock_);
      if (stub == nullptr) {
      // Limit the scope of this shared_ptr within these braces.
      std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub =
          GetAcceptorStub(node_id);
      grpc::ClientContext context;
      absl::ReaderMutexLock rl(&lock_);
      if (stub == nullptr) {
        status = grpc::Status(grpc::StatusCode::UNAVAILABLE,
                              "Acceptor not available right now.");
      } else {
        status = stub->Commit(&context, commit_request, &commit_response);
      }
    }

    if (!status.ok()) {
      break;
    }

    CHECK_LT(commit_idx, commit_response.first_unchosen_index())
        << "NODE: [" << static_cast<uint32_t>(node_id_)
        << "] This index was just committed, the response must return an "
           "index beyond it";
    commit_idx = commit_response.first_unchosen_index();
  }
}

void PaxosNode::MakeReady() {
  heartbeat_thread_ =
      std::jthread(std::bind_front(&PaxosNode::HeartbeatThread, this));
  truncation_thread_ =
      std::jthread(std::bind_front(&PaxosNode::TruncationLoop, this));
  commit_futures_.resize(GetNumNodes());
}

void PaxosNode::CommitOnPeerNodes(const std::vector<uint64_t>& commit_idxs) {
  auto commit_async = std::bind(&PaxosNode::CommitAsync, this,
                                std::placeholders::_1, std::placeholders::_2);
  for (size_t i = 0; i < GetNumNodes(); i++) {
    if (static_cast<uint8_t>(i) == node_id_) continue;

    if (commit_idxs[i] < this->replicated_log_->GetFirstUnchosenIdx()) {
      commit_futures_[i] = std::async(std::launch::async, commit_async,
                                      static_cast<uint8_t>(i), commit_idxs[i]);
    }
  }
}

std::string PaxosNode::GetNodeAddress(uint8_t node_id) const {
  return nodes_[node_id].GetAddressPortStr();
}

bool PaxosNode::IsLeader() const { return nodes_[node_id_].IsLeader(); }

bool PaxosNode::IsWitness() const { return nodes_[node_id_].IsWitness(); }

bool PaxosNode::ClusterHasEnoughNodesUp() {
  absl::MutexLock l(&lock_);
  absl::MutexLock l(&lock_);
  bool result = this->num_active_acceptors_conns_ >= quorum_;
  return result;
}

// Make boilerplate code a bit more readable with this macro.
#define RETURN_IF_NULLPTR(expr)                                 \
  do {                                                          \
    if (ABSL_PREDICT_FALSE(expr == nullptr)) {                  \
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,        \
                          "Acceptor not available right now."); \
    }                                                           \
  } while (0)

// Make boilerplate code a bit more readable with this macro.
#define RETURN_IF_NULLPTR(expr)                                 \
  do {                                                          \
    if (ABSL_PREDICT_FALSE(expr == nullptr)) {                  \
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,        \
                          "Acceptor not available right now."); \
    }                                                           \
  } while (0)

grpc::Status PaxosNode::PrepareGrpc(uint8_t node_id,
                                    paxos_rpc::PrepareRequest request,
                                    paxos_rpc::PrepareResponse* response) {
  std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(node_id);
  std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(node_id);
  grpc::ClientContext context;
  absl::ReaderMutexLock rl(&lock_);
  RETURN_IF_NULLPTR(stub);
  return stub->Prepare(&context, request, response);
}

std::unique_ptr<paxos_rpc::Acceptor::Stub>& PaxosNode::GetAcceptorStub(
    uint8_t node_id) {
  absl::ReaderMutexLock l(&lock_);
  if (acceptor_stubs_.size() <= node_id) {
    LOG(FATAL) << "PaxosNode::GetAcceptorStub - acceptor_stubs_ size("
               << acceptor_stubs_.size() << ") is <= node_id: " << node_id;
  absl::ReaderMutexLock rl(&lock_);
  RETURN_IF_NULLPTR(stub);
  return stub->Prepare(&context, request, response);
}

std::unique_ptr<paxos_rpc::Acceptor::Stub>& PaxosNode::GetAcceptorStub(
    uint8_t node_id) {
  absl::ReaderMutexLock l(&lock_);
  if (acceptor_stubs_.size() <= node_id) {
    LOG(FATAL) << "PaxosNode::GetAcceptorStub - acceptor_stubs_ size("
               << acceptor_stubs_.size() << ") is <= node_id: " << node_id;
  }
  return acceptor_stubs_[node_id];
  return acceptor_stubs_[node_id];
}

grpc::Status PaxosNode::AcceptGrpc(uint8_t node_id,
                                   paxos_rpc::AcceptRequest request,
                                   paxos_rpc::AcceptResponse* response) {
  std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(node_id);
  std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(node_id);
  grpc::ClientContext context;
  absl::ReaderMutexLock rl(&lock_);
  RETURN_IF_NULLPTR(stub);
  return stub->Accept(&context, request, response);
  absl::ReaderMutexLock rl(&lock_);
  RETURN_IF_NULLPTR(stub);
  return stub->Accept(&context, request, response);
}

grpc::Status PaxosNode::CommitGrpc(uint8_t node_id,
                                   paxos_rpc::CommitRequest request,
                                   paxos_rpc::CommitResponse* response) {
  std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(node_id);
  std::unique_ptr<paxos_rpc::Acceptor::Stub>& stub = GetAcceptorStub(node_id);
  grpc::ClientContext context;
  absl::ReaderMutexLock rl(&lock_);
  RETURN_IF_NULLPTR(stub);
  return stub->Commit(&context, request, response);
}

#undef RETURN_IF_NULLPTR

}  // namespace witnesskvs::paxos

