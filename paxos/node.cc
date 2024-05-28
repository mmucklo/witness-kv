#include "node.hh"
#include "proposer.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

#include "absl/flags/flag.h"

ABSL_FLAG(absl::Duration, paxos_node_heartbeat, absl::Seconds(3),
          "Heartbeat timeout for paxos node");

ABSL_FLAG(std::string, paxos_node_config_file, "paxos/nodes_config.txt",
          "Paxos config file for nodes ip addresses and ports");

ABSL_FLAG(bool, witness_support, true, "Enable witness support");
ABSL_FLAG(bool, lower_node_witness, false, "Lower nodes are witnesses");

namespace witnesskvs::paxos {

std::vector<Node> ParseNodesConfig() {
  std::vector<Node> nodes{};
  std::ifstream config_file(absl::GetFlag(FLAGS_paxos_node_config_file));

  CHECK(config_file.is_open()) << "Failed to open nodes configuration file";

  std::string line;
  while (std::getline(config_file, line)) {
    std::stringstream ss(line);
    std::string ip_address, port_str;
    int port;
    if (std::getline(ss, ip_address, ':') && std::getline(ss, port_str)) {
      try {
        port = std::stoi(port_str);
      } catch (const std::invalid_argument& e) {
        throw std::runtime_error("Invalid port number in config file");
      }
      nodes.push_back({ip_address, port});
    }
  }

  config_file.close();

  return nodes;
}

PaxosNode::PaxosNode(uint8_t node_id, std::shared_ptr<ReplicatedLog> rlog)
    : num_active_acceptors_conns_{},
      replicated_log_{rlog},
      leader_node_id_{UINT8_MAX} {
  nodes_ = ParseNodesConfig();
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
  if (hb_ss_.stop_possible()) {
    hb_ss_.request_stop();
  }
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
}

void PaxosNode::HeartbeatThread(const std::stop_source& ss) {
  auto sleep_time = absl::GetFlag(FLAGS_paxos_node_heartbeat);

  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Heartbeat Timeout " << sleep_time;

  std::stop_token stoken = ss.get_token();

  while (!stoken.stop_requested()) {
    uint8_t highest_node_id = 0;
    bool cluster_has_valid_leader = false;
    for (size_t i = 0; i < acceptor_stubs_.size(); i++) {
      paxos_rpc::PingRequest request;
      paxos_rpc::PingResponse response;
      grpc::ClientContext context;
      grpc::Status status;
      if (acceptor_stubs_[i]) {
        status = acceptor_stubs_[i]->SendPing(&context, request, &response);
        if (!status.ok()) {
          LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                       << "] Connection lost with node: " << i;
          absl::MutexLock l(&node_mutex_);
          acceptor_stubs_[i].reset();
          CHECK(num_active_acceptors_conns_);
          num_active_acceptors_conns_--;
        }
      } else {
        auto channel = grpc::CreateChannel(nodes_[i].GetAddressPortStr(),
                                           grpc::InsecureChannelCredentials());
        auto stub = paxos_rpc::Acceptor::NewStub(channel);
        status = stub->SendPing(&context, request, &response);
        if (status.ok()) {
          LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                    << "] [Witness: " << nodes_[node_id_].IsWitness()
                    << "] Connection established with node: " << i;
          absl::MutexLock l(&node_mutex_);
          acceptor_stubs_[i] = std::move(stub);
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
      absl::MutexLock l(&node_mutex_);
      if (!(cluster_has_valid_leader && (num_active_acceptors_conns_ > 1))) {
        leader_node_id_ = UINT8_MAX;
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

    absl::SleepFor(sleep_time);
  }

  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Shutting down heartbeat thread";
}

void PaxosNode::ProposeNopAsync(void) {
  auto proposer = std::make_unique<Proposer>( this->GetNumNodes(), node_id_,
                                          replicated_log_, shared_from_this());
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

    grpc::ClientContext context;
    grpc::Status status;

    {
      absl::ReaderMutexLock rl(&node_mutex_);
      if (acceptor_stubs_[node_id]) {
        status = acceptor_stubs_[node_id]->Commit(&context, commit_request,
                                                  &commit_response);
      } else {
        status = grpc::Status(grpc::StatusCode::UNAVAILABLE,
                              "Acceptor not available right now.");
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
  auto hb_thread =
      std::bind(&PaxosNode::HeartbeatThread, this, std::placeholders::_1);
  heartbeat_thread_ = std::jthread(hb_thread, hb_ss_);

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

std::string PaxosNode::GetProposerServiceAddress() {
  absl::MutexLock l(&node_mutex_);
  return nodes_[leader_node_id_].GetAddressPortStr();
}

bool PaxosNode::IsLeader() const { return nodes_[node_id_].IsLeader(); }

bool PaxosNode::IsWitness() const { return nodes_[node_id_].IsWitness(); }

bool PaxosNode::ClusterHasEnoughNodesUp() {
  absl::MutexLock l(&node_mutex_);
  bool result = this->num_active_acceptors_conns_ >= quorum_;
  return result;
}

grpc::Status PaxosNode::PrepareGrpc(uint8_t node_id,
                                    paxos_rpc::PrepareRequest request,
                                    paxos_rpc::PrepareResponse* response) {
  absl::ReaderMutexLock rl(&node_mutex_);
  grpc::ClientContext context;
  if (acceptor_stubs_[node_id]) {
    return acceptor_stubs_[node_id]->Prepare(&context, request, response);
  } else {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Acceptor not available right now.");
  }
}

grpc::Status PaxosNode::AcceptGrpc(uint8_t node_id,
                                   paxos_rpc::AcceptRequest request,
                                   paxos_rpc::AcceptResponse* response) {
  absl::ReaderMutexLock rl(&node_mutex_);
  grpc::ClientContext context;
  if (acceptor_stubs_[node_id]) {
    return acceptor_stubs_[node_id]->Accept(&context, request, response);
  } else {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Acceptor not available right now.");
  }
}

grpc::Status PaxosNode::CommitGrpc(uint8_t node_id,
                                   paxos_rpc::CommitRequest request,
                                   paxos_rpc::CommitResponse* response) {
  absl::ReaderMutexLock rl(&node_mutex_);
  grpc::ClientContext context;
  if (acceptor_stubs_[node_id]) {
    return acceptor_stubs_[node_id]->Commit(&context, request, response);
  } else {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Acceptor not available right now.");
  }
}

grpc::Status PaxosNode::SendPingGrpc(uint8_t node_id,
                                     paxos_rpc::PingRequest request,
                                     paxos_rpc::PingResponse* response) {
  absl::ReaderMutexLock rl(&node_mutex_);
  grpc::ClientContext context;
  if (acceptor_stubs_[node_id]) {
    return acceptor_stubs_[node_id]->SendPing(&context, request, response);
  } else {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Acceptor not available right now.");
  }
}

}  // namespace witnesskvs::paxos