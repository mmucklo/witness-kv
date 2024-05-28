#include "proposer.hh"

#include "absl/flags/flag.h"
#include "absl/log/check.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using paxos::Proposer;
using paxos::ProposeRequest;

ABSL_FLAG(absl::Duration, paxos_server_sleep, absl::Seconds(1),
          "Sleep time for paxos server");

namespace witnesskvs::paxoslibrary {

class ProposerImpl final : public Proposer::Service {
 private:
  std::unique_ptr<Proposer> proposer_;

  uint8_t node_id_;
  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;
  int majority_threshold_;

 public:
  ProposerImpl() = delete;
  ProposerImpl(uint8_t node_id, std::shared_ptr<ReplicatedLog> replicated_log,
               std::shared_ptr<PaxosNode> paxos_node, int majority_threshold);

  ~ProposerImpl() = default;

  void ProposeLocal(const std::string& value);

  Status Propose(ServerContext* context, const ProposeRequest* request,
                 google::protobuf::Empty* response) override;
};

ProposerImpl::ProposerImpl(uint8_t node_id,
                           std::shared_ptr<ReplicatedLog> replicated_log,
                           std::shared_ptr<PaxosNode> paxos_node,
                           int majority_threshold)
    : node_id_{node_id},
      replicated_log_{replicated_log},
      paxos_node_{paxos_node},
      majority_threshold_{majority_threshold} {}

Status ProposerImpl::Propose(ServerContext* context,
                             const ProposeRequest* request,
                             google::protobuf::Empty* response) {
  if (!paxos_node_->IsLeaderCaughtUp()) {
    if (!request->value().empty()) {
      LOG(WARNING)
          << "Leader is not caught up yet, it can only serve NOP request";
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                          "(Leader) Proposer not available right now.");
    }
  }

  ProposeLocal(request->value());
  return Status::OK;
}

void RunProposerServer(const std::string& address, uint8_t node_id,
                       const std::stop_source& stop_source,
                       std::shared_ptr<ReplicatedLog> replicated_log,
                       std::shared_ptr<PaxosNode> paxos_node) {
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id)
            << "] Starting Proposer service. " << address;
  auto sleep_time = absl::GetFlag(FLAGS_paxos_server_sleep);
  using grpc::ServerBuilder;

  ProposerImpl service{node_id, replicated_log, paxos_node,
                       static_cast<int>(paxos_node->GetNumNodes() / 2 + 1)};

  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  std::stop_token stoken = stop_source.get_token();
  while (!stoken.stop_requested()) {
    absl::SleepFor(sleep_time);
  }

  server->Shutdown();
}

ProposerService::ProposerService(const std::string& address, uint8_t node_id,
                                 std::shared_ptr<ReplicatedLog> rlog,
                                 std::shared_ptr<PaxosNode> paxos_node)
    : node_id_(node_id) {
  service_thread_ = std::jthread(RunProposerServer, address, node_id,
                                 stop_source_, rlog, paxos_node);
}

ProposerService::~ProposerService() {
  if (stop_source_.stop_possible()) {
    stop_source_.request_stop();
  }
  if (service_thread_.joinable()) {
    service_thread_.join();
  }
}

void ProposerImpl::ProposeLocal(const std::string& value) {
  bool is_nop = value.empty();
  bool done = false;
  while (!done) {
    std::string value_for_accept_phase = value;
    uint32_t num_promises = 0;

    // Perform phase 1 of the paxos operation i.e. find a proposal that will be
    // accepted by a quorum of acceptors.
    paxos::PrepareRequest request;
    request.set_index(this->replicated_log_->GetFirstUnchosenIdx());
    LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
              << "] Attempting replication at index " << request.index();
    do {
      num_promises = 0;
      request.set_proposal_number(
          this->replicated_log_->GetNextProposalNumber());

      uint64_t max_proposal_id = 0;
      for (size_t i = 0; i < this->paxos_node_->GetNumNodes(); i++) {
        paxos::PrepareResponse response;
        grpc::Status status = this->paxos_node_->PrepareGrpc(
            static_cast<uint8_t>(i), request, &response);
        if (!status.ok()) {
          LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                       << "] Prepare grpc failed for node: " << i
                       << " with error code: " << status.error_code()
                       << " and error message: " << status.error_message();
          continue;
        }

        if (response.min_proposal() > request.proposal_number()) {
          this->replicated_log_->UpdateProposalNumber(response.min_proposal());
          num_promises = 0;
          LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                    << "] Saw a proposal number larger than what we sent, "
                       "retry Propose operation with a bigger proposal.";
          break;
        }
        if (response.has_accepted_value()) {
          if (max_proposal_id < response.accepted_proposal()) {
            max_proposal_id = response.accepted_proposal();
            value_for_accept_phase = response.accepted_value();
            LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                      << "] Current index already has value: "
                      << value_for_accept_phase;
          }
        }
        ++num_promises;
      }
    } while (num_promises < majority_threshold_);

    bool nop_paxos_round = (is_nop and (value == value_for_accept_phase));
    done = nop_paxos_round;

    // Perform phase 2 of paxos operation i.e. try to get the value we
    // determined in phase 1 to be accepted by a quorum of acceptors.
    paxos::AcceptRequest accept_request;
    accept_request.set_proposal_number(request.proposal_number());
    accept_request.set_index(request.index());
    accept_request.set_value(value_for_accept_phase);
    paxos::AcceptResponse accept_response;
    uint32_t accept_majority_count = 0;
    std::vector<uint64_t> peer_unchosen_idx(this->paxos_node_->GetNumNodes());

    for (size_t i = 0; i < this->paxos_node_->GetNumNodes(); i++) {
      grpc::Status status = this->paxos_node_->AcceptGrpc(
          static_cast<uint8_t>(i), accept_request, &accept_response);
      if (!status.ok()) {
        LOG(WARNING) << "NODE: [" << static_cast<uint32_t>(node_id_)
                     << "] Accept grpc failed for node: " << i
                     << " with error code: " << status.error_code()
                     << " and error message: " << status.error_message();
        continue;
      }

      peer_unchosen_idx[i] = accept_response.first_unchosen_index();
      if (nop_paxos_round) {
        continue;
      }

      if (accept_response.min_proposal() > request.proposal_number()) {
        this->replicated_log_->UpdateProposalNumber(
            accept_response.min_proposal());
        accept_majority_count = 0;
        break;
      }
      ++accept_majority_count;
      LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] Got accept from " << i
                << " for proposal number: " << accept_response.min_proposal()
                << " and accepted value: " << value_for_accept_phase;
    }

    // Since a quorum of acceptors responded with an accept for this value,
    // we can mark this entry as chosen.
    if (accept_majority_count >= majority_threshold_) {
      CHECK(!nop_paxos_round)
          << "NODE: [" << static_cast<uint32_t>(node_id_)
          << "] NOP round of paxos should never make log commits.";

      this->replicated_log_->MarkLogEntryChosen(request.index());
      LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] Accepted Proposal number: "
                << accept_response.min_proposal()
                << ", accepted value: " << value_for_accept_phase
                << ", at index: " << request.index() << "\n";

      if (value == value_for_accept_phase) {
        done = true;
        VLOG(1) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] Got the value of interest committed: " << value;
      }
    }

    if (done) {
      this->paxos_node_->CommitOnPeerNodes(peer_unchosen_idx);
    }
  }
}

}  // namespace witnesskvs::paxoslibrary