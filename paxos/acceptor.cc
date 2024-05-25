#include "acceptor.hh"

#include <grpcpp/grpcpp.h>

#include "paxos.grpc.pb.h"

using grpc::ServerContext;
using grpc::Status;

using paxos::Acceptor;
using paxos::AcceptRequest;
using paxos::AcceptResponse;
using paxos::CommitRequest;
using paxos::CommitResponse;
using paxos::PingRequest;
using paxos::PingResponse;
using paxos::PrepareRequest;
using paxos::PrepareResponse;

class AcceptorImpl final : public Acceptor::Service {
 private:
  uint8_t node_id_;
  std::shared_ptr<ReplicatedLog> replicated_log_;

 public:
  AcceptorImpl(uint8_t node_id, std::shared_ptr<ReplicatedLog> rlog)
      : node_id_{node_id}, replicated_log_{rlog} {};
  ~AcceptorImpl() = default;

  Status Prepare(ServerContext* context, const PrepareRequest* request,
                 PrepareResponse* response) override;
  Status Accept(ServerContext* context, const AcceptRequest* request,
                AcceptResponse* response) override;
  Status SendPing(ServerContext* context, const PingRequest* request,
                  PingResponse* response) override;
  Status Commit(ServerContext* context, const CommitRequest* request,
                CommitResponse* response) override;
};

Status AcceptorImpl::Prepare(ServerContext* context,
                             const PrepareRequest* request,
                             PrepareResponse* response) {
  uint64_t log_min_proposal =
      this->replicated_log_->GetMinProposalForIdx(request->index());

  bool hasValue = (log_min_proposal != 0);

  if (request->proposal_number() > log_min_proposal) {
    this->replicated_log_->UpdateMinProposalForIdx(request->index(),
                                                   request->proposal_number());
  }

  auto entry = this->replicated_log_->GetLogEntryAtIdx(request->index());

  response->set_accepted_proposal(entry.min_proposal_);

  if (hasValue) {
    response->set_accepted_proposal(entry.accepted_proposal_);
    response->set_accepted_value(entry.accepted_value_);
    response->set_has_accepted_value(true);
  }

  return Status::OK;
}

Status AcceptorImpl::Accept(ServerContext* context,
                            const AcceptRequest* request,
                            AcceptResponse* response) {
  ReplicatedLogEntry entry = {};
  entry.idx_ = request->index();
  entry.min_proposal_ = request->proposal_number();
  entry.accepted_proposal_ = request->proposal_number();
  entry.accepted_value_ = request->value();
  entry.is_chosen_ = false;

  response->set_min_proposal(this->replicated_log_->UpdateLogEntry(entry));
  response->set_first_unchosen_index(
      this->replicated_log_->GetFirstUnchosenIdx());
  return Status::OK;
}

Status AcceptorImpl::SendPing(ServerContext* context,
                              const PingRequest* request,
                              PingResponse* response) {
  response->set_node_id(node_id_);
  return Status::OK;
}

Status AcceptorImpl::Commit(ServerContext* context,
                            const CommitRequest* request,
                            CommitResponse* response) {
  this->replicated_log_->SetLogEntryAtIdx(request->index(), request->value());
  response->set_first_unchosen_index(
      this->replicated_log_->GetFirstUnchosenIdx());
  return Status::OK;
}

void RunServer(const std::string& address, uint8_t node_id,
               std::shared_ptr<ReplicatedLog> rlog,
               const std::stop_source& stop_source) {
  using namespace std::chrono_literals;

  AcceptorImpl service{node_id, rlog};

  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  std::stop_token stoken = stop_source.get_token();
  while (!stoken.stop_requested()) {
    std::this_thread::sleep_for(300ms);
  }
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id)
            << "] Shutting down acceptor service";
}

AcceptorService::AcceptorService(const std::string& address, uint8_t node_id,
                                 std::shared_ptr<ReplicatedLog> rlog)
    : node_id_{node_id} {
  service_thread_ =
      std::jthread(RunServer, address, node_id, rlog, stop_source_);
}

AcceptorService::~AcceptorService() {
  if (stop_source_.stop_possible()) {
    stop_source_.request_stop();
  }
  if (service_thread_.joinable()) {
    service_thread_.join();
  }
}