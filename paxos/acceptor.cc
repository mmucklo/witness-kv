#include "acceptor.hh"

#include "paxos.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::ServerContext;
using grpc::Status;

using paxos::Acceptor;
using paxos::AcceptRequest;
using paxos::AcceptResponse;
using paxos::PrepareRequest;
using paxos::PrepareResponse;
using paxos::PingRequest;
using paxos::PingResponse;

class AcceptorImpl final : public Acceptor::Service
{
 private:
  uint64_t min_proposal_;
  std::optional<uint64_t> accepted_proposal_;
  std::optional<std::string> accepted_value_;
  std::mutex mutex_;

  uint8_t node_id_;

 public:
  AcceptorImpl(uint8_t node_id) 
    : min_proposal_ { 0 }, mutex_ {}, node_id_{node_id} {}
  ~AcceptorImpl() = default;

  Status Prepare( ServerContext* context, const PrepareRequest* request, PrepareResponse* response ) override;
  Status Accept( ServerContext* context, const AcceptRequest* request, AcceptResponse* response ) override;
  Status SendPing( ServerContext* context, const PingRequest* request, PingResponse* response ) override;
};

Status AcceptorImpl::Prepare( ServerContext* context, const PrepareRequest* request, PrepareResponse* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );

  uint64_t n = request->proposal_number();
  if ( n > min_proposal_ ) {
    min_proposal_ = n;
  }

  bool hasValue = accepted_value_.has_value();
  response->set_has_accepted_value( hasValue );
  response->set_min_proposal( min_proposal_ );

  if ( hasValue ) {
    response->set_accepted_proposal( accepted_proposal_.value() );
    response->set_accepted_value( accepted_value_.value() );
  }

  return Status::OK;
}

Status AcceptorImpl::Accept( ServerContext* context, const AcceptRequest* request, AcceptResponse* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );

  uint64_t n = request->proposal_number();
  if ( n >= min_proposal_ ) {
    accepted_proposal_ = n;
    min_proposal_ = n;
    accepted_value_ = request->value();
  }
  response->set_min_proposal( min_proposal_ );
  return Status::OK;
}

Status 
AcceptorImpl::SendPing( ServerContext* context, const PingRequest* request, PingResponse* response ) {
  response->set_node_id(node_id_);
  return Status::OK;
}

void RunServer( const std::string& address, uint8_t node_id, const std::stop_source& stop_source )
{
  using namespace std::chrono_literals;

  AcceptorImpl service{node_id};
  grpc::ServerBuilder builder;
  builder.AddListeningPort( address, grpc::InsecureServerCredentials() );
  builder.RegisterService( &service );

  std::unique_ptr<grpc::Server> server( builder.BuildAndStart() );

  std::stop_token stoken = stop_source.get_token();
  while ( !stoken.stop_requested() ) {
    std::this_thread::sleep_for( 300ms );
  }
  LOG(INFO) << "Shutting down acceptor service.";
}

AcceptorService::AcceptorService( const std::string& address, uint8_t node_id )
  : node_id_{node_id}
{
  service_thread_ = std::jthread( RunServer, address, node_id, stop_source_ );
}

AcceptorService::~AcceptorService()
{
  if ( stop_source_.stop_possible() ) {
    stop_source_.request_stop();
  }
  if (service_thread_.joinable()) {
    service_thread_.join();
  }
}