#include "acceptor.hh"

#include <grpcpp/grpcpp.h>

#include "paxos.grpc.pb.h"

using grpc::ServerContext;
using grpc::Status;

using paxos::Acceptor;
using paxos::AcceptRequest;
using paxos::AcceptResponse;
using paxos::PingRequest;
using paxos::PingResponse;
using paxos::PrepareRequest;
using paxos::PrepareResponse;

class AcceptorImpl final : public Acceptor::Service
{
 private:
  struct Proposal_Data
  {
    uint64_t min_proposal_;
    uint64_t accepted_proposal_;
    std::string accepted_value_;
  };
  std::map<uint64_t, Proposal_Data> proposal_data_;
  std::mutex mutex_;

  uint8_t node_id_;

 public:
  AcceptorImpl( uint8_t node_id ) : mutex_ {}, node_id_ { node_id } {}
  ~AcceptorImpl() = default;

  Status Prepare( ServerContext* context, const PrepareRequest* request,
                  PrepareResponse* response ) override;
  Status Accept( ServerContext* context, const AcceptRequest* request,
                 AcceptResponse* response ) override;
  Status SendPing( ServerContext* context, const PingRequest* request,
                   PingResponse* response ) override;
};

Status AcceptorImpl::Prepare( ServerContext* context,
                              const PrepareRequest* request,
                              PrepareResponse* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );

  bool hasValue = false;
  if ( proposal_data_.find( request->index() ) != proposal_data_.end() ) {
    hasValue = true;
  }

  uint64_t n = request->proposal_number();
  if ( n > proposal_data_[request->index()].min_proposal_ ) {
    proposal_data_[request->index()].min_proposal_ = n;
  }

  response->set_accepted_proposal(
      proposal_data_[request->index()].min_proposal_ );

  if ( hasValue ) {
    response->set_accepted_proposal(
        proposal_data_[request->index()].accepted_proposal_ );
    response->set_accepted_value(
        proposal_data_[request->index()].accepted_value_ );
    response->set_has_accepted_value( true );
  }

  return Status::OK;
}

Status AcceptorImpl::Accept( ServerContext* context,
                             const AcceptRequest* request,
                             AcceptResponse* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );

  uint64_t n = request->proposal_number();
  if ( n >= proposal_data_[request->index()].min_proposal_ ) {
    proposal_data_[request->index()].accepted_proposal_ = n;
    proposal_data_[request->index()].min_proposal_ = n;
    proposal_data_[request->index()].accepted_value_
        = std::move( request->value() );
  }
  response->set_min_proposal( proposal_data_[request->index()].min_proposal_ );
  return Status::OK;
}

Status AcceptorImpl::SendPing( ServerContext* context,
                               const PingRequest* request,
                               PingResponse* response )
{
  response->set_node_id( node_id_ );
  return Status::OK;
}

void RunServer( const std::string& address, uint8_t node_id,
                const std::stop_source& stop_source )
{
  using namespace std::chrono_literals;

  AcceptorImpl service { node_id };

  grpc::ServerBuilder builder;
  builder.AddListeningPort( address, grpc::InsecureServerCredentials() );
  builder.RegisterService( &service );

  std::unique_ptr<grpc::Server> server( builder.BuildAndStart() );

  std::stop_token stoken = stop_source.get_token();
  while ( !stoken.stop_requested() ) { std::this_thread::sleep_for( 300ms ); }
  LOG( INFO ) << "Shutting down acceptor service.";
}

AcceptorService::AcceptorService( const std::string& address, uint8_t node_id )
    : node_id_ { node_id }
{
  service_thread_ = std::jthread( RunServer, address, node_id, stop_source_ );
}

AcceptorService::~AcceptorService()
{
  if ( stop_source_.stop_possible() ) { stop_source_.request_stop(); }
  if ( service_thread_.joinable() ) { service_thread_.join(); }
}