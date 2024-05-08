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

class AcceptorImpl final : public Acceptor::Service
{
 private:
  uint64_t min_proposal_;
  std::optional<uint64_t> accepted_proposal_;
  std::optional<std::string> accepted_value_;
  // TODO: FIXME - For now we can use a terminal mutex.
  std::mutex mutex_;

 public:
  AcceptorImpl() : min_proposal_ { 0 }, mutex_ {} {}
  ~AcceptorImpl() = default;

  Status Prepare( ServerContext* context, const PrepareRequest* request, PrepareResponse* response ) override;
  Status Accept( ServerContext* context, const AcceptRequest* request, AcceptResponse* response ) override;
  Status SendPing( ServerContext* context, const google::protobuf::Empty *request, google::protobuf::Empty *response ) override;
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

  if ( hasValue ) {
    response->set_accepted_proposal( accepted_proposal_.value() );
    response->set_accepted_value( accepted_value_.value() );
  } else {
    // FIXME: Is this else block needed ?
    response->set_accepted_proposal( 0 );
    response->set_accepted_value( "" );
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
    accepted_value_ = std::move( request->value() );
  }
  response->set_min_proposal( min_proposal_ );
  return Status::OK;
}

Status 
AcceptorImpl::SendPing( ServerContext* context, const google::protobuf::Empty *request, google::protobuf::Empty *response ) {
  return Status::OK;
}

void RunServer( const std::string& address, const std::stop_source& stop_source )
{
  using namespace std::chrono_literals;

  AcceptorImpl service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort( address, grpc::InsecureServerCredentials() );
  builder.RegisterService( &service );

  std::unique_ptr<grpc::Server> server( builder.BuildAndStart() );

  std::stop_token stoken = stop_source.get_token();
  while ( !stoken.stop_requested() ) {
    std::this_thread::sleep_for( 300ms );
  }
}

AcceptorService::AcceptorService( const std::string& address )
{
  service_thread_ = std::jthread( RunServer, address, stop_source_ );
}

AcceptorService::~AcceptorService()
{
  if ( stop_source_.stop_possible() ) {
    stop_source_.request_stop();
  }
  service_thread_.join();
}