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
  std::map<uint64_t, uint64_t> min_proposal_;
  std::map<uint64_t, std::optional<uint64_t>> accepted_proposal_;
  std::map<uint64_t, std::optional<std::string>> accepted_value_;
  // TODO: FIXME - For now we can use a terminal mutex.
  std::mutex mutex_;

public:
  AcceptorImpl() : mutex_ {} {}
  ~AcceptorImpl() = default;

  Status Prepare( ServerContext* context, const PrepareRequest* request, PrepareResponse* response ) override;
  Status Accept( ServerContext* context, const AcceptRequest* request, AcceptResponse* response ) override;
  Status SendPing( ServerContext* context, const google::protobuf::Empty *request, google::protobuf::Empty *response ) override;
};

Status AcceptorImpl::Prepare( ServerContext* context, const PrepareRequest* request, PrepareResponse* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );

  uint64_t n = request->proposal_number();
  if ( n > min_proposal_[request->index_number()] ) {
    min_proposal_[request->index_number()] = n;
  }

  bool hasValue = false;
  if ( accepted_value_.find( request->index_number() ) != accepted_value_.end()) {
      hasValue = accepted_value_[request->index_number()].has_value(); 
  }

  response->set_has_accepted_value( hasValue );

  if ( hasValue ) {
    response->set_accepted_proposal( accepted_proposal_[request->index_number()].value() );
    response->set_accepted_value( accepted_value_[request->index_number()].value() );
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
  if ( n >= min_proposal_[request->index_number()]) {
    accepted_proposal_[request->index_number()] = n;
    min_proposal_[request->index_number()] = n;
    accepted_value_[request->index_number()] = std::move( request->value() );
  }
  response->set_min_proposal( min_proposal_[request->index_number()] );
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
  service_thread = std::jthread( RunServer, address, stop_source );
}

AcceptorService::~AcceptorService()
{
  if ( stop_source.stop_possible() ) {
    stop_source.request_stop();
  }
  service_thread.join();
}