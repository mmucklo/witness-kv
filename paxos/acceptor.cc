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

struct Proposal_Data {
  uint64_t min_proposal_;
  uint64_t accepted_proposal_;
  std::string accepted_value_;
};

class AcceptorImpl final : public Acceptor::Service
{
 private:
  std::map<uint64_t, Proposal_Data> proposal_data_;
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

  bool hasValue = false;
  if ( proposal_data_.find( request->index() ) != proposal_data_.end())
  {
    hasValue = true;
  }

  uint64_t n = request->proposal_number();
  if ( n > proposal_data_[request->index()]. min_proposal_) {
    proposal_data_[request->index()].min_proposal_ = n;
  }

  response->set_accepted_proposal( proposal_data_[request->index()].min_proposal_ );

  if ( hasValue ) {
    response->set_accepted_proposal( proposal_data_[request->index()].accepted_proposal_ );
    response->set_accepted_value( proposal_data_[request->index()].accepted_value_ );
    response->set_has_accepted_value( true );
  }

  return Status::OK;
}

Status AcceptorImpl::Accept( ServerContext* context, const AcceptRequest* request, AcceptResponse* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );

  uint64_t n = request->proposal_number();
  if ( n >= proposal_data_[request->index()].min_proposal_) {
    proposal_data_[request->index()].accepted_proposal_ = n;
    proposal_data_[request->index()].min_proposal_ = n;
    proposal_data_[request->index()].accepted_value_ = std::move( request->value() );
  }
  response->set_min_proposal( proposal_data_[request->index()].min_proposal_ );
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