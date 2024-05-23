#include "leader.hh"
#include "proposer.hh"

#include "paxos.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::ServerContext;
using grpc::Status;

using paxos::Leader;
using paxos::ProposeRequest;

class LeaderImpl final : public Leader::Service
{
 private:
    std::mutex mutex_;
    std::unique_ptr<Proposer> proposer_;

    uint8_t node_id_;


 public:
    LeaderImpl() = delete;
    LeaderImpl(uint8_t node_id, std::shared_ptr<ReplicatedLog> replicated_log, std::shared_ptr<PaxosNode> paxos_node); // Add declaration for the constructor

    ~LeaderImpl() = default;

    Status Propose( ServerContext* context, const ProposeRequest* request, google::protobuf::Empty* response ) override;
};

// Define the constructor outside of the class definition
LeaderImpl::LeaderImpl(uint8_t node_id, std::shared_ptr<ReplicatedLog> replicated_log, std::shared_ptr<PaxosNode> paxos_node)
    : mutex_ {}, node_id_{node_id}
{
    proposer_ = std::make_unique<Proposer>( paxos_node->GetNumNodes(), node_id,
                                            replicated_log, paxos_node );
}

Status LeaderImpl::Propose( ServerContext* context, const ProposeRequest* request, google::protobuf::Empty* response )
{
  std::lock_guard<std::mutex> guard( mutex_ );
  proposer_->Propose( request->value() );
  return Status::OK;
}

void RunLeaderServer( const std::string& address, uint8_t node_id, const std::stop_source& stop_source, 
                      std::shared_ptr<ReplicatedLog> replicated_log, std::shared_ptr<PaxosNode> paxos_node)
{
  LOG(INFO) << "Starting Leader service. " << address;
   using namespace std::chrono_literals;

  LeaderImpl service{node_id, replicated_log, paxos_node};

  grpc::ServerBuilder builder;
  builder.AddListeningPort( address, grpc::InsecureServerCredentials() );
  builder.RegisterService( &service );

  std::unique_ptr<grpc::Server> server( builder.BuildAndStart() );

  std::stop_token stoken = stop_source.get_token();
  while ( !stoken.stop_requested() ) {
    std::this_thread::sleep_for( 300ms );
  }
  LOG(INFO) << "Shutting down Leader service.";
}

LeaderService::LeaderService( const std::string& address, uint8_t node_id, std::shared_ptr<ReplicatedLog> replicated_log,
                              std::shared_ptr<PaxosNode> paxos_node)
  : node_id_{node_id}
{

  service_thread_ = std::jthread( RunLeaderServer, address, node_id, stop_source_, replicated_log, paxos_node);
}

LeaderService::~LeaderService()
{
  if ( stop_source_.stop_possible() ) {
    stop_source_.request_stop();
  }
  if (service_thread_.joinable()) {
    service_thread_.join();
  }
}