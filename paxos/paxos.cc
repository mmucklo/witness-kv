#include "paxos.hh"

#include "acceptor.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "proposer.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

// Paxos Impl class
class PaxosImpl
{
  // private:
 public:
  std::vector<Node> nodes_;
  std::unique_ptr<Proposer> proposer_;
  std::unique_ptr<AcceptorService> acceptor_;

  absl::Mutex paxos_mutex_;
  std::vector<std::unique_ptr<paxos::Acceptor::Stub>> acceptor_stubs_
      ABSL_GUARDED_BY( paxos_mutex_ );
  size_t num_active_acceptors_conns_ ABSL_GUARDED_BY( paxos_mutex_ );

  size_t quorum_;
  uint8_t node_id_;
  uint8_t leader_node_id_ ABSL_GUARDED_BY( paxos_mutex_ );

  std::jthread heartbeat_thread_;
  std::stop_source stop_source_ = {};
  std::chrono::seconds hb_timer_ { 3 };

  // Sends heartbeat/ping messages to all other nodes in `acceptor_stubs_`.
  // This thread then goes to sleep for `hb_timer_` number of seconds.
  // If it detects failure/timeout, it removes that stub from the vector,
  // and next time will attempt to establish a connection hoping the node is
  // back. If it detects a successful re-connection, reinstate the new stub in
  // the vector at the index corresponding to the node.
  void HeartbeatThread( const std::stop_source& stop_source );

 public:
  PaxosImpl() = delete;
  PaxosImpl( const std::string& config_file_name, uint8_t node_id );
  ~PaxosImpl();

  void CreateHeartbeatThread( void );
  void Propose( const std::string& value );
};

PaxosImpl::PaxosImpl( const std::string& config_file_name, uint8_t node_id )
    : num_active_acceptors_conns_ { 0 },
      quorum_ { 0 },
      node_id_ { node_id },
      leader_node_id_ { std::numeric_limits<uint8_t>::max() }
{
  nodes_ = ParseNodesConfig( config_file_name );
  CHECK_NE( nodes_.size(), 0 );

  std::set<std::pair<std::string, int>> s;
  for ( const auto& node : nodes_ ) {
    CHECK( s.insert( { node.ip_address_, node.port } ).second )
        << "Invalid config file : Duplicate IP address and port found";
  }

  proposer_ = std::make_unique<Proposer>( nodes_.size(), node_id );
  CHECK_NE( proposer_, nullptr );
  acceptor_ = std::make_unique<AcceptorService>(
      nodes_[node_id].GetAddressPortStr(), node_id );
  CHECK_NE( acceptor_, nullptr );

  quorum_ = nodes_.size() / 2 + 1;

  acceptor_stubs_.resize( nodes_.size() );
}

PaxosImpl::~PaxosImpl()
{
  if ( stop_source_.stop_possible() ) { stop_source_.request_stop(); }
  if ( heartbeat_thread_.joinable() ) { heartbeat_thread_.join(); }
}

void PaxosImpl::Propose( const std::string& value )
{
  CHECK_NE( this->proposer_, nullptr ) << "Proposer should not be NULL.";

  absl::MutexLock l( &paxos_mutex_ );
  if ( this->num_active_acceptors_conns_ < quorum_ ) {
    // TODO [V]: Fix this with a user specified timeout/deadline for request.
    LOG( WARNING )
        << "Replication not possible, majority of the nodes are not reachable.";
  } else {
    this->proposer_->Propose( this->acceptor_stubs_, value );
  }
}

void PaxosImpl::HeartbeatThread( const std::stop_source& stop_source )
{
  std::stop_token stoken = stop_source.get_token();

  while ( !stoken.stop_requested() ) {
    uint8_t highest_node_id = 0;
    for ( size_t i = 0; i < acceptor_stubs_.size(); i++ ) {
      paxos::PingRequest request;
      paxos::PingResponse response;
      grpc::ClientContext context;
      grpc::Status status;
      if ( acceptor_stubs_[i] ) {
        status = acceptor_stubs_[i]->SendPing( &context, request, &response );
        if ( !status.ok() ) {
          LOG( WARNING ) << "Connection lost with node: " << i;
          absl::MutexLock l( &paxos_mutex_ );
          acceptor_stubs_[i].reset();
          CHECK( num_active_acceptors_conns_ );
          num_active_acceptors_conns_--;
        }
      } else {
        auto channel = grpc::CreateChannel(
            nodes_[i].GetAddressPortStr(), grpc::InsecureChannelCredentials() );
        auto stub = paxos::Acceptor::NewStub( channel );
        status = stub->SendPing( &context, request, &response );
        if ( status.ok() ) {
          LOG( INFO ) << "Connection established with node: " << i;
          absl::MutexLock l( &paxos_mutex_ );
          acceptor_stubs_[i] = std::move( stub );
          num_active_acceptors_conns_++;
        }
      }

      if ( status.ok() ) {
        highest_node_id = std::max(
            highest_node_id, static_cast<uint8_t>( response.node_id() ) );
      }
    }

    {
      absl::MutexLock l( &paxos_mutex_ );
      if ( leader_node_id_ != highest_node_id ) {
        LOG( INFO ) << "New leader elected with node id: "
                    << static_cast<uint32_t>( highest_node_id );
        leader_node_id_ = highest_node_id;
      }
    }

    std::this_thread::sleep_for( this->hb_timer_ );
  }

  LOG( INFO ) << "Shutting down heartbeat thread.";
}

void PaxosImpl::CreateHeartbeatThread( void )
{
  auto hb_thread
      = std::bind( &PaxosImpl::HeartbeatThread, this, std::placeholders::_1 );
  heartbeat_thread_ = std::jthread( hb_thread, stop_source_ );
}

Paxos::Paxos( const std::string& config_file_name, uint8_t node_id )
{
  paxos_impl_ = new PaxosImpl( config_file_name, node_id );
  paxos_impl_->CreateHeartbeatThread();
}

Paxos::~Paxos() { delete paxos_impl_; }

void Paxos::Propose( const std::string& value )
{
  this->paxos_impl_->Propose( value );
}

std::string Paxos::GetValue() { return paxos_impl_->proposer_->GetValue(); }

uint64_t Paxos::GetIndex() { return paxos_impl_->proposer_->GetIndex(); }

std::vector<Node> ParseNodesConfig( const std::string& config_file_name )
{
  std::vector<Node> nodes {};
  std::ifstream config_file( config_file_name );

  CHECK( config_file.is_open() ) << "Failed to open nodes configuration file";

  std::string line;
  while ( std::getline( config_file, line ) ) {
    std::stringstream ss( line );
    std::string ip_address, port_str;
    int port;
    if ( std::getline( ss, ip_address, ':' ) && std::getline( ss, port_str ) ) {
      try {
        port = std::stoi( port_str );
      } catch ( const std::invalid_argument& e ) {
        throw std::runtime_error( "Invalid port number in config file" );
      }
      nodes.push_back( { ip_address, port } );
    }
  }

  config_file.close();

  return nodes;
}