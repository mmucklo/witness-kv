#include "node.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

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

PaxosNode::PaxosNode( const std::string& config_file_name, uint8_t node_id,
                      std::shared_ptr<ReplicatedLog> rlog )
    : num_active_acceptors_conns_ {}, replicated_log_ { rlog }
{
  nodes_ = ParseNodesConfig( config_file_name );
  CHECK_NE( nodes_.size(), 0 );

  std::set<std::pair<std::string, int>> s;
  for ( const auto& node : nodes_ ) {
    CHECK( s.insert( { node.ip_address_, node.port_ } ).second )
        << "Invalid config file : Duplicate IP address and port found";
  }

  node_id_ = node_id;
  quorum_ = nodes_.size() / 2 + 1;

  acceptor_stubs_.resize( nodes_.size() );
}

PaxosNode::~PaxosNode()
{
  if ( hb_ss_.stop_possible() ) { hb_ss_.request_stop(); }
  if ( heartbeat_thread_.joinable() ) { heartbeat_thread_.join(); }

  if ( commit_ss_.stop_possible() ) {
    commit_cv_.notify_one();
    commit_ss_.request_stop();
  }

  if ( commit_thread_.joinable() ) {
    commit_cv_.notify_one();
    commit_thread_.join();
  }
}

void PaxosNode::HeartbeatThread( const std::stop_source& ss )
{
  std::stop_token stoken = ss.get_token();

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
          std::lock_guard<std::mutex> guard( node_mutex_ );
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
          std::lock_guard<std::mutex> guard( node_mutex_ );
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
      std::lock_guard<std::mutex> guard( node_mutex_ );
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

void PaxosNode::CommitThread( const std::stop_source& ss )
{
  std::stop_token stoken = ss.get_token();
  std::unique_lock lk( node_mutex_ );
  while ( true ) {
    if ( stoken.stop_requested() ) { break; }
    VLOG( 1 ) << "Commit thread going to sleep.";
    commit_cv_.wait( lk );
    if ( stoken.stop_requested() ) { break; }

    VLOG( 1 ) << "Commit thread woken up..";

    // For each peer node attempt to get all their unchosen indices chosen
    // that you know are chosen.
    for ( size_t i = 0; i < GetNumNodes(); i++ ) {
      if ( i == node_id_ ) { continue; }

      while ( last_requested_commit_index_[i]
              < this->replicated_log_->GetFirstUnchosenIdx() ) {
        paxos::CommitRequest commit_request;
        commit_request.set_index( last_requested_commit_index_[i] );
        commit_request.set_value(
            this->replicated_log_
                ->GetLogEntryAtIdx( last_requested_commit_index_[i] )
                .accepted_value_ );
        paxos::CommitResponse commit_response;

        grpc::ClientContext context;
        grpc::Status status;
        if ( acceptor_stubs_[i] ) {
          status = acceptor_stubs_[i]->Commit( &context, commit_request,
                                               &commit_response );
        } else {
          status = grpc::Status( grpc::StatusCode::UNAVAILABLE,
                                 "Acceptor not available right now." );
        }

        if ( !status.ok() ) { break; }

        // TODO[V]: We can't satisfy this invariant right now because nodes
        // going down and coming back up are not using the stable log yet, once
        // we have that this should always be true.
        // CHECK_LT( last_requested_commit_index_[i],
        //          commit_response.first_unchosen_index() )
        //    << "This index was just committed, the response must return an "
        //       "index beyond it";
        last_requested_commit_index_[i]
            = commit_response.first_unchosen_index();
      }
    }
  }

  LOG( INFO ) << "Shutting down Commit thread";
}

void PaxosNode::MakeReady()
{
  auto hb_thread
      = std::bind( &PaxosNode::HeartbeatThread, this, std::placeholders::_1 );
  heartbeat_thread_ = std::jthread( hb_thread, hb_ss_ );

  last_requested_commit_index_.resize( GetNumNodes(), 0 );

  auto t = std::bind( &PaxosNode::CommitThread, this, std::placeholders::_1 );
  commit_thread_ = std::jthread( t, commit_ss_ );
}

void PaxosNode::CommitInBackground( const std::vector<uint64_t>& commit_idxs )
{
  {
    std::lock_guard<std::mutex> guard( node_mutex_ );
    for ( size_t i = 0; i < GetNumNodes(); i++ ) {
      last_requested_commit_index_[i]
          = std::max( last_requested_commit_index_[i], commit_idxs[i] );
    }
  }
  commit_cv_.notify_one();
}

std::string PaxosNode::GetNodeAddress( uint8_t node_id ) const
{
  return nodes_[node_id].GetAddressPortStr();
}

bool PaxosNode::ClusterHasEnoughNodesUp()
{
  std::lock_guard<std::mutex> guard( node_mutex_ );
  return this->num_active_acceptors_conns_ >= quorum_;
}

grpc::Status PaxosNode::PrepareGrpc( uint8_t node_id,
                                     paxos::PrepareRequest request,
                                     paxos::PrepareResponse* response )
{
  std::lock_guard<std::mutex> guard( node_mutex_ );
  grpc::ClientContext context;
  if ( acceptor_stubs_[node_id] ) {
    return acceptor_stubs_[node_id]->Prepare( &context, request, response );
  } else {
    return grpc::Status( grpc::StatusCode::UNAVAILABLE,
                         "Acceptor not available right now." );
  }
}

grpc::Status PaxosNode::AcceptGrpc( uint8_t node_id,
                                    paxos::AcceptRequest request,
                                    paxos::AcceptResponse* response )
{
  std::lock_guard<std::mutex> guard( node_mutex_ );
  grpc::ClientContext context;
  if ( acceptor_stubs_[node_id] ) {
    return acceptor_stubs_[node_id]->Accept( &context, request, response );
  } else {
    return grpc::Status( grpc::StatusCode::UNAVAILABLE,
                         "Acceptor not available right now." );
  }
}

grpc::Status PaxosNode::CommitGrpc( uint8_t node_id,
                                    paxos::CommitRequest request,
                                    paxos::CommitResponse* response )
{
  std::lock_guard<std::mutex> guard( node_mutex_ );
  grpc::ClientContext context;
  if ( acceptor_stubs_[node_id] ) {
    return acceptor_stubs_[node_id]->Commit( &context, request, response );
  } else {
    return grpc::Status( grpc::StatusCode::UNAVAILABLE,
                         "Acceptor not available right now." );
  }
}

grpc::Status PaxosNode::SendPingGrpc( uint8_t node_id,
                                      paxos::PingRequest request,
                                      paxos::PingResponse* response )
{
  std::lock_guard<std::mutex> guard( node_mutex_ );
  grpc::ClientContext context;
  if ( acceptor_stubs_[node_id] ) {
    return acceptor_stubs_[node_id]->SendPing( &context, request, response );
  } else {
    return grpc::Status( grpc::StatusCode::UNAVAILABLE,
                         "Acceptor not available right now." );
  }
}