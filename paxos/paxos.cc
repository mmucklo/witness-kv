#include "paxos.hh"

#include "acceptor.hh"
#include "proposer.hh"

#include "paxos.grpc.pb.h"
#include "paxos.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

// Paxos Impl class
class PaxosImpl
{
 public:
  std::vector<Node> nodes_;
  std::unique_ptr<Proposer> proposer_;
  std::unique_ptr<AcceptorService> acceptor_;

  std::vector<std::unique_ptr<paxos::Acceptor::Stub>> acceptor_stubs_;

  // get nodeId for now as a quick proto type
  uint8_t node_id_;

 public:
  PaxosImpl() = delete;
  PaxosImpl( const std::string& config_file_name, uint8_t node_id );
  ~PaxosImpl() = default;
};

PaxosImpl::PaxosImpl( const std::string& config_file_name, uint8_t node_id )
{
  nodes_ = ParseNodesConfig( config_file_name );

  std::set<std::pair<std::string, int>> s;
  for ( const auto& node : nodes_ ) {
    CHECK(s.insert( { node.ip_address_, node.port } ).second) <<
      "Invalid config file : Duplicate IP address and port found";
  }

  node_id_ = node_id;

  proposer_ = std::make_unique<Proposer>( nodes_.size(), node_id );
  CHECK_NE(proposer_, nullptr);
  acceptor_ = std::make_unique<AcceptorService>( nodes_[node_id].GetAddressPortStr() );
  CHECK_NE(acceptor_, nullptr);

  acceptor_stubs_.resize( nodes_.size() );

  for ( size_t i = 0; i < nodes_.size();  ) {
    auto channel = grpc::CreateChannel( nodes_[i].GetAddressPortStr(), grpc::InsecureChannelCredentials() );
    auto stub = paxos::Acceptor::NewStub( channel );
    grpc::ClientContext context;
    google::protobuf::Empty request;
    google::protobuf::Empty response;

    if (!stub->SendPing(&context, request, &response).ok()) {
      continue;
    }
    acceptor_stubs_[i] = std::move(stub);
    LOG(INFO) << "Established connection with node: " << i;
    i++;
  }
}

Paxos::Paxos( const std::string& config_file_name, uint8_t node_id )
{
  paxos_impl_ = new PaxosImpl( config_file_name, node_id );
}

Paxos::~Paxos()
{
  delete paxos_impl_;
}

void Paxos::Propose( const std::string& value )
{
  CHECK_NE(paxos_impl_->proposer_, nullptr) << "Proposer should not be NULL.";
  paxos_impl_->proposer_->Propose( this->paxos_impl_->acceptor_stubs_, value );
}

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
