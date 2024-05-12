#include "paxos.hh"

#include <fstream>
#include <memory>
#include <semaphore>
#include <set>
#include <sstream>

#include "acceptor.hh"
#include "proposer.hh"

#include "paxos.grpc.pb.h"
#include "paxos.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

static void validateUniqueNodes( const std::vector<Node>& nodes );

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
  PaxosImpl( const std::string& configFileName, uint8_t nodeId );
  ~PaxosImpl() = default;
};

PaxosImpl::PaxosImpl( const std::string& configFileName, uint8_t nodeId )
{
  nodes_ = parseNodesConfig( configFileName );
  validateUniqueNodes( nodes_ );

  node_id_ = nodeId;

  proposer_ = std::make_unique<Proposer>( nodes_.size(), nodeId );

  acceptor_ = std::make_unique<AcceptorService>( nodes_[nodeId].getAddressPortStr() );

  acceptor_stubs_.resize( nodes_.size() );

  for ( size_t i = 0; i < nodes_.size();  ) {
    auto channel = grpc::CreateChannel( nodes_[i].getAddressPortStr(), grpc::InsecureChannelCredentials() );
    auto lStub = paxos::Acceptor::NewStub( channel );
    grpc::ClientContext context;
    google::protobuf::Empty request;
    google::protobuf::Empty response;

    if (!lStub->SendPing(&context, request, &response).ok()) {
      continue;
    }
    acceptor_stubs_[i] = std::move(lStub);
    i++;
  }
}

Paxos::Paxos( const std::string& configFileName, uint8_t nodeId )
{
  paxos_impl_ = new PaxosImpl( configFileName, nodeId );
}

Paxos::~Paxos()
{
  delete paxos_impl_;
}

void Paxos::Replicate( const std::string& value )
{
  paxos_impl_->proposer_->Propose( this->paxos_impl_->acceptor_stubs_, value );
}

std::string Paxos::GetValue()
{
  return paxos_impl_->proposer_->GetValue();
}

uint64_t Paxos::GetIndex()
{
  return paxos_impl_->proposer_->GetIndex();
}


std::vector<Node> parseNodesConfig( const std::string& configFileName )
{
  std::vector<Node> nodes {};
  std::ifstream configFile( configFileName );

  if ( !configFile.is_open() ) {
    throw std::runtime_error( "Failed to open nodes configuration file ");
  }

  std::string line;
  while ( std::getline( configFile, line ) ) {
    std::stringstream ss( line );
    std::string ipAddress, portStr;
    int port;
    if ( std::getline( ss, ipAddress, ':' ) && std::getline( ss, portStr ) ) {
      try {
        port = std::stoi( portStr );
      } catch ( const std::invalid_argument& e ) {
        throw std::runtime_error( "Invalid port number in config file" );
      }
      nodes.push_back( { ipAddress, port } );
    }
  }

  configFile.close();

  return nodes;
}

static void validateUniqueNodes( const std::vector<Node>& nodes )
{
  std::set<std::pair<std::string, int>> s;
  for ( const auto& node : nodes ) {
    if ( !s.insert( { node.ipAddress, node.port } ).second ) {
      throw std::runtime_error( "Invalid config file : Duplicate IP address and port found in configuration\n" );
    }
  }
}