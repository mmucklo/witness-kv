#include "paxos.hh"

#include <fstream>
#include <memory>
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
  std::vector<Node> m_Nodes;
  std::unique_ptr<Proposer> m_proposer;
  std::unique_ptr<AcceptorService> m_acceptor;

  std::vector<std::unique_ptr<paxos::Acceptor::Stub>> m_acceptorStubs;

  // get nodeId for now as a quick proto type
  int m_nodeId;

public:
  PaxosImpl() = delete;
  PaxosImpl( const std::string& configFileName, int nodeId );
  ~PaxosImpl() = default;
};

PaxosImpl::PaxosImpl( const std::string& configFileName, int nodeId )
{
  m_Nodes = parseNodesConfig( configFileName );
  validateUniqueNodes( m_Nodes );

  m_nodeId = nodeId;

  m_proposer = std::make_unique<Proposer>( m_Nodes.size() );

  std::string str = m_Nodes[nodeId].ipAddress + ":" + std::to_string( m_Nodes[nodeId].port );
  std::cout << "paxos : node: ip address: " << str << "\n";
  m_acceptor = std::make_unique<AcceptorService>( str );

  m_acceptorStubs.resize( m_Nodes.size() );

  std::cout << "Getting stubs\n";
  for ( size_t i = 0; i < m_Nodes.size(); i++ ) {
    auto channel = grpc::CreateChannel( str, grpc::InsecureChannelCredentials() );
    m_acceptorStubs[i] = paxos::Acceptor::NewStub( channel );
  }
}

Paxos::Paxos( const std::string& configFileName, int nodeId )
{
  std::cout << "Init paxos on node: " << nodeId << std::endl;
  m_paxosImpl = new PaxosImpl( configFileName, nodeId );
}

Paxos::~Paxos()
{
  delete m_paxosImpl;
  std::cout << "Destroying Paxos object!\n";
}

void Paxos::Replicate( const std::string& value )
{
  std::cout << "Paxos: Got request to replicate following value: " << value << "\n";
  m_paxosImpl->m_proposer->Propose( this->m_paxosImpl->m_acceptorStubs, value );
}

std::vector<Node> parseNodesConfig( const std::string& configFileName )
{
  std::vector<Node> nodes {};
  std::ifstream configFile( configFileName );

  if ( !configFile.is_open() ) {
    throw std::runtime_error( "Failed to open nodes configuration file" );
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
