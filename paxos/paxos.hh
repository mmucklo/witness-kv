#ifndef __paxos_hh__
#define __paxos_hh__

#include "common.hh"

class PaxosImpl;

struct Node
{
  std::string ipAddress;
  int port;
  std::string getAddressPortStr() {
    return this->ipAddress + ":" + std::to_string( this->port );
  }
};

class Paxos
{
private:
  PaxosImpl* m_paxosImpl;

public:
  Paxos( const std::string& configFileName, uint32_t nodeId );
  ~Paxos();

  void Replicate( const std::string& value );
};

std::vector<Node> parseNodesConfig( const std::string& configFileName );

#endif // __paxos_hh__