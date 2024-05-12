#ifndef PAXOS_HH_
#define PAXOS_HH_

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
  PaxosImpl* paxos_impl_;

public:
  Paxos( const std::string& configFileName, uint8_t nodeId );
  ~Paxos();

  void Replicate( const std::string& value ) ;
  std::string GetValue();
  uint64_t GetIndex();
};

std::vector<Node> parseNodesConfig( const std::string& configFileName );

#endif // PAXOS_HH_