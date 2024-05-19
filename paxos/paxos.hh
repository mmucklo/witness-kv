#ifndef PAXOS_HH_
#define PAXOS_HH_

#include "common.hh"

class PaxosImpl;

struct Node
{
  std::string ip_address_;
  int port;
  std::string GetAddressPortStr() const
  {
    return this->ip_address_ + ":" + std::to_string( this->port );
  }
};

class Paxos
{
 private:
  PaxosImpl* paxos_impl_;

 public:
  Paxos( const std::string& config_file_name, uint8_t node_id );
  ~Paxos();

  void Propose( const std::string& value );
  std::string GetValue();
  uint64_t GetIndex();
};

std::vector<Node> ParseNodesConfig( const std::string& config_file_name );

#endif  // PAXOS_HH_