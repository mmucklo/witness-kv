#ifndef PAXOS_HH_
#define PAXOS_HH_

#include "common.hh"

class PaxosImpl;

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

#endif  // PAXOS_HH_