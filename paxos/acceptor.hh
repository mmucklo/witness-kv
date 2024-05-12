#ifndef ACCEPTOR_HH_
#define ACCEPTOR_HH_

#include "common.hh"

class AcceptorService
{
 private:
  std::jthread service_thread_;
  std::stop_source stop_source_ = {};
  uint8_t node_id_;

 public:
  AcceptorService( const std::string& address, uint8_t node_id );
  ~AcceptorService();
};
#endif // ACCEPTOR_HH_