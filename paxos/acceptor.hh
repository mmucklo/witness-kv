#ifndef ACCEPTOR_HH_
#define ACCEPTOR_HH_

#include "common.hh"

class AcceptorService
{
 private:
  // std::unique_ptr<AcceptorImpl> m_acceptorImpl;
  std::jthread service_thread;
  std::stop_source stop_source = {};

 public:
  AcceptorService( const std::string& address );
  ~AcceptorService();
};
#endif // ACCEPTOR_HH_