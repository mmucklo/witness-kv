#ifndef __acceptor_hh__
#define __acceptor_hh__

#include "common.hh"

class AcceptorService
{
private:
  // std::unique_ptr<AcceptorImpl> m_acceptorImpl;
  std::jthread m_serviceThread;
  std::stop_source m_stopSource = {};

public:
  AcceptorService( const std::string& address );
  ~AcceptorService();
};
#endif // __acceptor_hh__