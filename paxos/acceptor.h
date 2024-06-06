#ifndef PAXOS_ACCEPTOR_H_
#define PAXOS_ACCEPTOR_H_

#include "replicated_log.h"

namespace witnesskvs::paxos {

class AcceptorService {
 private:
  std::jthread service_thread_;
  std::stop_source stop_source_ = {};
  uint8_t node_id_;

 public:
  AcceptorService(const std::string& address, uint8_t node_id,
                  std::shared_ptr<ReplicatedLog> rlog);
  ~AcceptorService();
};
}  // namespace witnesskvs::paxos
#endif  // ACCEPTOR_HH_
