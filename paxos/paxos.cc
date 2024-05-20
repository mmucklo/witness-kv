#include "paxos.hh"

#include "acceptor.hh"
#include "node.hh"
#include "paxos.grpc.pb.h"
#include "paxos.pb.h"
#include "proposer.hh"
#include "replicated_log.hh"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

#include <memory>

// Paxos Impl class
class PaxosImpl
{
 public:
  // private:
  std::shared_ptr<ReplicatedLog> replicated_log_;
  std::shared_ptr<PaxosNode> paxos_node_;
  std::unique_ptr<Proposer> proposer_;
  std::unique_ptr<AcceptorService> acceptor_;

 public:
  PaxosImpl() = delete;
  PaxosImpl( const std::string& config_file_name, uint8_t node_id );
  ~PaxosImpl();

  void Propose( const std::string& value );
};

PaxosImpl::PaxosImpl( const std::string& config_file_name, uint8_t node_id )
{
  replicated_log_ = std::make_shared<ReplicatedLog>( node_id );
  paxos_node_ = std::make_shared<PaxosNode>( config_file_name, node_id,
                                             replicated_log_ );

  proposer_ = std::make_unique<Proposer>( paxos_node_->GetNumNodes(), node_id,
                                          replicated_log_, paxos_node_ );
  CHECK_NE( proposer_, nullptr );

  acceptor_ = std::make_unique<AcceptorService>(
      paxos_node_->GetNodeAddress( node_id ), node_id, replicated_log_ );
  CHECK_NE( acceptor_, nullptr );

  paxos_node_->MakeReady();
}

PaxosImpl::~PaxosImpl() {}

void PaxosImpl::Propose( const std::string& value )
{
  CHECK_NE( this->proposer_, nullptr ) << "Proposer should not be NULL.";

  // absl::MutexLock l( &paxos_mutex_ );
  // if ( this->num_active_acceptors_conns_ < quorum_ ) {
  //  TODO [V]: Fix this with a user specified timeout/deadline for request.
  // LOG( WARNING )
  //    << "Replication not possible, majority of the nodes are not reachable.";
  //}
  // else {
  this->proposer_->Propose( value );
  //}
}

Paxos::Paxos( const std::string& config_file_name, uint8_t node_id )
{
  paxos_impl_ = new PaxosImpl( config_file_name, node_id );
}

Paxos::~Paxos() { delete paxos_impl_; }

void Paxos::Propose( const std::string& value )
{
  this->paxos_impl_->Propose( value );
}

std::string Paxos::GetValue() { return paxos_impl_->proposer_->GetValue(); }

uint64_t Paxos::GetIndex() { return paxos_impl_->proposer_->GetIndex(); }