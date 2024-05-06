#include "acceptor.hh"

#include "paxos.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::ServerContext;
using grpc::Status;

using paxos::Acceptor;
using paxos::PrepareRequest;
using paxos::PrepareResponse;
using paxos::AcceptRequest;
using paxos::AcceptResponse;

class AcceptorImpl final : public Acceptor::Service {
private:
    uint64_t m_minProposal;
    std::optional<uint64_t> m_acceptedProposal;
    std::optional<std::string> m_acceptedValue;
    // TODO: FIXME - For now we can use a terminal mutex.
    std::mutex m_mutex;

public:
    AcceptorImpl() : m_minProposal{0}, m_mutex{} {
    }
    ~AcceptorImpl() = default;

    Status Prepare(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) override;
    Status Accept(ServerContext* context, const AcceptRequest* request, AcceptResponse* response) override;
};

Status 
AcceptorImpl::Prepare(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) {
    std::lock_guard<std::mutex> guard(m_mutex);

    uint64_t n = request->proposal_number();
    if (n > m_minProposal) {
        m_minProposal = n;
    }

    bool hasValue = m_acceptedValue.has_value();

    response->set_has_accepted_value(hasValue);

    if (hasValue) {
        response->set_accepted_proposal(m_acceptedProposal.value());
        response->set_accepted_value(m_acceptedValue.value());
    }
    else {
        // FIXME: Is this else block needed ?
        response->set_accepted_proposal(0);
        response->set_accepted_value("");
    }

    std::cerr << "In Acceptor's prepare call request: " << request->proposal_number() << "\n";

    return Status::OK;
}

Status
AcceptorImpl::Accept(ServerContext* context, const AcceptRequest* request, AcceptResponse* response) {
    std::lock_guard<std::mutex> guard(m_mutex);

    uint64_t n = request->proposal_number();
    if (n >= m_minProposal) {
        m_acceptedProposal = n;
        m_minProposal = n;
        m_acceptedValue = std::move(request->value());
    }
    response->set_min_proposal(m_minProposal);
    return Status::OK;
}

void 
RunServer(const std::string &address, const std::stop_source &stop_source)
{
  using namespace std::chrono_literals;

  AcceptorImpl service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Acceptor Server listening on " << address << std::endl;
  
  std::stop_token stoken = stop_source.get_token();
  while(!stoken.stop_requested()) {
    std::this_thread::sleep_for(300ms);
  }
  std::cout << "Exiting...\n";
}

AcceptorService::AcceptorService(const std::string &address)
{
    m_serviceThread = std::jthread(RunServer, address, m_stopSource);
}

AcceptorService::~AcceptorService()
{
    if (m_stopSource.stop_possible()) {
        m_stopSource.request_stop();
    }
    m_serviceThread.join();
}