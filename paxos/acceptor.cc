#include "acceptor.hh"

Status 
AcceptorService::Prepare(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) {
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

    return Status::OK;
}

Status
AcceptorService::Accept(ServerContext* context, const AcceptRequest* request, AcceptResponse* response) {
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