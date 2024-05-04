#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using paxos::Proposer;
using paxos::Acceptor;
using paxos::Leader;
using paxos::LeaderElection;
using paxos::PrepareRequest;
using paxos::PrepareResponse;
using paxos::AcceptRequest;
using paxos::AcceptResponse;
using paxos::ProposeRequest;
using paxos::ProposeResponse;
using paxos::RequestVoteRequest;
using paxos::RequestVoteResponse;

class ProposerImpl final : public Proposer::Service {
private:
    int majority_threshold;
    std::unordered_map<int, std::vector<PrepareResponse>> prepare_responses;

public:
    ProposerImpl(int num_acceptors) : majority_threshold(num_acceptors / 2 + 1) {}

    Status Prepare(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) override {
        prepare_responses[request->round()].push_back(*response);
        if (prepare_responses[request->round()].size() >= majority_threshold) {
            AcceptRequest accept_request;
            accept_request.set_round(request->round());
            accept_request.set_proposal_id(request->proposal_id());
            accept_request.set_value(prepare_responses[request->round()][0].value()); // Simplified assumption

            AcceptResponse accept_response;
        }
        return Status::OK;
    }

    Status Accept(ServerContext* context, const AcceptRequest* request, AcceptResponse* response) override {
        // Handle accept request logic here
        return Status::OK;
    }
};

class AcceptorImpl final : public Acceptor::Service {
private:
    int last_promised_round = 0;
    int last_accepted_round = 0;
    std::string last_accepted_value;

public:
    Status ReceivePrepare(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) override {
        if (request->round() > last_promised_round) {
            last_promised_round = request->round();
            response->set_round(request->round());
            response->set_proposal_id(request->proposal_id());
            if (last_accepted_round > 0) {
                response->set_value(last_accepted_value);
            }
        }
        return Status::OK;
    }

    Status SendPromise(ServerContext* context, const PrepareRequest* request, PrepareResponse* response) override {
        // This might simply be part of ReceivePrepare
        return Status::OK;
    }

    Status SendAccept(ServerContext* context, const AcceptRequest* request, AcceptResponse* response) override {
        if (request->round() >= last_promised_round) {
            last_accepted_round = request->round();
            last_accepted_value = request->value();
        }
        return Status::OK;
    }
};

class LeaderImpl final : public Leader::Service {
private:
    int current_leader_id;
    int my_id;

public:
    LeaderImpl(int my_id) : my_id(my_id), current_leader_id(-1) {}

    void set_leader_id(int id) {
        current_leader_id = id;
    }

    Status Propose(ServerContext* context, const ProposeRequest* request, ProposeResponse* response) override {
        if (my_id == current_leader_id) {
            std::cout << "Leader " << my_id << " accepted proposal: " << request->value() << std::endl;
            response->set_success(true);
        } else {
            std::cout << "Node " << my_id << " rejected proposal as it is not the leader." << std::endl;
            response->set_success(false);
        }
        return Status::OK;
    }
};

class LeaderElectionImpl final : public LeaderElection::Service {
private:
    int my_id;
    std::map<int, std::unique_ptr<LeaderElection::Stub>> peers;
    LeaderImpl* leader_service;

public:
    LeaderElectionImpl(int id, const std::map<int, std::string>& peer_addresses, LeaderImpl* leader_service)
        : my_id(id), leader_service(leader_service) {
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(INT_MAX);
        for (const auto& peer : peer_addresses) {
            if (peer.first != my_id) {  // Do not create a stub for this node itself
                peers[peer.first] = LeaderElection::NewStub(
                    grpc::CreateCustomChannel(peer.second, grpc::InsecureChannelCredentials(), args));
            }
        }
    }

    Status Election(ServerContext* context, const paxos::ElectionMessage* request, paxos::ElectionResponse* response) {
        std::cout << "Node " << my_id << " received election message from node " << request->node_id() << std::endl;
        if (request->node_id() > my_id) {
            response->set_ack(true);
        } else {
            response->set_ack(false);
            if (peers.empty()) {
                // No higher nodes, declare itself as leader
                leader_service->set_leader_id(my_id);
                std::cout << "Node " << my_id << " declares itself as leader." << std::endl;
            }
        }
        return Status::OK;
    }

    void initiate_election() {
        bool higher_exists = false;
        for (const auto& peer : peers) {
            if (peer.first > my_id) {
		paxos::ElectionMessage msg;
                msg.set_node_id(my_id);
		paxos::ElectionResponse resp;
                grpc::ClientContext context;

                grpc::Status status = peer.second->Election(&context, msg, &resp);
                if (status.ok() && resp.ack()) {
                    higher_exists = true;
                    std::cout << "Higher node " << peer.first << " acknowledged election." << std::endl;
                    break;
                }
            }
        }

        if (!higher_exists) {
            // No higher nodes responded, declare itself as leader
            leader_service->set_leader_id(my_id);
            std::cout << "Node " << my_id << " is now the leader after election." << std::endl;
        }
    }
};
