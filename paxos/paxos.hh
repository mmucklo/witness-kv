#ifndef __paxos_hh__
#define __paxos_hh__

#include "common.hh"
//#include "proposer.hh"
//#include "acceptor.hh"

// Protobuf headers
//#include "paxos.grpc.pb.h"
//#include "paxos.pb.h"

// GRPC headers
//#include <grpc/grpc.h>
//#include <grpcpp/create_channel.h>

class PaxosImpl;

struct Node{
    std::string ipAddress;
    int port;
};

class Paxos {
private:
    /*std::vector<Node> m_Nodes;
    std::unique_ptr<Proposer> m_proposer;
    std::unique_ptr<AcceptorService> m_acceptor;

    //std::vector<std::unique_ptr<paxos::Acceptor::Stub>> stubs;

    // get nodeId for now as a quick proto type
    int m_nodeId;*/
    PaxosImpl *m_paxosImpl;

public:
    Paxos(const std::string& configFileName, int nodeId);
    ~Paxos();

    void Replicate(const std::string &value);
};

std::vector<Node> parseNodesConfig(const std::string& configFileName);

#endif // __paxos_hh__