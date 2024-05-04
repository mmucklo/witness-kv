#ifndef __paxos_hh__
#define __paxos_hh__

#include "common.hh"

struct Node{
    std::string ipAddress;
    int port;
};

class Paxos {
private:
    std::vector<Node> m_Nodes;
    void parseNodesConfig(const std::string& configFileName);
public:
    Paxos(const std::string& configFileName);
    ~Paxos() = default;
};
#endif // __paxos_hh__