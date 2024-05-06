#ifndef __proposer_hh__
#define __proposer_hh__

#include "common.hh"

class Proposer {
private:
    uint64_t m_roundNumber;
    int majority_threshold;

public:
    Proposer(int num_acceptors)
        : majority_threshold{num_acceptors / 2 + 1},
          m_roundNumber{0} { }
    ~Proposer() = default;

    void Propose(const std::vector<std::string> &nodes, const std::string &value);
};
#endif // __proposer_hh__