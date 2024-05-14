#include "gtest/gtest.h"
#include "paxos.hh"


TEST(PaxosReplicateTest, ReplicateValue)
{
    // Create a Proposer and Acceptor
    Paxos node0("paxos/node_config.txt", 0);
    Paxos node1("paxos/node_config.txt", 1);
    Paxos node2("paxos/node_config.txt", 2);

    // Set up the value to be replicated
    std::string value = "Hello, world!";

    // Call the Replicate function
    node0.Propose(value);

    // Verify that the value has been replicated by the acceptor
    std::string replicatedValue = node0.GetValue();
    uint64_t replicatedIndex = node0.GetIndex();
    EXPECT_EQ(replicatedValue, value);
    EXPECT_EQ(replicatedIndex, 1);
}

/*more test to simulate consesu failure and prep failure in next iteration*/
