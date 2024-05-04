// Gtest header
#include <gtest/gtest.h>

#include <iostream>
#include <ostream>
#include "paxos.hh"

TEST( PaxosTests, BasicPaxosTest )
{
    Paxos p{"paxos/nodes_config.txt"};
}