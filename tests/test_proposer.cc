#include "gtest/gtest.h"
#include "paxos.hh"

/*
TEST( PaxosReplicateTest, ReplicateValue )
{
  // Create a Proposer and Acceptor
  Paxos node0( "paxos/nodes_config.txt", 0 );
  Paxos node1( "paxos/nodes_config.txt", 1 );
  Paxos node2( "paxos/nodes_config.txt", 2 );

  // Set up the value to be replicated
  std::string value = "Hello, world!";

  // Call the Replicate function
  sleep( 5 );
  node0.Propose( value );

  // Verify that the value has been replicated by the acceptor
  std::string replicatedValue = node0.GetValue();
  uint64_t replicatedIndex = node0.GetIndex();
  EXPECT_EQ( replicatedValue, value );
  EXPECT_EQ( replicatedIndex - 1, 0 );
}

TEST( PaxosReplicateTest, NoCorum )
{
  // Create a Proposer and Acceptor
  Paxos node0( "paxos/nodes_config.txt", 0 );

  // Set up the value to be replicated
  std::string value = "Hello, world!";

  // Call the Replicate function
  sleep( 5 );
  node0.Propose( value );

  // Verify that the value has been replicated by the acceptor
  std::string replicatedValue = node0.GetValue();
  uint64_t replicatedIndex = node0.GetIndex();
  EXPECT_EQ( replicatedValue, "" );
}
//This test would change once we communicate the commit to all nodes
TEST( PaxosReplicateTest, CorrectIndex )
{
  // Create a Proposer and Acceptor
  Paxos node0( "paxos/nodes_config.txt", 0 );
  Paxos node1( "paxos/nodes_config.txt", 1 );
  Paxos node2( "paxos/nodes_config.txt", 2 );

  // Set up the value to be replicated
  std::string value = "Hello, world!";
  std::string value1 = "Hello, world!1";
  std::string value2 = "Hello, world!2";
  std::string value3 = "Hello, world!3";

  // Call the Replicate function
  sleep( 5 );
  node0.Propose( value );
  node1.Propose( value1 );
  node2.Propose( value2 );
  node0.Propose( value3 );

  sleep( 5 );

  // Verify that the value has been replicated by the acceptor
  std::string replicatedValue0 = node0.GetValue();
  uint64_t replicatedIndex0 = node0.GetIndex();
  EXPECT_EQ( replicatedValue0, value3 );
  EXPECT_EQ( replicatedIndex0 - 1, 3 );

  std::string replicatedValue1 = node1.GetValue();
  uint64_t replicatedIndex1 = node1.GetIndex();
  EXPECT_EQ( replicatedValue1, value1 );
  EXPECT_EQ( replicatedIndex1 - 1, 1 );

  std::string replicatedValue2 = node2.GetValue();
  uint64_t replicatedIndex2 = node2.GetIndex();
  EXPECT_EQ( replicatedValue2, value2 );
  EXPECT_EQ( replicatedIndex2 - 1, 2 );
}

TEST( PaxosReplicateTest, MultipleValues )
{
  // Create a Proposer and Acceptor
  Paxos node0( "paxos/nodes_config.txt", 0 );
  Paxos node1( "paxos/nodes_config.txt", 1 );
  Paxos node2( "paxos/nodes_config.txt", 2 );
  // Set up multiple values to be replicated
  std::vector<std::string> values = { "Value1", "Value2", "Value3" };
  // Call the Replicate function for each value
  sleep( 5 );
  for ( const auto& value : values ) { node0.Propose( value ); }
  // Verify that the values have been replicated by the acceptor
  std::string replicatedValue = node0.GetValue();
  uint64_t replicatedIndex = node0.GetIndex();
  EXPECT_EQ( replicatedValue, values[2] );
  EXPECT_EQ( replicatedIndex - 1, 2 );
}

TEST( PaxosReplicateTest, SameValue )
{
  // Create a Proposer and Acceptor
  Paxos node0( "paxos/nodes_config.txt", 0 );
  Paxos node1( "paxos/nodes_config.txt", 1 );
  Paxos node2( "paxos/nodes_config.txt", 2 );

  // Set up the value to be replicated
  std::string value = "Hello, world!";

  // Call the Replicate function
  sleep( 5 );
  node0.Propose( value );
  node1.Propose( value );
  node2.Propose( value );

  sleep( 5 );

  // Verify that the value has been replicated by the acceptor
  std::string replicatedValue0 = node0.GetValue();
  uint64_t replicatedIndex0 = node0.GetIndex();
  EXPECT_EQ( replicatedValue0, value );
  EXPECT_EQ( replicatedIndex0 - 1, 0 );

  std::string replicatedValue1 = node1.GetValue();
  uint64_t replicatedIndex1 = node1.GetIndex();
  EXPECT_EQ( replicatedValue1, value );
  EXPECT_EQ( replicatedIndex1 - 1, 0 );

  std::string replicatedValue2 = node2.GetValue();
  uint64_t replicatedIndex2 = node2.GetIndex();
  EXPECT_EQ( replicatedValue2, value );
  EXPECT_EQ( replicatedIndex2 - 1, 0 );
}
*/