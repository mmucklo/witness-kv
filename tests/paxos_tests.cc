// Gtest header
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <ostream>

#include "paxos.hh"

// Simple test to sanity check file parsing logic.
TEST( PaxosSanity, ConfigFileParseTest )
{
  std::vector<std::string> addrs = { "0.0.0.0", "0.1.2.3", "8.7.6.5", "10.10.10.10" };
  std::vector<std::string> ports = { "10", "20", "30", "40" };
  ASSERT_EQ( addrs.size(), ports.size() );

  char filename[] = "/tmp/paxos_config_file_test";
  std::ofstream temp_file( filename );
  ASSERT_TRUE( temp_file.is_open() ) << "Failed to create temporary file\n";

  for ( size_t i = 0; i < addrs.size(); i++ ) {
    temp_file << addrs[i] << ":" << ports[i] << "\n";
  }
  temp_file << std::endl;

  auto nodes = ParseNodesConfig( filename );
  ASSERT_EQ( addrs.size(), nodes.size() );

  for ( size_t i = 0; i < addrs.size(); i++ ) {
    ASSERT_EQ( addrs[i], nodes[i].ip_address_ );
    ASSERT_EQ( std::stoi( ports[i] ), nodes[i].port );
  }

  temp_file.close();
  ASSERT_EQ(remove( filename ), 0);
}