// Gtest header
#include <gtest/gtest.h>

// Protobuf headers
#include "kvs.grpc.pb.h"
#include "kvs.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

// std headers
#include <sys/wait.h>
#include <unistd.h>

#include <iostream>
#include <memory>

struct SanityTests : public ::testing::Test
{
  pid_t server_pid;
  virtual void SetUp() override
  {
    server_pid = fork();
    ASSERT_NE( server_pid, -1 );

    if ( server_pid == 0 ) {
      // Child process: Run the server executable
      char serverName[] = "build/server/kvs_server";
      char arg1[] = "50062";
      char arg2[] = "1";
      char* server_args[] = { serverName, arg1, arg2, nullptr };
      ASSERT_NE( execvp( server_args[0], server_args ), -1 );
    }

    // Probably not needed, but wait a bit just to be safe.
    std::this_thread::sleep_for( std::chrono::milliseconds( 400 ) );
  }

  virtual void TearDown() override
  {
    if ( server_pid != 0 ) {
      ASSERT_EQ( kill( server_pid, SIGINT ), 0 );
      int status;
      waitpid( server_pid, &status, 0 );
    }
  }
};

TEST_F( SanityTests, BasicSanityTest )
{
  auto channel = grpc::CreateChannel( "0.0.0.0:50062",
                                      grpc::InsecureChannelCredentials() );
  std::unique_ptr<KeyValueStore::Kvs::Stub> stub
      = KeyValueStore::Kvs::NewStub( channel );
  grpc::ClientContext context;

  // 1. Test "Get" on empty kvs
  {
    KeyValueStore::GetRequest request;
    request.set_key( "1" );

    KeyValueStore::GetResponse response;
    ASSERT_FALSE( stub->Get( &context, request, &response ).ok() );
  }

  // 2. Verify "Get" after a "Set" returns the expect value.
  {
    std::string k { "key0" };
    std::string v { "value0" };
    {
      grpc::ClientContext context;
      KeyValueStore::PutRequest request;
      KeyValueStore::PutResponse response;

      request.set_key( k );
      request.set_value( v );
      ASSERT_TRUE( stub->Put( &context, request, &response ).ok() );
    }

    {
      grpc::ClientContext context;
      KeyValueStore::GetRequest getRequest;
      getRequest.set_key( k );
      KeyValueStore::GetResponse response;
      ASSERT_TRUE( stub->Get( &context, getRequest, &response ).ok() );

      ASSERT_EQ( response.value(), v );
    }
  }
}