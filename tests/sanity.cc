// Gtest header
#include <gtest/gtest.h>

// Protobuf headers
#include "kvs.grpc.pb.h"
#include "kvs.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

// std headers
#include <iostream>
#include <memory>

#include <sys/wait.h>
#include <unistd.h>

struct SanityTests
    : public ::testing::Test
{
    pid_t server_pid;
    virtual void SetUp() override {
        server_pid = fork();
        ASSERT_NE( server_pid, -1 );

        if ( server_pid == 0 ) {
            // Child process: Run the server executable
            char serverName[] = "build/server/kvs_server";
            char* server_args[] = { serverName, nullptr };
            ASSERT_NE( execvp( server_args[0], server_args ), -1 );
        }

        // Probably not needed, but wait a bit just to be safe.
        std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );
    }

    virtual void TearDown() override {
        if (server_pid != 0) {
            ASSERT_EQ(kill(server_pid, SIGINT), 0);
            int status;
            waitpid(server_pid, &status, 0);
        }
    }
};

TEST_F( SanityTests, BasicSanityTest )
{
  auto channel = grpc::CreateChannel( "localhost:50051", grpc::InsecureChannelCredentials() );
  std::unique_ptr<KeyValueStore::Kvs::Stub> stub = KeyValueStore::Kvs::NewStub( channel );
  grpc::ClientContext context;

  // 1. Test "Get" on empty kvs
  {
    KeyValueStore::KvsKey request;
    request.set_key( "1" );

    KeyValueStore::KvsValue response;
    ASSERT_FALSE( stub->Get( &context, request, &response ).ok() );
  }

  // 2. Verify "Get" after a "Set" returns the expect value.
  {
    std::string k { "key0" };
    std::string v { "value0" };
    {
      grpc::ClientContext context;
      KeyValueStore::KvsSetRequest request;
      google::protobuf::Empty empty;

      request.set_key( k );
      request.set_value( v );
      ASSERT_TRUE( stub->Set( &context, request, &empty ).ok() );
    }

    {
      grpc::ClientContext context;
      KeyValueStore::KvsKey getRequest;
      getRequest.set_key( k );
      KeyValueStore::KvsValue response;
      ASSERT_TRUE( stub->Get( &context, getRequest, &response ).ok() );

      ASSERT_EQ( response.value(), v );
    }
  }
}