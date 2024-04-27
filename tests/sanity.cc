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

TEST( Sanity, BasicSanityTest )
{
  pid_t server_pid = fork();
  ASSERT_NE( server_pid, -1 );

  if ( server_pid == 0 ) {
    // Child process: Run the server executable
    char serverName[] = "kvs_server";
    char* server_args[] = { serverName, nullptr };
    execvp( server_args[0], server_args );
    exit( 1 ); // Should not reach here if execvp succeeds
  }

  // Probably not needed, but wait a bit just to be safe.
  std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

  auto channel = grpc::CreateChannel( "localhost:50051", grpc::InsecureChannelCredentials() );
  std::unique_ptr<KeyValueStore::Kvs::Stub> stub = KeyValueStore::Kvs::NewStub( channel );
  grpc::ClientContext context;

  google::protobuf::Empty empty;

  KeyValueStore::KvsKey request;
  KeyValueStore::KvsValue response;
  request.set_key( "1" );
  grpc::Status status = stub->Get( &context, request, &response );
  ASSERT_FALSE( status.ok() );
}