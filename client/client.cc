// Protobuf headers
/*#include "kvs.grpc.pb.h"
#include "kvs.pb.h"

// GRPC headers
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>

// std headers
#include <iostream>
#include <memory>

#include <sys/wait.h>
#include <unistd.h>*/

#include <cstdlib>
#include <iostream>
#include <memory>

#include "paxos.hh"

int main ( int argc, char* argv[] )
{
    if ( argc != 2 ) {
        std::cout << "Invalid argc\n";
        exit( EXIT_FAILURE );
    }

    Paxos p{"paxos/nodes_config.txt", std::atoi(argv[1])};
    p.Replicate("Hello");

    return 0;
}

/*
int main( int argc, char* argv[] )
{
  if ( argc < 2 || argc > 3 ) {
    std::cout << "Invalid argc\n";
    exit( EXIT_FAILURE );
  }

  auto channel = grpc::CreateChannel( "localhost:50051", grpc::InsecureChannelCredentials() );
  std::unique_ptr<KeyValueStore::Kvs::Stub> stub = KeyValueStore::Kvs::NewStub( channel );
  grpc::ClientContext context;

  google::protobuf::Empty empty;

  if ( argc == 2 ) {
    KeyValueStore::KvsKey request;
    KeyValueStore::KvsValue response;
    request.set_key( argv[1] );
    grpc::Status status = stub->Get( &context, request, &response );
    if ( !status.ok() ) {
      std::cout << "Not found in kvs!\n";
    } else {
      std::cout << "Got back " << response.value() << "for " << argv[1] << "\n";
    }
  } else if ( argc == 3 ) {
    KeyValueStore::KvsSetRequest request;
    request.set_key( argv[1] );
    request.set_value( argv[2] );
    grpc::Status status = stub->Set( &context, request, &empty );
  } else {
    exit( EXIT_FAILURE );
  }

  return 0;
}*/