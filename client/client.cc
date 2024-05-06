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

int main( int argc, char* argv[] )
{
  using namespace std::chrono_literals;

  if ( argc != 2 ) {
    std::cout << "Invalid argc\n";
    exit( EXIT_FAILURE );
  }

  Paxos p { "paxos/nodes_config.txt", static_cast<uint32_t>(std::atoi( argv[1] )) };
  p.Replicate( "Hello" );

  return 0;
}