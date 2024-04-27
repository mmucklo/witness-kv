#include <iostream>
#include <map>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "kvs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using KeyValueStore::Kvs;
using KeyValueStore::KvsKey;
using KeyValueStore::KvsValue;
using KeyValueStore::KvsSetRequest;

std::map<std::string, std::string> globalMap {};

// Logic and data behind the server's behavior.
class KvsServiceImpl final : public Kvs::Service
{
  Status Get( ServerContext* context, const KvsKey* k, KvsValue* v ) override
  {
    auto it = globalMap.find( k->key() );
    if ( it == globalMap.end() ) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Key does not exist!");
    }

    v->set_value( it->second );
    return Status::OK;
  }

  Status Set( ServerContext* context, const KvsSetRequest* request, google::protobuf::Empty* response ) override
  {
    globalMap[request->key()] = request->value();
    return Status::OK;
  }

  Status Delete( ServerContext* context, const KvsKey* k, google::protobuf::Empty* response ) override
  {
    return Status::OK;
  }
};

void RunServer( uint16_t port )
{
  std::string server_address = "0.0.0.0:50051";
  KvsServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort( server_address, grpc::InsecureServerCredentials() );
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService( &service );
  // Finally assemble the server.
  std::unique_ptr<Server> server( builder.BuildAndStart() );
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main( int argc, char** argv )
{
  RunServer( 50051 );
  return 0;
}
