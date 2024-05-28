#include <grpcpp/grpcpp.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "absl/log/log.h"
#include "kvs.grpc.pb.h"
#include "rocksdb/db.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using KeyValueStore::DeleteRequest;
using KeyValueStore::DeleteResponse;
using KeyValueStore::GetRequest;
using KeyValueStore::GetResponse;
using KeyValueStore::Kvs;
using KeyValueStore::PutRequest;
using KeyValueStore::PutResponse;

std::map<std::string, std::string> globalMap {};

class KvsServiceImpl final : public Kvs::Service
{
 public:
  KvsServiceImpl( const std::string& db_path, bool isLeader )
  {
    isLeader_ = isLeader;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open( options, db_path, &db_ );
    if ( !status.ok() ) {
      throw std::runtime_error( "Failed to open RocksDB: "
                                + status.ToString() );
    }
  }

  ~KvsServiceImpl() { delete db_; }

  Status Get( ServerContext* context, const GetRequest* request,
              GetResponse* response ) override
  {
    if ( !IsLeader() ) {
      std::string leader = GetLeader();
      LOG( INFO ) << "Not a leader, forwarding leader info to client: "
                  << leader << "\n";
      return grpc::Status( grpc::StatusCode::NOT_FOUND, leader );
    }
    std::string value;
    rocksdb::Status status
        = db_->Get( rocksdb::ReadOptions(), request->key(), &value );
    if ( status.ok() ) {
      response->set_value( value );
      return Status::OK;
    } else {
      return Status::CANCELLED;
    }
  }

  Status Put( ServerContext* context, const PutRequest* request,
              PutResponse* response ) override
  {
    if ( !IsLeader() ) {
      std::string leader = GetLeader();
      LOG( INFO ) << "Not a leader, forwarding leader info to client: "
                  << leader << "\n";
      return grpc::Status( grpc::StatusCode::NOT_FOUND, leader );
    }
    rocksdb::Status status
        = db_->Put( rocksdb::WriteOptions(), request->key(), request->value() );
    if ( status.ok() ) {
      response->set_status( "OK" );
      return Status::OK;
    } else {
      return Status::CANCELLED;
    }
  }

  Status Delete( ServerContext* context, const DeleteRequest* request,
                 DeleteResponse* response ) override
  {
    if ( !IsLeader() ) {
      std::string leader = GetLeader();
      LOG( INFO ) << "Not a leader, forwarding leader info to client: "
                  << leader << "\n";
      return grpc::Status( grpc::StatusCode::NOT_FOUND, leader );
    }
    rocksdb::Status status
        = db_->Delete( rocksdb::WriteOptions(), request->key() );
    if ( status.ok() ) {
      response->set_status( "OK" );
      return Status::OK;
    } else {
      return Status::CANCELLED;
    }
  }

  bool IsLeader() { return isLeader_; }
  std::string GetLeader() { return "localhost:50062"; }

 private:
  rocksdb::DB* db_;
  bool isLeader_;
};

void RunServer( uint16_t port, const std::string& db_path,
                bool isLeader = false )
{
  std::string port_str = std::to_string( port );
  std::string ip_address = "0.0.0.0";
  std::string server_address = ip_address + ":" + port_str;
  KvsServiceImpl service( db_path, isLeader );

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort( server_address, grpc::InsecureServerCredentials() );
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService( &service );
  // Finally assemble the server.
  std::unique_ptr<Server> server( builder.BuildAndStart() );

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  std::cout << "Server actually exited!\n";
}

int main( int argc, char** argv )
{
  std::string db_path = "/tmp/rocksdb";
  uint16_t server_port = 50061;
  bool isLeader
      = false;  // Just a WAR until we get Paxos integrated with Server

  if ( argc > 2 ) {
    server_port = std::stoi( argv[1] );
    isLeader = std::stoi( argv[2] );
    db_path = "/tmp/rocksdb" + std::to_string( server_port );
  }

  RunServer( server_port, db_path, isLeader );
}