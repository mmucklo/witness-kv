#include <grpcpp/grpcpp.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include "rocksdb/db.h"

#include "kvs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using KeyValueStore::Kvs;
using KeyValueStore::GetRequest;
using KeyValueStore::GetResponse;
using KeyValueStore::PutRequest;
using KeyValueStore::PutResponse;
using KeyValueStore::DeleteRequest;
using KeyValueStore::DeleteResponse;

std::map<std::string, std::string> globalMap {};

class KvsServiceImpl final : public Kvs::Service
{
  public:
   KvsServiceImpl( const std::string& db_path )
   {
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
     rocksdb::Status status = db_->Put( rocksdb::WriteOptions(), request->key(),
                                        request->value() );
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
     rocksdb::Status status
         = db_->Delete( rocksdb::WriteOptions(), request->key() );
     if ( status.ok() ) {
       response->set_status( "OK" );
       return Status::OK;
     } else {
       return Status::CANCELLED;
     }
   }

 private:
  rocksdb::DB* db_;
};

void RunServer( uint16_t port, const std::string& db_path)
{
  // FIXME: Hard-coded port for now.
  std::string server_address = "0.0.0.0:50061";
  KvsServiceImpl service( db_path );

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
  std::string dirname = "/tmp/rocksdb";
  RunServer( 50051, dirname );
  return 0;
}
