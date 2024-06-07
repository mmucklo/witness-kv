#include <grpcpp/grpcpp.h>

#include "kvs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using KeyValueStore::DeleteRequest;
using KeyValueStore::DeleteResponse;
using KeyValueStore::GetRequest;
using KeyValueStore::GetResponse;
using KeyValueStore::Kvs;
using KeyValueStore::PutRequest;
using KeyValueStore::PutResponse;

class KvsClient {
 private:
  std::unique_ptr<Kvs::Stub> stub_;

 public:
  KvsClient(std::shared_ptr<Channel> channel);
  ~KvsClient() = default;

  Status PutGrpc(const std::string& key, const std::string& value,
                 PutResponse* response) const;

  Status GetGrpc(const std::string& key, GetResponse* response) const;

  Status DeleteGrpc(const std::string& key, DeleteResponse* response) const;
  Status LinearizabilityCheckerInitGrpc() const;
  Status LinearizabilityCheckerDeInitGrpc() const;
};