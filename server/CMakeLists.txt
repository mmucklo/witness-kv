find_package(Threads)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(SOURCES
    kvs_server.cc
)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/third_party/json/include)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_executable(kvs_server ${SOURCES})
target_link_libraries(kvs_server PRIVATE
    gRPC::grpc
    gRPC::grpc++
    gRPC::grpcpp_channelz
    gRPC::grpc++_reflection
    kvsproto
    paxos
    rocksdb
    node_lib
    absl::flags
    absl::flags_parse
    absl::log)
