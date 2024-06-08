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
    rocksdb_container_lib
    node_lib
    absl::flags
    absl::flags_parse
    absl::log)
configure_file("Dockerfile" "Dockerfile" COPYONLY)

add_library(rocksdb_container_lib rocksdb_container.cc)
target_include_directories(rocksdb_container_lib
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(rocksdb_container_lib PUBLIC
    absl::log
    absl::strings
    rocksdb
)

add_executable(rocksdb_container_test rocksdb_container_test.cc)
target_link_libraries(rocksdb_container_test PUBLIC
    test_main
    gmock
    gtest
    rocksdb_container_lib
    test_util_lib
    absl::core_headers
    absl::flags
    absl::flat_hash_set
    absl::log
    absl::strings
    kvsproto
)
include(GoogleTest)

gtest_discover_tests(rocksdb_container_test)
