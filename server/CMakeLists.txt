find_package(Threads)

set(SOURCES
    kvs_server.cc
)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/third_party/json/include)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_executable(kvs_server ${SOURCES})
target_link_libraries(kvs_server PRIVATE 
                                kvsproto
                                paxos
                                rocksdb
                                absl::flags
                                absl::flags_parse
                                absl::log)
