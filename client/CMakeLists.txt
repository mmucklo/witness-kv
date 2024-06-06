
find_package(Threads)

include_directories(${PROJECT_SOURCE_DIR})

set(SOURCES
    client.cc
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})
include_directories(${PROJECT_SOURCE_DIR}/paxos)

add_executable(client ${SOURCES})
target_link_libraries(client
    PRIVATE
    kvsproto
    paxos
    node_lib
    absl::flags
    absl::flags_parse
    absl::log
)