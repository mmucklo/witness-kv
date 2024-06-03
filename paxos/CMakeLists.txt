find_package(Threads)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

include_directories(${PROJECT_SOURCE_DIR})

set(SOURCES
    acceptor.cc
    paxos.cc
    proposer.cc
    replicated_log.cc
    node.cc
    utils.cc
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_subdirectory("proto")

add_library(paxos STATIC ${SOURCES})
target_link_libraries(paxos
    PUBLIC
	paxosproto
    absl::base
    absl::core_headers
    absl::flags
    absl::log
    absl::synchronization
    absl::strings
    log_writer_lib
    logs_loader_lib
    logs_truncator_lib
)
