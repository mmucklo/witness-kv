find_package(Threads)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(SOURCES
    acceptor.cc
    paxos.cc
    proposer.cc
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_subdirectory("proto")

add_library(paxos STATIC ${SOURCES})
target_link_libraries(paxos
    PUBLIC
	paxosproto
)
