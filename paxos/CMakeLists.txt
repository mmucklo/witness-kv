find_package(Threads)

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(SOURCES
    acceptor.cc
    paxos.cc
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/proto")

add_library(paxos SHARED ${SOURCES})
target_link_libraries(paxos
    PRIVATE
	paxosproto
)
