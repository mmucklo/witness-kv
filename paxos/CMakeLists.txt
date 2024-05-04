find_package(Threads)

set(SOURCES
    acceptor.cc
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/proto")

add_library(libpaxos SHARED ${SOURCES})
target_link_libraries(libpaxos
    PRIVATE
	paxosproto
)
