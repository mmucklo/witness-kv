find_package(Threads)

set(SOURCES
    kvs_server.cc
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})

add_executable(kvs_server ${SOURCES})
target_link_libraries(kvs_server
    PRIVATE
        kvsproto
)
