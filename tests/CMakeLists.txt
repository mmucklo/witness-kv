cmake_minimum_required(VERSION 3.10)

add_library(test_main test_main.cc)
target_link_libraries(test_main PUBLIC
    gtest
    absl::flags
)

set(This witnesskvTest)

set(Sources
    sanity.cc
    paxos_tests.cc
)

include_directories(${PROJECT_SOURCE_DIR}/paxos)

enable_testing()

add_executable(${This} ${Sources})

target_link_libraries(${This} PUBLIC
    gtest_main
    kvsproto
    paxos
)

add_test(
    NAME ${This}
    COMMAND ${This}
)