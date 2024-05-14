cmake_minimum_required(VERSION 3.29)

add_library(test_main test_main.cc)
target_link_libraries(test_main PUBLIC 
    gtest
    absl::log
    absl::flags
    # See https://github.com/abseil/abseil-cpp/blob/c14dfbf9/absl/log/CMakeLists.txt#L464-L467
    $<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags>
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
    gmock_main
    kvsproto
    paxos
)

add_test(
    NAME ${This}
    COMMAND ${This}
)

set(This ProposerTest)

set(Sources
    test_proposer.cc
)

include_directories(${PROJECT_SOURCE_DIR}/paxos)

enable_testing()

add_executable(${This} ${Sources})

target_link_libraries(${This} PUBLIC
    gtest_main
    gmock_main
    kvsproto
    paxos
)

add_test(
    NAME ${This}
    COMMAND ${This}
)
