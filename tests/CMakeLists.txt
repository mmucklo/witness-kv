cmake_minimum_required(VERSION 3.29)
find_package(protobuf CONFIG REQUIRED)

include_directories(${PROJECT_SOURCE_DIR})

add_library(test_main test_main.cc)
add_library(test_util_lib test_util.cc)

add_executable(rocksdb_test rocksdb_test.cc)

target_link_libraries(test_main PUBLIC 
    gtest
    absl::log
    absl::flags
    # See https://github.com/abseil/abseil-cpp/blob/c14dfbf9/absl/log/CMakeLists.txt#L464-L467
    $<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags>
)

target_link_libraries(test_util_lib PUBLIC
    absl::flags
    # See https://github.com/abseil/abseil-cpp/blob/c14dfbf9/absl/log/CMakeLists.txt#L464-L467
    $<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags>
    absl::status
    absl::strings
    absl::time
)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/paxos)

enable_testing()

add_executable(paxos_test paxos_test.cc)

target_link_libraries(paxos_test PUBLIC
    gtest
    gmock
    test_main
    kvsproto
    paxos
    absl::flags
    test_util_lib
)

add_test(
    NAME paxos_test
    COMMAND paxos_test
)

target_link_libraries(rocksdb_test PUBLIC 
    test_main
    gmock
    test_util_lib
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    absl::status
    gtest
    protobuf::libprotobuf
    rocksdb
)

include(GoogleTest)

gtest_discover_tests(paxos_test rocksdb_test)
