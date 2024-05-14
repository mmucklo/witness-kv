add_library(file_writer file_writer.cc)
add_library(log_writer log_writer.cc)
add_library(log_reader log_reader.cc)
add_library(crc32 crc32.cc)

include_directories(${PROJECT_SOURCE_DIR}/tests)

target_include_directories(file_writer
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(log_writer
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(log_reader
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(crc32
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)

enable_testing()

add_executable(file_writer_test file_writer_test.cc)
add_executable(log_writer_test log_writer_test.cc)

target_link_libraries(file_writer PUBLIC
    absl::base
    absl::core_headers
    absl::flags
    absl::log
)

target_link_libraries(log_writer PUBLIC
    file_writer
    logproto
    absl::base
    absl::core_headers
    absl::flags
    absl::log
    absl::synchronization
    crc32
)

target_link_libraries(log_reader PUBLIC
    log_writer
    logproto
    absl::base
    absl::core_headers
    absl::flags
    absl::log
    absl::synchronization
    absl::strings
    crc32
)

target_link_libraries(file_writer_test PUBLIC
    test_main
    file_writer
    absl::core_headers
    absl::flags
    absl::log
    absl::status
    absl::strings
    gtest
    kvsproto
)

target_link_libraries(log_writer_test PUBLIC
    test_main
    gmock_main
    log_writer
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    absl::status
    gtest
    kvsproto
)

include(GoogleTest)
gtest_discover_tests(file_writer_test log_writer_test)