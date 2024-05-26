add_library(file_writer_lib file_writer.cc)
add_library(log_writer_lib log_writer.cc)
add_library(log_reader_lib log_reader.cc)
add_library(logs_loader_lib logs_loader.cc)

find_package(re2 REQUIRED)

include_directories(${PROJECT_SOURCE_DIR})

target_include_directories(file_writer_lib
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(log_writer_lib
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(log_reader_lib
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(logs_loader_lib
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(log_reader log_reader_main.cc)
add_executable(log_faker log_faker.cc)

add_executable(file_writer_test file_writer_test.cc)
add_executable(log_writer_test log_writer_test.cc)
add_executable(log_reader_test log_reader_test.cc)
add_executable(logs_loader_test logs_loader_test.cc)

target_include_directories(log_reader
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(log_faker
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(file_writer_lib PUBLIC
    absl::base
    absl::core_headers
    absl::flags
    absl::log
)

target_link_libraries(log_writer_lib PUBLIC
    file_writer_lib
    logproto
    absl::base
    absl::core_headers
    absl::crc32c
    absl::flags
    absl::log
    absl::synchronization
    re2::re2
)

target_link_libraries(log_reader_lib PUBLIC
    log_writer_lib
    logproto
    absl::base
    absl::core_headers
    absl::crc32c
    absl::flags
    absl::log
    absl::synchronization
    absl::strings
    status_macros
)

target_link_libraries(logs_loader_lib PUBLIC
    log_reader_lib
    log_writer_lib
    logproto
    absl::base
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    re2::re2
    status_macros
)

target_link_libraries(log_reader PRIVATE
    log_reader_lib
    logproto
    absl::flags_parse
    absl::log
    absl::strings
    absl::time
)

target_link_libraries(log_faker PRIVATE
    log_writer_lib
    logproto
    absl::flags_parse
    absl::log
    absl::status
    absl::strings
    absl::time
)

target_link_libraries(file_writer_test PUBLIC
    test_main
    file_writer_lib
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
    gmock
    log_writer_lib
    test_util_lib
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    absl::status
    gtest
    kvsproto
)

target_link_libraries(log_reader_test PUBLIC
    test_main
    gmock
    log_writer_lib
    log_reader_lib
    test_util_lib
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    absl::status
    gtest
    protobuf_matchers
    kvsproto
)

target_link_libraries(logs_loader_test PUBLIC
    test_main
    gmock
    log_writer_lib
    logs_loader_lib
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    absl::status
    gtest
    protobuf_matchers
    kvsproto
)

include(GoogleTest)
gtest_discover_tests(file_writer_test log_writer_test log_reader_test logs_loader_test)
