
add_library(file_writer file_writer.cc)
add_library(log_writer log_writer.cc)
add_library(log_writer log_writer.cc)

target_include_directories(log_writer
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(log_writer
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(file_writer
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
)

set(FileWriterTest fileWriterTest)

set(Sources
    file_writer.cc
    file_writer_test.cc
)

enable_testing()

add_executable(${FileWriterTest} ${Sources})

target_link_libraries(log_writer PUBLIC
    absl::base
    absl::core_headers
    absl::log
)

target_link_libraries(file_writer PUBLIC
    absl::base
    absl::core_headers
    absl::log
    absl::base
    absl::core_headers
    absl::log
)

target_link_libraries(log_writer PUBLIC
    file_writer
    logproto
    absl::base
    absl::core_headers
    absl::log
)

target_link_libraries(${FileWriterTest} PUBLIC
    test_main
    absl::core_headers
    absl::flags
    absl::log
    absl::strings
    gtest
    kvsproto
)

add_test(
    NAME ${FileWriterTest}
    COMMAND ${FileWriterTest}
)
