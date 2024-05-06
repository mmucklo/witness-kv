
add_library(file_writer file_writer.cc)

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

target_link_libraries(file_writer PUBLIC
absl::base
absl::core_headers
absl::log
)

target_link_libraries(${FileWriterTest} PUBLIC
absl::core_headers
absl::log
absl::strings
    gtest_main
    kvsproto
)

add_test(
    NAME ${FileWriterTest}
    COMMAND ${FileWriterTest}
)