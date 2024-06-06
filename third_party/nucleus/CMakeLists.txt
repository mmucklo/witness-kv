cmake_minimum_required(VERSION 3.29)
find_package(protobuf CONFIG REQUIRED)

include_directories(${PROJECT_SOURCE_DIR})

add_library(protobuf_matchers protobuf_matchers.cc)

target_link_libraries(protobuf_matchers PUBLIC 
    protobuf::libprotobuf
    gtest
    gmock
    absl::log
    absl::flags
    # See https://github.com/abseil/abseil-cpp/blob/c14dfbf9/absl/log/CMakeLists.txt#L464-L467
    $<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags>
    absl::strings
)
