cmake_minimum_required(VERSION 3.29)

include_directories(${PROJECT_SOURCE_DIR})

add_library(node_lib node.cc)

target_link_libraries(node_lib PUBLIC 
    absl::base
    absl::synchronization
    absl::log
    absl::flags
    # See https://github.com/abseil/abseil-cpp/blob/c14dfbf9/absl/log/CMakeLists.txt#L464-L467
    $<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags>
    absl::strings
)
