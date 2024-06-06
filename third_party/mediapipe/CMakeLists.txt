add_library(status_macros mediapipe_status_builder.cc)
target_link_libraries(status_macros PUBLIC 
    absl::base
    absl::core_headers
    absl::log
    absl::status
    absl::memory
    absl::strings
)
