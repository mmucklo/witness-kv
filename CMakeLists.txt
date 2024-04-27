cmake_minimum_required(VERSION 3.8)
project(witness-kv LANGUAGES CXX)

include(etc/build_type.cmake)
include(etc/cflags.cmake)
include(etc/format.cmake)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CXX_STANDARD 20)

add_subdirectory("${PROJECT_SOURCE_DIR}/server")
add_subdirectory("${PROJECT_SOURCE_DIR}/client")
add_subdirectory("${PROJECT_SOURCE_DIR}/proto")
