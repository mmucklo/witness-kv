cmake_minimum_required(VERSION 3.29)
project(witness-kv LANGUAGES CXX)

include(etc/build_type.cmake)
include(etc/cflags.cmake)
include(etc/format.cmake)

enable_testing()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Debug)

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
set(GTEST_HAS_ABSL ON)
add_subdirectory("${PROJECT_SOURCE_DIR}/abseil-cpp")
add_subdirectory("${PROJECT_SOURCE_DIR}/server")
add_subdirectory("${PROJECT_SOURCE_DIR}/tests")
add_subdirectory("${PROJECT_SOURCE_DIR}/googletest")
add_subdirectory("${PROJECT_SOURCE_DIR}/proto")
add_subdirectory("${PROJECT_SOURCE_DIR}/paxos")
add_subdirectory("${PROJECT_SOURCE_DIR}/client")
add_subdirectory("${PROJECT_SOURCE_DIR}/log")
