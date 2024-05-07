cmake_minimum_required(VERSION 3.10)

set(This witnesskvTest)

set(Sources
    sanity.cc
)

enable_testing()

add_executable(${This} ${Sources})

target_link_libraries(${This} PUBLIC
    gtest_main
    kvsproto
)

add_test(
    NAME ${This}
    COMMAND ${This}
)