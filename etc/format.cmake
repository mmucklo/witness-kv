file (GLOB_RECURSE WITNESSKV_CC_FILES ${CMAKE_SOURCE_DIR}/*.cc)

file (GLOB_RECURSE ALL_SRC_FILES *h *.hh *.cc)

add_custom_target (format "clang-format" -i ${ALL_SRC_FILES} COMMENT "Formatting source code...")