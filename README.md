# witness-kv

Prerequisites
- Install CMake
- Need cmake 3.29
-  
- Install grpc and protobuf: https://grpc.io/docs/languages/cpp/quickstart/

Build Instructions
- In the top level directory (witness-kv):
    cmake -S . -B build
    cd build; make -j`nproc`
- To format the code
    cmake --build build --target format

Unit tests and LOG(INFO):
You may need to specify "-- --minloglevel=0" when running unit tests to get logging messages to show:
  e.g.
        ./log_writer_test -- --minloglevel=0

"--" tells gtest that the rest of the arguments should get passed to the tests.
