# witness-kv

Prerequisites
- Install CMake
  - Need cmake 3.29
- Install grpc and protobuf: https://grpc.io/docs/languages/cpp/quickstart/
- sudo apt install -y libgflags-dev liburing-dev libzstd-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libbz2-dev

Build Instructions
- In the top level directory (witness-kv):
    cmake -S . -B build
    cd build; make -j `nproc`
- To format the code
    cmake --build build --target format

Unit tests and LOG(INFO):
You may need to specify "-- --stderrthreshold=0" when running unit tests to get logging messages to show:
  e.g.
        ./log_writer_test -- --stderrthreshold=0

"--" tells gtest that the rest of the arguments should get passed to the tests.
