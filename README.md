# witness-kv

Prerequisites
- Building works on Ubuntu 22.04 LTS with g++ 11 or clang++ 15 (clang++ 14 may work as well)
- Install CMake
- Need cmake 3.29
- Install grpc and protobuf: https://grpc.io/docs/languages/cpp/quickstart/
- sudo apt install -y libgflags-dev liburing-dev libzstd-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libbz2-dev

## Build Instructions
- In the top level directory (witness-kv):
  - cmake -S . -B build
  - cd build; make -j `nproc`
- To format the code
  - cmake --build build --target format

# Unit tests

## LOG(INFO):
You may need to specify "-- --stderrthreshold=0" when running unit tests to get logging messages to show:
  e.g.
        ./log_writer_test -- --stderrthreshold=0

"--" tells gtest that the rest of the arguments should get passed to the tests.

To make VLOG show, add --v=2 (or =1 or whatever level you want to see).

## NOTE: on log/ unit tests
Unit tests for logging currently write to /tmp - however tmpfs typically ignores or doesn't pay attention to fsync.
There is a possibility of some flakiness as a result.

TODO(mmucklo): make test temp directory flag configurable.

# For RocksDB, may need the following libs
sudo apt install -y libgflags-dev liburing-dev libzstd-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libbz2-dev

# GRPC (one way to install it)
git clone --recurse-submodules -b v1.64.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build; pushd cmake/build
cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=$HOME/.local ../..
cmake --build . -j `nproc`
cmake --install .

# Latest cmake build (one way to install it)
1. Download cmake-3.29.3.tar.gz from cmake.org
2. tar xzf cmake-3.29.3.tar.gz
3. mkdir $HOME/.local
4. cd cmake-3.29.3
5. sudo apt install libssl-dev
6. mkdir build
7. cd build
8. cmake .. -DCMAKE_INSTALL_PREFIX=$HOME/.local
9. cmake --build . -j `nproc`
10. cmake --install .


# External citations
@misc{athalye2017porcupine,
  author = {Anish Athalye},
  title = {Porcupine: A fast linearizability checker in {Go}},
  year = {2017},
  howpublished = {\url{https://github.com/anishathalye/porcupine}}
}