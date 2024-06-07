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
1. git clone --recurse-submodules -b v1.64.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
2. cd grpc
3. mkdir -p cmake/build; pushd cmake/build
4. cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=$HOME/.local ../..
5. cmake --build . -j `nproc`
6. cmake --install .

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

# YCSB
To run YCSB against this:

1. First BUILD this repo with mvn: cd /path/to/this/witness-kv; mvn clean package
2. Clone our fork of YCSB: https://github.com/mmucklo/YCSB
3. In YCSB, git checkout witness-kv
4. BUILD like this: mvn -Dwitnesskvpath=/path/to/this/witness-kv -pl site.ycsb:witnesskvs-binding -am clean package
5. Set ENV variable first: export witnesskvpath=/path/to/this/witness-kv
6. RUN load: ./bin/ycsb load witnesskvs -P witnesskvs/conf/witnesskvs.properties -P workloads/workloada -p recordcount=100 -threads 10 -s
7. RUN test: ./bin/ycsb run witnesskvs -P witnesskvs/conf/witnesskvs.properties -P workloads/workloadb -p recordcount=100 -p operationcount=1000 -threads 10 -s

- OPTIONALLY see witnesskvs/README.md for more details
- OPTIONALLY change node config in witnesskvs/conf/witnesskvs.properties

# Porcupine
We use porcupine linearizability (cited below) checker for the correctness of the witness-kv. 
To run porcupine checker against this:

1. Install `go` and make sure `go` is added to your PATH
2. Run the witness-kv servers
3. Run the client binary (build/client/client) with the `--linearizability=true` option. Which will enable and disable linearizability logging on the server.
4. After the test completes, a new json file(default name = `history.json`) with the operation history will be stored in your current working drectory.
5. Run the linearizability checker in `witness-kv/kv-linearizability-check` by running `go test`. An example output of a successful run is pasted below:
   PASS
   ok      github.com/vramesh7/kv-linearizability-check/kv-linearizability-check   0.010s
6. External Citation:
   @misc{athalye2017porcupine,
     author = {Anish Athalye},
     title = {Porcupine: A fast linearizability checker in {Go}},
     year = {2017},
     howpublished = {\url{https://github.com/anishathalye/porcupine}}
   }