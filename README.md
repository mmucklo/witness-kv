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


