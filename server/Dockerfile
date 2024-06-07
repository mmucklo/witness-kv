FROM ubuntu:jammy
RUN apt update
RUN apt install libgflags2.2 libatomic1
COPY kvs_server /kvs_server
ENTRYPOINT ["/kvs_server"]
