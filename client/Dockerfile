FROM ubuntu:jammy
RUN apt update
RUN apt install libgflags2.2 libatomic1
COPY client /client
ENTRYPOINT ["/client"]

