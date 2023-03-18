FROM ubuntu:20.04
LABEL author="hsj"
# skip interaction
ARG DEBIAN_FRONTEND=noninteractive
# install dependency
RUN apt update -y &&\
    apt install -y curl wget gcc g++ gdb git cmake pkg-config autoconf libtool
WORKDIR /root
# 3rd lib
RUN git clone https://github.com/xsjlmzs/Rep.git --branch dev &&\
    cd Rep &&\
    sh install-ext
