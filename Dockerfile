FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN sed -i 's#http://archive.ubuntu.com/#http://mirrors.tuna.tsinghua.edu.cn/#' /etc/apt/sources.list

RUN apt-get -y update && \
    apt upgrade && \
    apt-get -y install \
      build-essential \
      clang-14 \
      clang-format-14 \
      clang-tidy-14 \
      clandd \
      cmake \
      doxygen \
      git \
      g++-12 \
      gdb \
      pkg-config \
      zlib1g-dev \
      vim

CMD bash
