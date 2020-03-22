FROM ubuntu:18.04
# Do not exclude man pages & other documentation
RUN rm /etc/dpkg/dpkg.cfg.d/excludes

RUN sed -i -e 's/:\/\/(archive.ubuntu.com\|security.ubuntu.com)/old-releases.ubuntu.com/g' /etc/apt/sources.list

RUN apt-get update --fix-missing
RUN apt-get upgrade -y
RUN apt-get install g++ valgrind cmake git clang man -y
RUN apt-get install g++-8 -y
