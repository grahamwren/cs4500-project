FROM alpine:3.11.5

# use unstable edge repo packages
RUN sed -i -e 's/v[[:digit:]]\..*\//edge\//g' /etc/apk/repositories

RUN apk upgrade --update-cache --available
RUN apk add bash make clang g++ tcpdump valgrind
