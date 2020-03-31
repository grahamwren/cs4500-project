FROM alpine:3.11.5

# apparently not available in edge repo?
RUN apk add man man-pages

# use unstable edge repo packages
RUN sed -i -e 's/v[[:digit:]]\..*\//edge\//g' /etc/apk/repositories

RUN apk upgrade --update-cache --available
RUN apk add bash make clang g++ tcpdump valgrind
RUN apk add linux-tools --update-cache --repository http://dl-3.alpinelinux.org/alpine/edge/testing/
