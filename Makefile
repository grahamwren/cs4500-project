DEBUG=true
CC=g++

ifeq ($(DEBUG),true)
	CCOPTS=-Ofast --std=c++17 -Wno-varargs
else
	CCOPTS==-O0 -g --std=c++17 -Wno-varargs -W@all
endif

CPATH=src

SHARED_HEADER_FILES=src/*
SRC_DIR=src
BUILD_DIR=build

run: build
	$(BUILD_DIR)/example.exe datafile.sor

$(BUILD_DIR)/example.exe: $(SRC_DIR)/example.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	$(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/parser.o: $(SRC_DIR)/parser.cpp $(SHARED_HEADER_FILES)
	$(CC) $(CCOPTS) -c $< -o $@


build: $(BUILD_DIR)/example.exe

native_valgrind: example.exe
	vagrind --leak-check=yes ./example.exe datafile.sor

clean:
	rm -rf build/[!.]*

# docker
CONT_NAME=gwjq-eau2:0.1
define docker_run
	docker run -ti -v `pwd`:/eau2 $(CONT_NAME) bash -c 'cd /eau2; $(1)'
endef

docker_vagrind:
	$(call docker_run, make clean build)
	$(call docker_run, make native_valgrind)
