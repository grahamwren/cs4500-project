DEBUG=true
CC=g++

ifeq ($(DEBUG),true)
	CCOPTS=-O0 -g --std=c++17 -Wno-varargs -Wno-sign-compare -Wall
else
	CCOPTS=-Ofast --std=c++17 -Wno-varargs -Wno-sign-compare 
endif

CPATH=src

SHARED_HEADER_FILES=src/*
SRC_DIR=src
BUILD_DIR=build

run: build
	$(BUILD_DIR)/example.exe datafile.sor

build: $(BUILD_DIR)/example.exe

valgrind: docker_valgrind

$(BUILD_DIR)/example.exe: $(SRC_DIR)/example.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	$(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/bench.exe: $(SRC_DIR)/bench.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	$(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/parser.o: $(SRC_DIR)/parser.cpp $(SHARED_HEADER_FILES)
	$(CC) $(CCOPTS) -c $< -o $@



bench: $(BUILD_DIR)/bench.exe
	./$(BUILD_DIR)/bench.exe bench_files/big_file_0.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_1.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_2.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_3.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_4.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_5.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_6.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_7.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_8.sor
	./$(BUILD_DIR)/bench.exe bench_files/big_file_9.sor

native_valgrind: $(BUILD_DIR)/example.exe
	valgrind --leak-check=yes $(BUILD_DIR)/example.exe datafile.sor

clean:
	rm -rf build/[!.]*

# docker
CONT_NAME=gwjq-eau2:0.1
define docker_run
	docker run -ti -v `pwd`:/eau2 $(CONT_NAME) bash -c 'cd /eau2; $(1)'
endef

docker_valgrind: docker_install
	$(call docker_run, make clean build)
	$(call docker_run, make native_valgrind)

docker_install: Dockerfile
	docker build -t $(CONT_NAME) .
