DEBUG=true
CC=clang++
FAST_CCOPTS=-Ofast --std=c++17 -Wno-varargs -Wno-sign-compare -DKV_LOG=false -DNODE_LOG=false -DSOCK_LOG=false -DCLUSTER_LOG=false -pthread
DEBUG_CCOPTS=-O0 -g --std=c++17 -Wno-varargs -Wno-sign-compare -Wall -DKV_LOG=true -DNODE_LOG=true -DSOCK_LOG=true -DCLUSTER_LOG=true -pthread

ifeq ($(DEBUG),true)
	CCOPTS=$(DEBUG_CCOPTS)
else
	CCOPTS=$(FAST_CCOPTS)
endif

CPATH=src

SHARED_HEADER_FILES=src/**/*
SRC_DIR=src
BUILD_DIR=build

run: run_network

build: $(BUILD_DIR)/demo.exe

valgrind: CCOPTS=$(DEBUG_CCOPTS) -DNUM_ELEMENTS=10000 -DDF_CHUNK_SIZE=4096
valgrind: docker_net_valgrind

test: FORCE
	cd test && make

$(BUILD_DIR)/example.exe: $(SRC_DIR)/example.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/node_example.exe: $(SRC_DIR)/node_example.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@

$(BUILD_DIR)/kv_node.exe: $(SRC_DIR)/kv_node.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@

$(BUILD_DIR)/demo.exe: $(SRC_DIR)/demo.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@

$(BUILD_DIR)/bench.exe: $(SRC_DIR)/bench.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/parser.o: $(SRC_DIR)/parser.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) -c $< -o $@


bench: CCOPTS=$(FAST_CCOPTS)
bench: clean $(BUILD_DIR)/bench.exe
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

clean:
	rm -rf build/[!.]*

# docker
CONT_NAME=gwjq-eau2:0.1
define docker_run
	docker run -ti -v `pwd`:/eau2 $(CONT_NAME) bash -c 'cd /eau2; $(1)'
endef

# run in network with randomly assigned IP, don't detach
define docker_net_run
	docker run -ti --cap-add sys_admin --network clients-project -v `pwd`:/eau2 $(CONT_NAME) bash -c 'cd /eau2; $(1)'
endef

# starts container in the network with the given IP and then detach
define docker_net_start
	docker run -tid --network clients-project --ip $(2) -v `pwd`:/eau2 $(CONT_NAME) bash -c 'cd /eau2; $(1)'
endef

docker_net_valgrind: DEBUG=true
docker_net_valgrind: docker_install
	$(call docker_run, make clean $(BUILD_DIR)/demo.exe $(BUILD_DIR)/kv_node.exe CCOPTS="$(CCOPTS)")
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.2, 172.168.0.2)
	$(call docker_net_run, valgrind --leak-check=yes $(BUILD_DIR)/demo.exe --ip 172.168.0.2)

run_network: CCOPTS=$(FAST_CCOPTS)
run_network: APP=demo
run_network: docker_install
	$(call docker_run, make clean $(BUILD_DIR)/$(APP).exe $(BUILD_DIR)/kv_node.exe CCOPTS="$(CCOPTS)")
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.2, 172.168.0.2)
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.3  --server-ip 172.168.0.2, 172.168.0.3)
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.4  --server-ip 172.168.0.2, 172.168.0.4)
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.5  --server-ip 172.168.0.2, 172.168.0.5)
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.6  --server-ip 172.168.0.2, 172.168.0.6)
	$(call docker_net_run, ./$(BUILD_DIR)/$(APP).exe --ip 172.168.0.2)

run_perf: CCOPTS=$(DEBUG_CCOPTS) -DKV_LOG=false -DNODE_LOG=false -DSOCK_LOG=false
run_perf: APP=demo
run_perf: docker_install
	$(call docker_run, make clean $(BUILD_DIR)/$(APP).exe $(BUILD_DIR)/kv_node.exe CCOPTS="$(CCOPTS)")
	$(call docker_net_start, ./$(BUILD_DIR)/kv_node.exe --ip 172.168.0.2, 172.168.0.2)
	$(call docker_net_run, perf record -F 99 -a -g -- ./$(BUILD_DIR)/$(APP).exe --ip 172.168.0.7 --server-ip 172.168.0.2, 172.168.0.7)
	$(call docker_run, perf script > out.stacks)
	cd ~/FlameGraph; ./stackcollapse-perf.pl out.stacks > out.folded
	cd ~/FlameGraph; ./flamegraph.pl out.folded > out.svg
	open ~/FlameGraph/out.svg -a Google\ Chrome.app

docker_install: docker_clean Dockerfile
	docker build -t $(CONT_NAME) .
	- docker network create --subnet 172.168.0.0/16 clients-project 2>/dev/null

docker_clean: FORCE
	docker ps | grep "$(CONT_NAME)" | awk '{ print $$1 }' | xargs docker kill >/dev/null
	docker ps -a | grep "$(CONT_NAME)" | awk '{ print $$1 }' | xargs docker rm >/dev/null

FORCE:
