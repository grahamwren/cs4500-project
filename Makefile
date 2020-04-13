DEBUG=false
CC=clang++

ifeq ($(DEBUG),true)
	KV_LOG ?= true
	NODE_LOG ?= true
	SOCK_LOG ?= true
	CLUSTER_LOG ?= true
	CCOPTS=-O0 -g -Wno-varargs -Wno-sign-compare -Wall
else
	KV_LOG ?= false
	NODE_LOG ?= false
	SOCK_LOG ?= false
	CLUSTER_LOG ?= false
	CCOPTS=-Ofast -Wno-varargs -Wno-sign-compare
endif

CCOPTS += --std=c++17 -pthread -DKV_LOG=$(KV_LOG) -DNODE_LOG=$(NODE_LOG) -DSOCK_LOG=$(SOCK_LOG) -DCLUSTER_LOG=$(CLUSTER_LOG)

CPATH=src

SHARED_HEADER_FILES=src/**/*
SRC_DIR=src
BUILD_DIR=build

# run linus app on easy data
run: APP=linus_compute
run: build load_easy_data run_app

# start the cluster and load easy linus data in
build: FORCE launch_cluster load_easy_data

# run valgrind of WordCountDemo in docker
valgrind: DEBUG=true
valgrind: CCOPTS += -DDF_CHUNK_SIZE=4096
valgrind: docker_net_valgrind

# run tests in test directory
test: FORCE
	cd test && make

# task for benchmarking the parser
bench: DEBUG=false
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

# DOCKER COMMANDS

CONT_PREFIX=gwjq

define docker_run
	docker run -ti -v `pwd`:/eau2 $(CONT_PREFIX)-eau2:0.1 bash -c 'cd /eau2; $(1)'
endef

# run in network with randomly assigned IP, don't detach
define docker_net_run
	docker run -ti --cap-add sys_admin --network clients-project -v `pwd`:/eau2 $(CONT_PREFIX)-eau2:0.1 bash -c 'cd /eau2; $(1)'
endef

# starts container in the network with the given IP and then detach
define docker_net_start
	docker run -tid --network clients-project --ip $(2) -v `pwd`:/eau2 $(CONT_PREFIX)-eau2:0.1 bash -c 'cd /eau2; $(1)'
endef

docker_net_valgrind: DEBUG=true
docker_net_valgrind: docker_install launch_one_node
	$(call docker_run, make clean $(BUILD_DIR)/word_count_demo.exe DEBUG="$(DEBUG)")
	$(call docker_net_run, valgrind --leak-check=yes $(BUILD_DIR)/word_count_demo.exe --ip 172.168.0.2)

docker_install: docker_clean Dockerfile
	docker build -t $(CONT_PREFIX)-eau2:0.1 .
	- docker network create --subnet 172.168.0.0/16 clients-project 2>/dev/null

docker_clean: FORCE
	docker ps | grep "$(CONT_PREFIX)" | awk '{ print $$1 }' | xargs docker kill >/dev/null || echo "Nothing to delete."
	docker ps -a | grep "$(CONT_PREFIX)" | awk '{ print $$1 }' | xargs docker rm >/dev/null

launch_one_node: docker_clean
	docker build --build-arg debug=$(DEBUG) -f Dockerfile.kv -t kv_node:0.1 .
	- docker network create --subnet 172.168.0.0/16 clients-project 2>/dev/null
	docker run -d --network clients-project --ip 172.168.0.2 kv_node:0.1 --ip 172.168.0.2

launch_cluster: docker_clean
	docker build --build-arg debug=$(DEBUG) -f Dockerfile.kv -t $(CONT_PREFIX)-kv_node:0.1 .
	- docker network create --subnet 172.168.0.0/16 clients-project 2>/dev/null
	docker run -d --network clients-project --ip 172.168.0.2 $(CONT_PREFIX)-kv_node:0.1 --ip 172.168.0.2
	docker run -d --network clients-project --ip 172.168.0.3 $(CONT_PREFIX)-kv_node:0.1 --ip 172.168.0.3 --server-ip 172.168.0.2
	docker run -d --network clients-project --ip 172.168.0.4 $(CONT_PREFIX)-kv_node:0.1 --ip 172.168.0.4 --server-ip 172.168.0.2
	docker run -d --network clients-project --ip 172.168.0.5 $(CONT_PREFIX)-kv_node:0.1 --ip 172.168.0.5 --server-ip 172.168.0.2

build_cont:
	docker build --build-arg debug=$(DEBUG) -f Dockerfile.$(APP) -t $(CONT_PREFIX)-$(APP):0.1 .

run_app: build_cont
	docker run --network clients-project $(CONT_PREFIX)-$(APP):0.1 --ip 172.168.0.2

load_easy_data: FORCE
	make build_cont APP=load_file
	make build_cont APP=dump_cluster_state
	docker run --network clients-project -v `pwd`/datasets:/eau2/datasets $(CONT_PREFIX)-load_file:0.1 --ip 172.168.0.2 --key commits --file easy-data/commits.ltgt
	docker run --network clients-project -v `pwd`/datasets:/eau2/datasets $(CONT_PREFIX)-load_file:0.1 --ip 172.168.0.2 --key users --file easy-data/users.ltgt
	docker run --network clients-project -v `pwd`/datasets:/eau2/datasets $(CONT_PREFIX)-load_file:0.1 --ip 172.168.0.2 --key projects --file easy-data/projects.ltgt
	docker run --network clients-project $(CONT_PREFIX)-dump_cluster_state:0.1 --ip 172.168.0.2

# COMPILE TARGETS

$(BUILD_DIR)/kv_node.exe: $(SRC_DIR)/kv_node.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@

$(BUILD_DIR)/load_file.exe: $(SRC_DIR)/examples/load_file.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/linus_compute.exe: $(SRC_DIR)/examples/linus_compute.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/word_count_demo.exe: $(SRC_DIR)/examples/word_count_demo.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/dump_cluster_state.exe: $(SRC_DIR)/utils/dump_cluster_state.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/demo.exe: $(SRC_DIR)/demo.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/bench.exe: $(SRC_DIR)/examples/bench.cpp $(BUILD_DIR)/parser.o $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/df_builder.exe: $(SRC_DIR)/utils/df_builder.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@ $(BUILD_DIR)/parser.o

$(BUILD_DIR)/parser.o: $(SRC_DIR)/parser.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) -c $< -o $@

# UTILS

FORCE:
