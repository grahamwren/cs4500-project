CC=g++
CCOPTS=-O0 -g --std=c++17 -Wno-varargs -Wno-return-type
CPATH=generated_code:src

SHARED_HEADER_FILES=generated_code/* src/*
BUILD_DIR=build

run: build
	./example.exe datafile.sor

$(BUILD_DIR)/example.exe: src/example.cpp $(SHARED_HEADER_FILES)
	CPATH=$(CPATH) $(CC) $(CCOPTS) $< -o $@

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
