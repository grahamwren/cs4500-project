CUSTOM_FILTER=*

test_all: build/test.exe
	cd build && ./test.exe --gtest_filter="*$(CUSTOM_FILTER)*-*LONG*"

long_tests: build/test.exe
	cd build && ./test.exe --gtest_filter="*$(CUSTOM_FILTER)*LONG*:*LONG*$(CUSTOM_FILTER)*"

test_list: build/test.exe
	cd build && ./test.exe --gtest_filter=":TestList.$(CUSTOM_FILTER)"

test_block_list: build/test.exe
	cd build && ./test.exe --gtest_filter="TestBlockList.$(CUSTOM_FILTER)"

test_queue: build/test.exe
	cd build && ./test.exe --gtest_filter="TestQueue.$(CUSTOM_FILTER)"

build/test.exe: rebuild_cmake
	cd build && make

rebuild_cmake: CMakeLists.txt
	cd build && cmake ..

rerun: rebuild_cmake
	cd build && rerun -d ../.. -p "**/*.{cpp,hpp,c,h}" "make && ./test.exe"
