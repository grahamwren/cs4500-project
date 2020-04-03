CUSTOM_FILTER=*

test_all: build/test.exe
	cd build && ./test.exe --gtest_filter="*$(CUSTOM_FILTER)*"

build/test.exe: rebuild_cmake
	cd build && make

rebuild_cmake: CMakeLists.txt
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug ..

rerun: rebuild_cmake
	cd build && rerun -d ../.. -p "**/*.{cpp,hpp,c,h}" "make && ./test.exe  --gtest_filter='*$(CUSTOM_FILTER)*'"
