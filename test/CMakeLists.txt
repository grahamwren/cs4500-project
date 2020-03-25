cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_COMPILER /usr/bin/clang++)

find_package(Threads REQUIRED)

configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download
    )

if (result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download
    )

if (result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL
                 )

include_directories(../src)

add_library(parser.o ../src/parser.cpp)
target_compile_options(parser.o PRIVATE -g -O0 -Wall -Wvarargs)

add_executable(test.exe test.cpp)
target_link_libraries(test.exe gtest parser.o)
target_compile_options(test.exe PRIVATE -g -O0 -Wall -Wvarargs)
