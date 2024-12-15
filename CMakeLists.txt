cmake_minimum_required(VERSION 3.14)
project(vkdb CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(${PROJECT_SOURCE_DIR}/include)
file(GLOB_RECURSE SOURCES src/*.cpp)

add_library(vkdb STATIC)
target_include_directories(vkdb INTERFACE include)
target_sources(vkdb PRIVATE ${SOURCES})

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)

enable_testing()
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        main
)
FetchContent_MakeAvailable(googletest)
    
target_link_libraries(vkdb_tests PRIVATE gtest gtest_main)