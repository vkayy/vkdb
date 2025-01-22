cmake_minimum_required(VERSION 3.25)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

project(vkdb CXX)

option(VKDB_BUILD_TESTS "Build vkdb tests" OFF)
option(VKDB_BUILD_EXAMPLES "Build vkdb examples" OFF)
option(VKDB_BUILD_BENCHMARKS "Build vkdb benchmarks" OFF)

if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(VKDB_BUILD_TESTS ON)
  set(VKDB_BUILD_EXAMPLES ON)
  set(VKDB_BUILD_BENCHMARKS ON)
endif()

add_subdirectory(src)

if(VKDB_BUILD_TESTS)
  add_subdirectory(test)
  enable_testing()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        main
  )
  FetchContent_MakeAvailable(googletest)
  target_link_libraries(vkdb_tests PRIVATE gtest gtest_main)
endif()

if(VKDB_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(VKDB_BUILD_BENCHMARKS)
  add_subdirectory(benchmark)
  include(FetchContent)
  FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.9.1
  )
  FetchContent_MakeAvailable(benchmark)
  target_link_libraries(vkdb_benchmarks PRIVATE benchmark::benchmark)
endif()

    