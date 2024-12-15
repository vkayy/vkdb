cmake_minimum_required(VERSION 3.14)
project(vkdb CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

option(VKDB_BUILD_TESTS "Build vkdb tests" OFF)
option(VKDB_BUILD_EXAMPLES "Build vkdb examples" OFF)

if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(VKDB_BUILD_TESTS ON)
    set(VKDB_BUILD_EXAMPLES ON)
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}/include/database
  ${CMAKE_SOURCE_DIR}/include/query
  ${CMAKE_SOURCE_DIR}/include/storage
  ${CMAKE_SOURCE_DIR}/include/utils
)
file(GLOB_RECURSE SOURCES src/*.cpp)

add_library(vkdb STATIC)
target_include_directories(vkdb INTERFACE include)
target_sources(vkdb PRIVATE ${SOURCES})

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
    