file(GLOB_RECURSE vkdb_benchmark_SRC
  "*.cpp"
)

add_executable(vkdb_benchmark
  ${vkdb_benchmark_SRC}
)

target_link_libraries(vkdb_benchmark PRIVATE vkdb)