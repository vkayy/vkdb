file(GLOB_RECURSE vkdb_benchmarks_SRC
  "*.cpp"
)

add_executable(vkdb_benchmarks
  ${vkdb_benchmarks_SRC}
)

target_link_libraries(vkdb_benchmarks PRIVATE vkdb)