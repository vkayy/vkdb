file(GLOB_RECURSE vkdb_examples_SRC
    "*.cpp"
)

foreach(example_src ${vkdb_examples_SRC})
  get_filename_component(example_name ${example_src} NAME_WE)
  add_executable(${example_name} ${example_src})
  target_link_libraries(${example_name} PRIVATE vkdb)
endforeach()
