file(GLOB_RECURSE vkdb_tests_SRC
    "*.cpp"
)

add_executable(vkdb_tests
    ${vkdb_tests_SRC}
)

target_link_libraries(vkdb_tests PRIVATE vkdb)

add_test(NAME vkdb_tests COMMAND vkdb_tests)
