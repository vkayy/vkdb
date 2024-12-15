file(GLOB_RECURSE vkdb_lib_SRC
    "*.cpp"
)

add_library(vkdb_lib
    ${vkdb_lib_SRC}
)

target_include_directories(vkdb_lib PUBLIC ${PROJECT_SOURCE_DIR}/include)
