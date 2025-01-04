file(GLOB_RECURSE vkdb_SRC
  "*.cpp"
)

add_library(vkdb
  ${vkdb_SRC}
)

target_include_directories(vkdb PUBLIC
  ${PROJECT_SOURCE_DIR}/include/database
  ${PROJECT_SOURCE_DIR}/include/query
  ${PROJECT_SOURCE_DIR}/include/storage
  ${PROJECT_SOURCE_DIR}/include/utils
)
