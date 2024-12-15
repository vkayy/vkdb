# vkdb

a time series database engine built in c++ with minimal dependencies.

## why?

wanted to challenge myself architecturally and push my boundaries with c++ in terms of both knowledge and performance.

## how do i use it?

using cmake will make your life easier. just add this to your `CMakeLists.txt` file:

```cmake
include(FetchContent)
FetchContent_Declare(
    vkdb
    GIT_REPOSITORY https://github.com/vkayy/vkdb.git
    GIT_TAG        main
)
FetchContent_MakeAvailable(vkdb)
target_link_libraries(${PROJECT_NAME} vkdb)
```

then, simply include the database header, and you'll have access to the interface.

```cpp
#include <vkdb/database.h>

int main()  {
  vkdb::Database db{"example-db"};
  db.createTable("example-table");
  // ...
}
```

## how does it work?

vkdb is built on log-structured merge (lsm) trees. in their simplest form, these have an in-memory layer and a disk layer, paired with a write-ahead log (wal) for persistence of in-memory changes.

when you create your database (by instantiating a `vkdb::Database`), it persists on disk until you clear it via `vkdb::Database::clear`. it's best to make all interactions via this type, or perhaps the `vkdb::Table` type via `vkdb::Database::getTable`.

in terms of typing, i've tried to make vkdb as robust as possible (as you can see with some of the verbose concepts), but there are bound to be some flaws here and there. bring them up!

## authors

[vinz kakilala](https://linkedin.com/in/vinzkakilala) (me).

## credits

used [murmurhash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) for the bloom filters. fast, uniform, deterministic.
