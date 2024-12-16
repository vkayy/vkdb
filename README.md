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

if you want to play around with some mock timestamps/values, feel free to use `vkdb::random<>`. any arithmetic type (with no cv- or ref- qualifiers) can be passed in as a template argument, and you can optionally pass in a lower and upper bound (inclusive).

```cpp
auto random_int{vkdb::random<int>(-100'000, 100'000)};
auto random_double{vkdb::random<double>(-10.0, 10.0)};
```

lastly, you can execute queries both from strings and files. for instance, to yank a few examples from the `examples` directory:

```cpp
auto query_result{db.executeQuery(
  "SELECT COUNT temperature "
  "FROM atmospheric "
  "BETWEEN 1702550000 AND 1702650000 "
  "WHERE region=na"
)};

auto file_result{db.executeFile(
    std::filesystem::current_path() / "../examples/vq_setup.vq"
)};
```

## how does it work?

vkdb is built on log-structured merge (lsm) trees. in their simplest form, these have an in-memory layer and a disk layer, paired with a write-ahead log (wal) for persistence of in-memory changes.

when you create your database (by instantiating a `vkdb::Database`), it persists on disk until you clear it via `vkdb::Database::clear`. it's best to make all interactions via this type, or perhaps the `vkdb::Table` type via `vkdb::Database::getTable`.

in terms of typing, i've tried to make vkdb as robust as possible (as you can see with some of the verbose concepts), but there are bound to be some flaws here and there. bring them up!

## what's the query language?

i whipped something up called vq, and i have not refined it at all. here are some examples queries! i'm shamelessly using sql highlighting here to save you from plain, white text.

```sql
SELECT DATA temperature FROM sensors ALL;

SELECT AVG humidity FROM sensors BETWEEN 1000 AND 2000 WHERE location=warehouse type=sensor;

SELECT MIN pressure FROM sensors AT 1500 WHERE type=pressure location=external;

PUT temperature 1234567890 23.5 INTO sensors location=room1 type=celsius;

DELETE temperature 1234567890 FROM sensors location=room1 type=celsius;
```

moreover, here are some table management queries.
```sql
CREATE TABLE sensors TAGS location type;

CREATE TABLE devices;

ADD TAGS host TO devices;

REMOVE TAGS host FROM devices;

ADD TAGS location type TO devices;
```

and here's the grammar claude interpreted from the parsing! take from it what you will.

```bnf
<query> ::= <select_query> | <put_query> | <delete_query> | <create_query> 
          | <drop_query> | <add_query> | <remove_query>

<select_query> ::= "SELECT" <select_type> <metric> "FROM" <table_name> <select_clause>

<select_type> ::= "DATA" | "AVG" | "SUM" | "COUNT" | "MIN" | "MAX"

<select_clause> ::= <all_clause> | <between_clause> | <at_clause>

<all_clause> ::= "ALL" [<where_clause>] ";"

<between_clause> ::= "BETWEEN" <timestamp> "AND" <timestamp> [<where_clause>] ";"

<at_clause> ::= "AT" <timestamp> [<where_clause>] ";"

<where_clause> ::= "WHERE" <tag_list>

<put_query> ::= "PUT" <metric> <timestamp> <value> "INTO" <table_name> [<tag_list>] ";"

<delete_query> ::= "DELETE" <metric> <timestamp> "FROM" <table_name> [<tag_list>] ";"

<create_query> ::= "CREATE" "TABLE" <table_name> ["TAGS" <tag_columns>] ";"

<drop_query> ::= "DROP" "TABLE" <table_name> ";"

<add_query> ::= "ADD" "TAGS" <tag_key_list> "TO" <table_name> ";"

<remove_query> ::= "REMOVE" "TAGS" <tag_key_list> "FROM" <table_name> ";"

<tag_list> ::= <tag> [<tag>]*

<tag> ::= <tag_key> "=" <tag_value> [";"]*

<tag_columns> ::= <tag_key> [<tag_key>]*

<tag_key_list> ::= <tag_key> [<tag_key>]*

<metric> ::= <identifier>

<table_name> ::= <identifier>

<tag_key> ::= <identifier>

<tag_value> ::= <identifier>

<timestamp> ::= <unsigned_integer>

<value> ::= <number>

<identifier> ::= <letter> [<letter> | <digit>]*

<number> ::= ["-"] <digit> [<digit>]* ["." <digit>*]

<unsigned_integer> ::= <digit> [<digit>]*

<letter> ::= "A" | ... | "Z" | "a" | ... | "z"

<digit> ::= "0" | "1" | ... | "9"
```

again, if there are any holes in my logic, let me know. the midnight commits typically aren't the best.

## authors

[vinz kakilala](https://linkedin.com/in/vinzkakilala) (me).

## credits

used [murmurhash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) for the bloom filters. fast, uniform, deterministic.
