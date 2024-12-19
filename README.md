# vkdb

A time-series database engine built in C++ with minimal dependencies.

## Why?

Wanted to challenge myself architecturally and push my boundaries with C++ in terms of both knowledge and performance.

## How do I use it?

Using CMake will make your life easier—add this to your `CMakeLists.txt` file:

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

Then, simply include the database header (or the vq header), and you'll have access to the interface.

```cpp
#include <vkdb/database.h>

int main()  {
  vkdb::Database db{"example-db"};
  db.createTable("example-table");
  // ...
}
```

Where you'd like to chain calls, you most likely can.

```cpp
db.createTable("sensor_data")
  .addTagColumn("location")
  .addTagColumn("type");

db.run("REMOVE TAGS type FROM sensor_data;")
  .run("PUT temperature 10 20.0 INTO sensor_data;")
  .runPrompt()
  .clear();
```

Moreover, you could play around with the vq REPL by running `vkdb::Database::runPrompt()` or `vkdb::VQ::runPrompt()`. The former operates on the database you call from, whilst the latter operates on a reserved database called `interpreter_default`.

The default REPL is generally for experimental purposes—there's not much to gain from it in practice besides having a vq playground.

```cpp
#include <vkdb/vq.h>

int main() {
  vkdb::Database database{"test"};
  database.runPrompt();
  vkdb::VQ::runPrompt();
}
```

If you would like to play around with some mock timestamps/values, feel free to use `vkdb::random<>`. Any arithmetic type (with no cv- or ref-qualifiers) can be passed in as a template argument, and you can optionally pass in a lower and upper bound (inclusive).

```cpp
auto random_int{vkdb::random<int>(-100'000, 100'000)};
auto random_double{vkdb::random<double>(-10.0, 10.0)};
```

Lastly, you can execute queries both from strings/files and via the builder interface. For instance, to take a few examples from the aptly named `examples` directory:

```cpp
vkdb::VQ::run(
  "SELECT COUNT temperature "
  "FROM atmospheric "
  "BETWEEN 1702550000 AND 1702650000 "
  "WHERE region=na;"
);

vkdb::VQ::runFile(
  std::filesystem::current_path() / "../examples/vq_setup.vq"
);

auto sum{table_REPLay.query()
  .whereTimestampBetween(0, 999)
  .whereMetricIs("metric")
  .whereTagsContain({"tag1", "value1"})
  .sum()
};

test_db
  .run("CREATE TABLE temp TAGS tag1, tag2;")
  .runFile(std::filesystem::current_path() / "../examples/vq_setup.vq")
  .runPrompt()
  .clear();
```

Again, note that execution using `vkdb::VQ` (see the first two examples) only executes on a default interpreter database. However, using the `vkdb::Database::run...` methods (see last two examples) will execute on the calling database. This is recommended!

## How does it work?

vkdb is built on log-structured merge (LSM) trees. In their simplest form, these have an in-memory layer and a disk layer, paired with a write-ahead log (WAL) for persistence of in-memory changes.

When you create your database (by instantiating a `vkdb::Database`), it persists on disk until you clear it via `vkdb::Database::clear`. It's best to make all interactions via this type, or perhaps the `vkdb::Table` type via `vkdb::Database::getTable`.

One important thing to note is that you should not manipulate the interpreter's database (`vkdb::INTERPRETER_DEFAULT_DATABASE`) via `vkdb::Database`. This is because the interpreter-based instance can become out-of-sync (due to some operations modifying memory and not disk).

In terms of typing, I've tried to make vkdb as robust as possible (as you can see with some of the verbose concepts), but there are bound to be some flaws here and there. Please bring them up!

## What's the query language?

I made a language called vq. Here are some example queries! I'm shamelessly using SQL highlighting here to save you from plain, white text.

```sql
SELECT DATA status FROM sensors ALL;

SELECT AVG temperature FROM weather BETWEEN 1234 AND 1240 WHERE city=london, unit=celsius;

PUT temperature 1234 23.5 INTO weather TAGS city=paris, unit=celsius;

DELETE rainfall 1234 FROM weather TAGS city=tokyo, unit=millimetres;
```

Moreover, here are some table management queries.

```sql
CREATE TABLE climate TAGS region, season;

DROP TABLE devices;

ADD TAGS host, status TO servers;

REMOVE TAGS host FROM servers;
```

And, here's the EBNF grammar encapsulating vq.

```bnf
<expr> ::= {<query> ";"}+

<query> ::= <select_query> | <put_query> | <delete_query> | <create_query>  | <drop_query> | <add_query> | <remove_query> | <tables_query>

<select_query> ::= "SELECT" <select_type> <metric> "FROM" <table_name> <select_clause>

<select_type> ::= "DATA" | "AVG" | "SUM" | "COUNT" | "MIN" | "MAX"

<select_clause> ::= <all_clause> | <between_clause> | <at_clause>

<all_clause> ::= "ALL" {<where_clause>}?

<between_clause> ::= "BETWEEN" <timestamp> "AND" <timestamp> {<where_clause>}?

<at_clause> ::= "AT" <timestamp> {<where_clause>}?

<where_clause> ::= "WHERE" <tag_list>

<put_query> ::= "PUT" <metric> <timestamp> <value> "INTO" <table_name> {"TAGS" <tag_list>}?

<delete_query> ::= "DELETE" <metric> <timestamp> "FROM" <table_name> {"TAGS" <tag_list>}?

<create_query> ::= "CREATE" "TABLE" <table_name> {"TAGS" <tag_list>}?

<drop_query> ::= "DROP" "TABLE" <table_name>

<add_query> ::= "ADD" "TAGS" <tag_columns> "TO" <table_name>

<remove_query> ::= "REMOVE" "TAGS" <tag_columns> "FROM" <table_name>

<tables_query> ::= "TABLES"

<tag_list> ::= <tag> {"," <tag>}*

<tag> ::= <tag_key> "=" <tag_value>

<tag_columns> ::= <tag_key> {"," <tag_key>}*

<tag_key> ::= <identifier>

<tag_value> ::= <identifier>

<metric> ::= <identifier>

<table_name> ::= <identifier>

<timestamp> ::= <number>

<value> ::= <number>

<identifier> ::= <char> [<char> | <digit>]*

<number> ::= ["-"] <digit> [<digit>]* ["." <digit>+]

<char> ::= "A" | ... | "Z" | "a" | ... | "z" | "_"

<digit> ::= "0" | "1" | ... | "9"
```

Again, if there are any holes in my logic, let me know. I hope you enjoy working with vkdb!

## Authors

[Vinz Kakilala](https://linkedin.com/in/vinzkakilala) (Me).

## Credits

Used [murmurhash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) for the Bloom filters. Fast, uniform, and deterministic.
