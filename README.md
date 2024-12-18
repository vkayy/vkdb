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

where you'd like to chain calls, you most likely can.

```cpp
db.createTable("sensor_data")
  .addTagColumn("location")
  .addTagColumn("type");

db.run("REMOVE TAGS type FROM sensor_data;")
  .run("PUT temperature 10 20.0 INTO sensor_data;")
  .runPrompt()
  .clear();
```

moreover, you could play around with the vq repl by running `vkdb::Database::runPrompt()` or `vkdb::VQ::runPrompt()`! doing the the former allows you to operate on the specific database you call from, whilst the latter means you operate on a default interpreter database. the default repls are honestly purely for experimental purposes—there's not much to gain besides having a vq playground.

```cpp
#include <vkdb/vq.h>

int main() {
  vkdb::Database database{"test"};
  database.runPrompt();
  vkdb::VQ::runPrompt();
}
```

if you want to play around with some mock timestamps/values, feel free to use `vkdb::random<>`. any arithmetic type (with no cv- or ref-qualifiers) can be passed in as a template argument, and you can optionally pass in a lower and upper bound (inclusive).

```cpp
auto random_int{vkdb::random<int>(-100'000, 100'000)};
auto random_double{vkdb::random<double>(-10.0, 10.0)};
```

lastly, you can execute queries both from strings/files and via the builder interface. for instance, to yank a few examples from the aptly named `examples` directory:

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

auto sum{table_replay.query()
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

again, note that execution using `vkdb::VQ` (see the first two examples) only executes on a default interpreter database. however, using the `vkdb::Database::run...` interface (see last two examples) will execute on the called database. this is recommended!

## how does it work?

vkdb is built on log-structured merge (lsm) trees. in their simplest form, these have an in-memory layer and a disk layer, paired with a write-ahead log (wal) for persistence of in-memory changes.

when you create your database (by instantiating a `vkdb::Database`), it persists on disk until you clear it via `vkdb::Database::clear`. it's best to make all interactions via this type, or perhaps the `vkdb::Table` type via `vkdb::Database::getTable`.

one important thing to note is that you should not manipulate the interpreter's database (`vkdb::INTERPRETER_DEFAULT_DATABASE`) via `vkdb::Database`. this is because the interpreter-based instance can become out-of-sync (due to some operations modifying memory and not disk).

in terms of typing, i've tried to make vkdb as robust as possible (as you can see with some of the verbose concepts), but there are bound to be some flaws here and there. bring them up!

## what's the query language?

i made a language called vq. here are some example queries! i'm shamelessly using sql highlighting here to save you from plain, white text.

```sql
SELECT DATA status FROM sensors ALL;

SELECT AVG temperature FROM weather BETWEEN 1234 AND 1240 WHERE city=london, unit=celsius;

PUT temperature 1234 23.5 INTO weather TAGS city=paris, unit=celsius;

DELETE rainfall 1234 FROM weather TAGS city=tokyo, unit=millimetres;
```

moreover, here are some table management queries.
```sql
CREATE TABLE climate TAGS region, season;

DROP TABLE devices;

ADD TAGS host, status TO servers;

REMOVE TAGS host FROM servers;
```

and here's the EBNF grammar encapsulating vq.

```bnf
<expr> ::= {<query> ";"}+

<query> ::= <select_query> | <put_query> | <delete_query> | <create_query>  | <drop_query> | <add_query> | <remove_query>

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

again, if there are any holes in my logic, let me know. i'm going to be working on adding more utility (including changing databases within interpreter-based execution), so look forward to that.

## authors

[vinz kakilala](https://linkedin.com/in/vinzkakilala) (me).

## credits

used [murmurhash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) for the bloom filters. fast, uniform, deterministic.
