# Working with vq

## Table management

Here are some table management queries.

```sql
CREATE TABLE climate TAGS region, season;

DROP TABLE devices;

ADD TAGS host, status TO servers;

REMOVE TAGS host FROM servers;
```

## Data manipulation

Here are some data manipulation queries.
```sql
SELECT DATA status FROM sensors ALL;

SELECT AVG temperature FROM weather BETWEEN 1234 AND 1240 WHERE city=london, unit=celsius;

PUT temperature 1234 23.5 INTO weather TAGS city=paris, unit=celsius;

DELETE rainfall 1234 FROM weather TAGS city=tokyo, unit=millimetres;
```

## Errors

There are two kinds of errors you can get—parse errors and runtime errors, occurring at the named points in time for self-explanatory reasons.

| ![vq-errors.png](images/vq-errors.png) | 
|:--:| 
| *A parse error and a runtime error in the REPL.* |

## EBNF

Here's the EBNF grammar encapsulating vq.

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

<identifier> ::= <char> {<char> | <digit>}*

<number> ::= {"-"}? <digit> {<digit>} {"." <digit>+}?

<char> ::= "A" | ... | "Z" | "a" | ... | "z" | "_"

<digit> ::= "0" | "1" | ... | "9"
```