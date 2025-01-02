# Internals

## Database engine

### Architecture

vkdb is built on log-structured merge (LSM) trees. In their simplest form, these have an in-memory layer and a disk layer, paired with a write-ahead log (WAL) for persistence of in-memory changes.

When you instantiate a `vkdb::Database`, all of the prior in-memory information (in-memory layer, metadata, etc.) will be loaded in if the database already exists, and if not, a new one is set up. This persists on disk until you clear it via `vkdb::Database::clear`.

It's best to make all interactions via `vkdb::Database`, or the `vkdb::Table` type via `vkdb::Database::getTable`, unless you just want to play around with vq (more on the playground [here](2_usage.md)).

> [!NOTE]
> Make sure the `$HOME` environment variable is set correctly, as all database files will be stored in `.vkdb` within your home directory. Only tamper with this directory if moving databases between machines!

![](images/database-engine-internals.png)

### Compaction

The LSM tree uses time-window compaction to efficiently organise and merge SSTables across different layers (C0-C7). Each layer has a specific time window size and maximum number of SSTables:

| Layer | Time Window | Max. SSTables |
|-------|------------|--------------|
| C0 | Overlapping | 10 |
| C1 | 30 minutes | 100 |
| C2 | 1 hour | 100 |
| C3 | 1 day | 1,000 |
| C4 | 1 week | 1,000 |
| C5 | 1 month | 1,000 |
| C6 | 3 months | 10,000 |
| C7 | 1 year | 10,000 |

When the memtable fills up, it is flushed to C0 as an SSTable. C0 acts as a buffer for the later layers, and when it exceeds its SSTable limit, all the SSTables are merged into C1 at once, with each SSTable spanning a 30-minute window.

When any other layer exceeds its SSTable limit, its oldest excess SSTables are merged with the next layer's SSTables based on the layer's time window. For example, if C1 has too many SSTables.

1. The oldest SSTables from C1 are selected.
2. Any overlapping SSTables in C2 are identified based on 1-hour time windows.
3. The selected SSTables are merged into new SSTables in C2.
4. Original SSTables are removed after successful merge.

![](images/compaction-internals.png)

This time-window compaction strategy enables:
- Fast queries, as SSTables beyond C0 are disjoint and only intersecting ranges need to be scanned.
- Efficient storage, as older data is consolidated into larger chunks whilst recent data stays grnaular.
- Reduced write amplification, with C0 as a buffer and merges occurring on progressively larger time windows.

## Query processing

Lexing is done quite typically, with enumerated token types and line/column number stored for error messages. Initially, I directly executed queries as string streams, but that was a nightmare for robustness.

In terms of parsing, vq has been constructed to have an LL(1) grammar—this meant I could write a straightforward recursive descent parser for the language. This directly converts queries to an abstract syntax tree (AST) with `std::variant`.

Finally, the interpreter makes quick use of the AST via the visitor pattern, built into C++ with `std::variant` (mentioned earlier) and `std::visit`. This ended up making the interpreter (and pretty-printer) very satisfying to write.

![](images/query-processing-internals.png)