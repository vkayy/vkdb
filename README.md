# vkdb

A time series database engine currently being built in C++ with minimal dependencies.

## How will it work?

Initially, key-value pairs are written to an in-memory table (memtable). As in-memory data is volatile, to ensure durability, every write operation is first recorded in a write-ahead log (WAL), guaranteeing no data is lost in the event of a crash. Once a memtable exceeds a certain threshold, it is frozen and made immutable. From this point, it is flushed to the disk as an SSTable.

These SSTables make up the majority of the LSM tree's pyramid architecture, being divided into levels of increasing population and ordered by recency. When a key is looked up, we initially check the memtable (at any point in time, only one will be active) and continue to move down the levels of the LSM tree.

As operations are append-only, updates and deletions are done a little differently to a typical relational database engine's. That is, updates are treated no differently to insertions; after all, key-value stores are checked in order of recency. Similarly, deletions are just like insertions, but the value is marked with a 'tombstone', signifying deletion.

Now, from this point, we notice that this can lead to a lot of wasted disk space -- if we've updated a key hundreds of times, it'll have hundreds of redundant entries. This is solved with periodic compaction, which is a process where SSTables with overlapping key ranges are merged together, and is divided into many kinds, including time-window compaction strategy (TWCS).

TWCS organises and compacts data based on time intervals, and this is beneficial to us, given that we're working with time series data workloads. Hence, TWCS becomes most appropriate, and with it, we're also able to avoid size, write, and read amplification.

From this point on, there are a few other optimisations I intend on implementing, and also my own custom query language.

## What's the roadmap?

- [x] Memtables.
- [x] Write-ahead log.
- [x] SSTables.
- [x] LSM tree architecture.
- [x] Time-window compaction strategy.
- [x] Bloom filters.
- [x] Summary tables.
- [x] LRU cache.
- [x] LSM tree caching.
  - [x] Key-value pair cache.
  - [x] SSTable block cache. 
- [ ] Query parser.
- [ ] Query execution engine.
