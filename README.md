# `vkdb`

A time series database engine currently being built in C++ with minimal dependencies.

## About the engine

`vkdb` is being built to be a performant database engine in C++, speciaised for time series data. With the implementation of the query language VQL, it will enable range and aggregation queries with a simple syntax, combining the readability of SQL with the flexibiity of Q.

I wanted to gain a deeper understanding of databases, with time series databases being an area of complete inexperience in particular. Moreover, time series anaysis is integral to quantitative finance, and so `vkdb` single-handedly fuses both of these interests of mine.

### What's under the hood

#### The storage engine

Arguably, the most interesting part! `vkdb`'s storage engine is backed by an LSM tree, which is best suited for high write throughput. Since time series data is generally append-only, I decided on this architecture, supported with a write-through (key-value pair) and read-through (SSTable block) cache to make up for costly read operations.

Key-value pairs are first written to an in-memory table (memtable), which is volatile. To ensure durability, every write operation is first recorded in a write-ahead log (WAL), guaranteeing no data is lost in the event of a crash. Once a memtable exceeds a certain threshold, it is frozen and made immutable. From this point, it is flushed to the disk as an SSTable.

These SSTables make up the majority of the LSM tree's pyramid architecture, being divided into levels of increasing population and ordered by recency. When a key is looked up, we initially check the memtable (at any point in time, only one will be active) and continue to move down the levels of the LSM tree to check SSTables.

As operations are append-only, updates and deletions are done a little differently to a typical relational database engine's. That is, updates are treated no differently to insertions; after all, key-value stores are checked in order of recency. Similarly, deletions are just like insertions, but the value is marked with a 'tombstone', signifying deletion.

Now, from this point, we notice that this can lead to a lot of wasted disk space – if we've updated a key hundreds of times, it'll have hundreds of redundant entries. This is solved with periodic compaction, which is a process where SSTables with overlapping key ranges are merged together, and is divided into many kinds, with our strategy being time-window compaction.

TWCS organises and compacts data based on time intervals, and this helps us to reduce size, write, and read amplification when working with time series workloads.

From this point on, there are a few other optimisations I've implemented. Bloom filters are a probabilistic, space-efficient data structure that can determine whether an item is 'probably' in a set, or definitely not in a set. Moreover, summary tables map an SSTable to their min-max key range, enabling us to skip over certain SSTables when checking for the values associated with a key. Together, both of these features help to reduce the need for unnecessary disk I/O, and this has much more overhead than reading from memory.

#### Query language (VQL) & Transport layer

Work-in-progress!

## Roadmap

- [x] Storage engine.
  - [x] LSM tree architecture.
    - [x] Memtables.
    - [x] Write-ahead log.
    - [x] SSTables.
      - [x] SSTable indexing.
    - [x] Time-window compaction.
    - [x] Bloom filters.
    - [x] Summary tables.
  - [x] LSM tree caching.
    - [x] LRU cache implementation.
    - [x] Key-value pair cache.
    - [x] SSTable block cache.
- [ ] Query language (VQL). 
  - [ ] Query parser.
  - [ ] Query execution engine.
- [ ] Transport layer.

## License

This project is licensed under the MIT License – see the [LICENSE](https://github.com/vkayy/vkdb/blob/main/LICENSE) file for details.

## Authors

Vinz Kakilala: [GitHub](https://github.com/vkayy) - [LinkedIn](https://linkedin.com/in/vinzkakilala)

## Acknowledgements

- [MurmurHash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) (A uniform, deterministic hash function used in `vkdb`'s Bloom filters.)
