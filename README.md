# LSM-Tree Storage Engine

A basic implementation of an **LSM-tree (Log-Structured Merge Tree) based storage engine** in Python.  
This project demonstrates the fundamental concepts behind modern storage engines like **LevelDB, RocksDB, and Cassandra**.

## ✨ Features

- **In-memory storage (memtable)** : Fast writes stored in memory before being flushed to disk.
- **Write-Ahead Log (WAL)** : Ensures durability by logging every operation before applying it to memory. On crash, the WAL is replayed to restore state.
- **Segment Files** : Immutable on-disk JSON files created when memtable exceeds threshold.
- **Bloom Filters** : Efficient key existence checks before reading segment files.
- **Compaction** : Merges multiple segment files, discarding old values and tombstones.
- **Tombstones** : Deletion markers to handle deletes across immutable segments.
- **CLI Interface** : Simple shell to interact with the store (put, get, remove).
