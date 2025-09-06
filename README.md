# LSM-Tree Storage Engine

A basic implementation of an **LSM-tree (Log-Structured Merge Tree) based storage engine** in Python.  
This project demonstrates the fundamental concepts behind modern storage engines like **LevelDB, RocksDB, and Cassandra**.

## ✨ Features

- **In-memory storage (memtable)** with configurable threshold  
- **Segment files (SSTables)** written as JSON on disk  
- **Bloom Filters** for efficient key lookups  
- **Compaction & Merge** of segment files  
- **CLI** for continuous interaction (`put`, `get`, `remove`)  
- **Background compaction** (runs every 3 hours automatically)
