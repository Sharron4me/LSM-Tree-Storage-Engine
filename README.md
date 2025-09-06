# LSM-Tree Storage Engine

A basic implementation of an **LSM-tree (Log-Structured Merge Tree) based storage engine** in Python.  
This project demonstrates the fundamental concepts behind modern storage engines like **LevelDB, RocksDB, and Cassandra**.

## 🖥️ How to Run? 
### Install dependencies
```
pip install -r requirements.txt
```
### Start Cli
```
python3 storage.py
```

## ✨ Features

- **In-memory storage (memtable)** : Fast writes stored in memory before being flushed to disk.
- **Write-Ahead Log (WAL)** : Ensures durability by logging every operation before applying it to memory. On crash, the WAL is replayed to restore state.
- **Segment Files** : Immutable on-disk JSON files created when memtable exceeds threshold.
- **Bloom Filters** : Efficient key existence checks before reading segment files.
- **Compaction** : Merges multiple segment files, discarding old values and tombstones.
- **Tombstones** : Deletion markers to handle deletes across immutable segments.
- **CLI Interface** : Simple shell to interact with the store (put, get, remove).


## 📌 ToDo
- SSTable format with sorted keys.
- Range queries & prefix scans.
- More efficient Bloom filter tuning (adaptive).
- WAL file rotation and checkpointing.
- Multi-threaded background compaction.

### Sample Output
```
🚀 Welcome to LSM Storage CLI (type 'help' for commands)

lsm> put a 10
('OK', True)

lsm> get a
10

lsm> remove a
('DELETED', True)

lsm> get a
None
```

