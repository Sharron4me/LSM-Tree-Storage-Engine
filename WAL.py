import os
import json
import datetime
from dotenv import load_dotenv

load_dotenv()

class WAL:
    def __init__(self):
        self.WAL_file_path = os.path.expanduser(os.getenv("WAL_PATH", "./wal"))
        os.makedirs(self.WAL_file_path, exist_ok=True)
        self.wal_path = os.path.join(self.WAL_file_path, "wal.log")

    def append(self, operation, key, value=None):
        entry = {
            "ts": int(datetime.datetime.now().timestamp() * 1000),
            "op": operation,
            "key": key,
            "value": value
        }
        with open(self.wal_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def replay(self):
        memtable = {}
        if not os.path.exists(self.wal_path):
            return memtable

        with open(self.wal_path, "r") as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if entry["op"] == "PUT":
                        memtable[entry["key"]] = {
                            "key": entry["key"],
                            "value": entry["value"],
                            "tombstone": False,
                            "ts": entry["ts"]
                        }
                    elif entry["op"] == "REMOVE":
                        memtable[entry["key"]] = {
                            "key": entry["key"],
                            "value": None,
                            "tombstone": True,
                            "ts": entry["ts"]
                        }
                except Exception:
                    continue
        return memtable

    def clear(self):
        open(self.wal_path, "w").close()
