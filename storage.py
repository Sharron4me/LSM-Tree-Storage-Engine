import threading
import datetime
import time
import json
import os
from dotenv import load_dotenv
from segment import Segment

load_dotenv()


class storage_engine:
    def __init__(self):
        self.inmemory_storage = {}
        self.inmemory_lock = threading.RLock()
        self.segment_files = []
        self.segment_file_path = os.path.expanduser(os.getenv("SEGMENTS_PATH", "./segments"))
        self.threshold = int(os.getenv("THRESHOLD", 100))
        self.segment = Segment()

    def insert_inmemory(self, key, val):
        self.check_capacity_inmemory_storage()
        self.inmemory_storage[key] = val
        return 'OK', True

    def delete_inmemory(self, key):
        return self.inmemory_storage.pop(key, None), True

    def rotate_segment_file(self):
        now = datetime.datetime.now()
        res = int(now.timestamp() * 1000)
        with self.inmemory_lock:
            segment_file_name = os.path.join(self.segment_file_path, f"segment_{res}.json")
            self.segment_files.append(segment_file_name)
            with open(segment_file_name, 'w') as f:
                json.dump(self.inmemory_storage, f,indent=4)
            self.inmemory_storage = {}
    
    def check_capacity_inmemory_storage(self):
        if len(self.inmemory_storage)>self.threshold:
            self.rotate_segment_file()
            return "Overflow", True
        return "No Overflow", True
 
    def find_value(self, key):
        with self.inmemory_lock:
            if key in self.inmemory_storage:
                return self.inmemory_storage[key]
        return self.segment.search_in_json_segments(key)
    
    def background_compaction(self):
        while True:
            self.segment.merge_segments()
            time.sleep(3 * 60 * 60)

    def cli(self):
        print("🚀 Welcome to LSM Storage CLI (type 'help' for commands)")
        while True:
            cmd = input("lsm> ").strip().split()

            if not cmd:
                continue

            if cmd[0].lower() == "put" and len(cmd) == 3:
                print(self.insert_inmemory(cmd[1], cmd[2]))

            elif cmd[0].lower() == "get" and len(cmd) == 2:
                print(self.find_value(cmd[1]))

            elif cmd[0].lower() == "remove" and len(cmd) == 2:
                print(self.delete_inmemory(cmd[1]))

            elif cmd[0].lower() == "exit":
                print("👋 Exiting CLI...")
                break

            elif cmd[0].lower() == "help":
                print("Commands:")
                print("  put <key> <value>  → Insert key/value")
                print("  get <key>          → Retrieve value")
                print("  remove <key>          → Remove value")
                print("  exit               → Quit CLI")

            else:
                print("❌ Invalid command. Type 'help' for usage.")

if __name__ == '__main__':
    store = storage_engine()
    threading.Thread(target=store.background_compaction, daemon=True).start()
    store.cli()