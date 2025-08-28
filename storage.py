import threading
import datetime
import json

from BloomFilter import search_in_json_segments


class storage_engine:
    def __init__(self):
        self.inmemory_storage = {}
        self.inmemory_lock = threading.RLock()
        self.inmemory_max_size = 1000
        self.segment_files = []

    def insert_inmemory(self, key, val):
        self.inmemory_storage[key] = val
        return 'OK', True

    def delete_inmemory(self, key):
        return self.inmemory_storage.pop(key, None), True

    def store_inmemory(self):
        now = datetime.datetime.now()
        res = int(now.timestamp() * 1000)
        with self.inmemory_lock:
            segment_file_name = 'segment'+res+'.json'
            self.segment_files.append(segment_file_name)
            with open(segment_file_name, 'w') as f:
                json.dump(self.inmemory_storage, indent=4)
            self.inmemory_storage = {}
    
    def check_capacity_inmemort_storage(self):
        if len(self.inmemory_storage)>self.inmemory_max_size:
            self.store_inmemory()
            return "Overflow", True
        return "No Overflow", True

    def perform_operation(self, operation, *args):
        with self.inmemory_lock:
            self.store_inmemory()
            if operation == 'delete' and len(args) == 1:
                    return self.delete_inmemory(args[0])
            elif operation == 'insert' and len(args) == 2:
                    return self.delete_inmemory(args[0], args[1])
            else:
                return "Invalid Operation", False

    def find_value(self, key):
        with self.inmemory_lock:
            if key in self.inmemory_storage:
                return self.inmemory_storage[key]
        return search_in_json_segments(key, self.segment_files)