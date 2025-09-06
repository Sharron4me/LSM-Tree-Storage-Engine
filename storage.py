import threading
import datetime
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
        self.segment_file_path = os.getenv("SEGMENTS_PATH")
        self.threshold = os.getenv("THRESHOLD")
        self.segment = Segment()

    def insert_inmemory(self, key, val):
        self.inmemory_storage[key] = val
        return 'OK', True

    def delete_inmemory(self, key):
        return self.inmemory_storage.pop(key, None), True

    def store_inmemory(self):
        now = datetime.datetime.now()
        res = int(now.timestamp() * 1000)
        with self.inmemory_lock:
            segment_file_name = os.path.join(self.segment_file_path, 'segment_'+res+'.json')
            self.segment_files.append(segment_file_name)
            with open(segment_file_name, 'w') as f:
                json.dump(self.inmemory_storage, indent=4)
            self.inmemory_storage = {}
    
    def check_capacity_inmemort_storage(self):
        if len(self.inmemory_storage)>self.threshold:
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
        return self.segment.search_in_json_segments(key)