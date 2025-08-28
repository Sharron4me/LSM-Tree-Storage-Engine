import mmh3
from bitarray import bitarray
import os
import json

class BloomFilter:
    def __init__(self, size=1000, hash_count=5):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)

    def add(self, item):
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            self.bit_array[digest] = 1

    def check(self, item):
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            if self.bit_array[digest] == 0:
                return False
        return True

def build_bloom_from_json(segment_file, size=5000, hash_count=7):
    bf = BloomFilter(size, hash_count)
    with open(segment_file, "r") as f:
        data = json.load(f)
        for entry in data:
            bf.add(entry["key"])
    return bf

def search_in_json_segments(value, segment_files):
    for segment_file in segment_files:
        bf = build_bloom_from_json(segment_file)
        if not bf.check(value):
            continue
        with open(segment_file, "r") as f:
            data = json.load(f)
            for entry in data:
                if entry["key"] == value:
                    return entry, True
    return None, False