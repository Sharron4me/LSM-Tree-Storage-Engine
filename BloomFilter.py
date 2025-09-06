import mmh3
from bitarray import bitarray
import json
import math


class BloomFilter:
    def __init__(self, n=None, p=None, size=None, hash_count=None):
        if n and p:
            self.size = int(- (n * math.log(p)) / (math.log(2) ** 2))
            self.hash_count = int((self.size / n) * math.log(2))
        elif size and hash_count:
            self.size = size
            self.hash_count = hash_count
        else:
            raise ValueError("Provide either (n, p) or (size, hash_count)")

        self.bit_array = bitarray(self.size)
        self.bit_array.setall(0)

    def add(self, item: str):
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            self.bit_array[digest] = 1

    def check(self, item: str) -> bool:
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            if self.bit_array[digest] == 0:
                return False
        return True


def build_bloom_from_json(segment_file):
    # Example: expecting ~1000 keys, 1% false positive rate
    bf = BloomFilter(n=1000, p=0.01)

    with open(segment_file, "r") as f:
        data = json.load(f)
        if isinstance(data, dict):
            for key in data.keys():
                bf.add(key)
        elif isinstance(data, list):
            for entry in data:
                bf.add(entry["key"])

    return bf