import os
import glob
import json
import datetime
from dotenv import load_dotenv
from BloomFilter import build_bloom_from_json

load_dotenv()

class Segment:
    def __init__(self):
        self.segment_file_path = os.path.expanduser(os.getenv("SEGMENTS_PATH", "./segments"))
        self.threshold = int(os.getenv("THRESHOLD", 100))
        self.create_ws()

    def create_ws(self):
        expanded_path = os.path.expanduser(self.segment_file_path)
        os.makedirs(expanded_path, exist_ok=True)
        return expanded_path

    def get_segment_files(self):
        return glob.glob(os.path.join(self.segment_file_path, "*.json"))

    def merge_segments(self):
        json_files = self.get_segment_files()
        merged_data = {}
        output_files = []

        for segment_file in json_files:
            with open(segment_file, "r") as f:
                data = json.load(f)
                merged_data.update(data)

            if len(json.dumps(merged_data)) > self.threshold:
                now = datetime.datetime.now()
                res = str(int(now.timestamp() * 1000))
                output_file = os.path.join(self.segment_file_path, f"segment_{res}.json")
                with open(output_file, "w") as out:
                    json.dump(merged_data, out, indent=2)
                output_files.append(output_file)
                merged_data = {}

        if merged_data:
            now = datetime.datetime.now()
            res = str(int(now.timestamp() * 1000))
            output_file = os.path.join(self.segment_file_path, f"segment_{res}.json")
            with open(output_file, "w") as out:
                json.dump(merged_data, out, indent=2)
            output_files.append(output_file)

        return output_files

    def merge_segments(self):
        json_files = sorted(self.get_segment_files())
        merged = {}
        output_files = []

        for segment_file in json_files:
            with open(segment_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data = [{"key": k, "value": v, "tombstone": False, "ts": 0} for k, v in data.items()]

                for entry in data:
                    k, ts = entry["key"], entry.get("ts", 0)
                    if k not in merged or merged[k]["ts"] < ts:
                        merged[k] = entry

            if len(json.dumps(merged)) > self.threshold:
                now = int(datetime.datetime.now().timestamp() * 1000)
                output_file = os.path.join(self.segment_file_path, f"segment_{now}.json")

                cleaned = [v for v in merged.values() if not v.get("tombstone", False)]

                with open(output_file, "w") as out:
                    json.dump(cleaned, out, indent=2)
                output_files.append(output_file)

                merged = {}
        if merged:
            now = int(datetime.datetime.now().timestamp() * 1000)
            output_file = os.path.join(self.segment_file_path, f"segment_{now}.json")

            cleaned = [v for v in merged.values() if not v.get("tombstone", False)]
            with open(output_file, "w") as out:
                json.dump(cleaned, out, indent=2)
            output_files.append(output_file)

        return output_files

    def search_in_json_segments(self, key):
        for segment_file in self.get_segment_files():
            bf = build_bloom_from_json(segment_file)
            if not bf.check(key):
                continue
            with open(segment_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    if key in data:
                        return data[key]
                elif isinstance(data, list):
                    for entry in data:
                        if entry["key"] == key:
                            return entry
        return None