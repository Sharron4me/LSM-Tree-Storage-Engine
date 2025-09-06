import os
import glob
import json
import datetime
from dotenv import load_dotenv

from BloomFilter import build_bloom_from_json

# load .env file
load_dotenv()

def Segment():
    
    def __init__(self):
        self.segment_file_path = os.getenv("SEGMENTS_PATH")
        self.threshold = os.getenv("THRESHOLD")
        self.create_ws()

    def create_ws(self):
        expanded_path = os.path.expanduser(self.segment_file_path)
        os.makedirs(expanded_path, exist_ok=True)
        return expanded_path

    def get_segment_files(self):
        return glob.glob(os.path.join(self.segment_file_path, "*.json"))

    def merge_segments(self):
        json_files = glob.glob(os.path.join(self.segment_file_path, "*.json"))
        merged_data = {}
        output_files = []

        for segment_file in json_files:
            with open(segment_file, "r") as f:
                data = json.load(f)
                merged_data.update(data)  # merge segment

            # Check size (approximate: length of JSON string)
            if len(json.dumps(merged_data)) > threshold:
                now = datetime.datetime.now()
                res = str(int(now.timestamp() * 1000))
                segment_file_name = 'segment'+res+'.json'
                output_file = f"segment_{segment_file_name}.json"
                with open(output_file, "w") as out:
                    json.dump(merged_data, out, indent=2)
                output_files.append(output_file)

                # Reset for next iteration
                merged_data = {}

        # Save any remaining data
        if merged_data:
            now = datetime.datetime.now()
            res = str(int(now.timestamp() * 1000))
            output_file = f"segment_{res}.json"
            with open(output_file, "w") as out:
                json.dump(merged_data, out, indent=2)
            output_files.append(output_file)

        return output_files

    def search_in_json_segments(self, value):
        for segment_file in self.get_segment_files():
            bf = build_bloom_from_json(segment_file)
            if not bf.check(value):
                continue
            with open(segment_file, "r") as f:
                data = json.load(f)
                for entry in data:
                    if entry["key"] == value:
                        return entry, True
        return None, False