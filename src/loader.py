import json
from typing import List, Dict


class JSONLoader:
    @staticmethod
    def load(file_path: str) -> List[Dict]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, list):
                raise ValueError("JSON must contain an array")

            print(f"Uploaded {len(data)} records from {file_path}")
            return data

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON error: {e}")

    @staticmethod
    def validate(data: List[Dict], expected_fields: List[str], data_type: str) -> bool:
        for i, item in enumerate(data):
            for field in expected_fields:
                if field not in item:
                    print(f"In the record {i} field is missing '{field}'")
                    return False

        print(f" {data_type} the data is valid")
        return True