import json
import logging
import os
from typing import Any, Dict, List

import fastavro


def list_files(dir: str) -> List[str]:
    return [os.path.join(dir, file) for file in os.listdir(dir)]


def load_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r") as f:
        return json.load(f)


schema = {
    "doc": "Sales data reading.",
    "name": "Sales",
    "namespace": "sales",
    "type": "record",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": {"type": "string", "logicalType": "date"}},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "long"},
    ],
}


parsed_schema = fastavro.parse_schema(schema)


def save_avro(content: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        logging.info(f"Writing Avro file {path}...")
        fastavro.writer(f, parsed_schema, content)


def load_avro(path: str) -> List[Dict[str, Any]]:
    logging.info(f"Reading Avro file {path}...")
    result = []
    with open(path, "rb") as f:
        for line in fastavro.reader(f, parsed_schema):
            result.append(line)
    return result
