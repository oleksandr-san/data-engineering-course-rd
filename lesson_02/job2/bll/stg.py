import logging
import os

from job2.dal import local_disk


def process_stg(stg_dir: str, raw_dir: str) -> None:
    logging.info("Processing %s into %s", raw_dir, stg_dir)
    for path in local_disk.list_files(raw_dir):
        data = local_disk.load_json(path)
        logging.info("Read %s rows from %s", len(data), path)

        stg_file = os.path.basename(path).replace(".json", ".avro")
        stg_path = os.path.join(stg_dir, stg_file)
        local_disk.save_avro(data, path=stg_path)
