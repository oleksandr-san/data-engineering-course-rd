import os

from dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    print("\tI'm in get_sales(...) function!")
    data = sales_api.get_sales(date)
    path = os.path.join(raw_dir, f"sales_{date}.json")
    local_disk.save_to_disk(data, path)
