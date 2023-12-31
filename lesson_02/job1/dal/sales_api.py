import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import requests
from job1.settings import AUTH_TOKEN

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/"
PAGES_COUNT = 4


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    if not AUTH_TOKEN:
        raise ValueError("API_AUTH_TOKEN environment variable must be set")

    with requests.Session() as session:
        session.headers.update({"Authorization": AUTH_TOKEN})
        futures = {}
        with ThreadPoolExecutor() as executor:
            for page in range(1, PAGES_COUNT + 1):
                f = executor.submit(
                    session.get,
                    url=API_URL + "sales",
                    params={"date": date, "page": page},
                )
                futures[f] = page

        results = []
        for future in futures:
            page = futures[future]
            response = future.result()
            try:
                response.raise_for_status()
                results.extend(response.json())
            except requests.RequestException as ex:
                logging.error("Failed to get data for page %s: %s", page, ex)
        return results
