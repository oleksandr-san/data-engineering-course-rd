"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

from flask import Flask, request
from flask import typing as flask_typing

from job1.bll.sales_api import save_sales_to_local_disk

app = Flask(__name__)


@app.route("/", methods=["POST"])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "date: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    if request.json is None:
        return {"message": "JSON body is required"}, 400

    input_data: dict = request.json
    date = input_data.get("date")
    raw_dir = input_data.get("raw_dir")

    if not date:
        return {"message": "date parameter is required"}, 400
    if not raw_dir:
        return {"message": "raw_dir parameter is required"}, 400

    save_sales_to_local_disk(date=date, raw_dir=raw_dir)
    return {"message": "Data retrieved successfully from API"}, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
