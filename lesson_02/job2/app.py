"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

import logging

import bll.stg as stg
from flask import Flask, request
from flask import typing as flask_typing

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


@app.route("/", methods=["POST"])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09",
    }
    """
    if request.json is None:
        return {"message": "JSON body is required"}, 400

    input_data: dict = request.json
    stg_dir = input_data.get("stg_dir")
    raw_dir = input_data.get("raw_dir")

    if not stg_dir:
        return {"message": "stg_dir parameter is required"}, 400
    if not raw_dir:
        return {"message": "raw_dir parameter is required"}, 400

    stg.process_stg(raw_dir=raw_dir, stg_dir=stg_dir)
    return {"message": "Data processed successfully"}, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
