import argparse
import json
import os
import sys
import time

from dotenv import load_dotenv
import requests


def get_status(status_uri: str):
    """
    query for the execution status and write result to a file
    """
    res = requests.get(status_uri, timeout=10)
    if res.ok:
        data = res.json()
        if data["runtimeStatus"] not in ("Pending", "Running"):
            print(f"Execution {data['runtimeStatus']}")
            with open("output.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        else:
            print(f"{data['runtimeStatus']}...")
            time.sleep(1)
            return get_status(status_uri)
    else:
        raise requests.HTTPError(response=res)


def main():
    parser = argparse.ArgumentParser(
        prog="client",
        description="A client to test the Jupyter-notebook-as-a-function locally or in production",
    )
    parser.add_argument(
        "-e", "--env", required=True, choices=["dev", "prod"], default="dev"
    )
    args = vars(parser.parse_args(sys.argv[1:]))
    print(f"Querying for execution in environment {args['env']}")

    load_dotenv()

    base_url = "http://localhost:7071"
    API_PATH = "/api/orchestrators/execute_notebook?notebook_path=example/example.ipynb"

    if args["env"] == "prod":
        base_url = "https://jupyter-notebook-as-a-function.azurewebsites.net"

    request_url = base_url + API_PATH

    res = requests.post(
        request_url,
        json={
            "write_to_sql": True,
            "debug": True,
            "data": {
                "super_mario": [
                    {"name": "Mario", "age": 34},
                    {"name": "Luigi", "age": 34},
                ]
            },
            "kwargs": {"hello": "world"},
        },
        headers={"x-functions-key": os.environ["FUNCTION_KEY"]},
        timeout=30,
    )
    if res.ok:
        data = res.json()
        get_status(status_uri=data["statusQueryGetUri"])
    else:
        print(res.status_code, res.text)
        raise requests.HTTPError(response=res)


if __name__ == "__main__":
    main()
