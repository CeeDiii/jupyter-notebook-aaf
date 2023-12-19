import argparse
import json
import os
import sys
import time
from typing import TypedDict

from dotenv import load_dotenv
import requests


class ClientConfig(TypedDict):
    DEV_BASE_URL: str
    PROD_BASE_URL: str
    ORCHESTRATOR_PATH: str
    NOTEBOOK_PATH: str


def read_json_config(file_path: str) -> ClientConfig:
    """
    read the config file and return it as dict
    """
    with open(file_path, "r", encoding="utf-8") as f:
        config = json.load(f)
        return config


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
        "-e",
        "--env",
        required=True,
        choices=["dev", "prod"],
        default="dev",
        help="Define if you want to query the local endpoint in development or the deployed endpoint in production.",
    )

    parser.add_argument(
        "-n",
        "--notebook",
        required=False,
        help="The path to the notebook that you want to execute. Remember that the notebooks have to be stored in the 'jupyter-notebooks' blob container.",
    )

    args = vars(parser.parse_args(sys.argv[1:]))
    print(f"Querying for execution in environment {args['env']}")

    load_dotenv()
    script_path = os.path.dirname(sys.argv[0])
    config_path = script_path + "/client.config.json"
    client_config = read_json_config(config_path)

    base_url = client_config["DEV_BASE_URL"]
    orchestrator_path = client_config["ORCHESTRATOR_PATH"]
    notebook_path = client_config["NOTEBOOK_PATH"]

    if args["env"] == "prod":
        base_url = client_config["PROD_BASE_URL"]

    user_notebook_path = args.get("notebook")
    if user_notebook_path is not None:
        notebook_path = user_notebook_path

    request_url = base_url + orchestrator_path + notebook_path

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
