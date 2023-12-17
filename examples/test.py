import json
import os
import time

from dotenv import load_dotenv
import requests

load_dotenv()


def get_status(status_uri: str):
    res = requests.get(status_uri)
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


DEV_PATH = "http://localhost:7071/api/orchestrators/execute_notebook?notebook_path=test/test.ipynb"
PROD_PATH = "https://jupyter-notebook-as-a-function.azurewebsites.net/api/orchestrators/execute_notebook?notebook_path=test/test.ipynb"

res = requests.post(
    DEV_PATH,
    json={"write_to_sql": True},
    headers={"x-functions-key": os.environ["FUNCTION_KEY"]},
    timeout=30,
)
if res.ok:
    data = res.json()
    get_status(status_uri=data["statusQueryGetUri"])
else:
    raise requests.HTTPError(response=res)
