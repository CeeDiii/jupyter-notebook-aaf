import json
import time
import requests

res = requests.get("http://localhost:7071/api/orchestrators/notebook_orchestrator")


def get_status(status_uri: str):
    res = requests.get(status_uri)
    if res.ok:
        data = res.json()
        if data["runtimeStatus"] not in ("Pending", "Running"):
            with open("output.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        else:
            print("Waiting...")
            time.sleep(1)
            return get_status(status_uri)
    else:
        raise requests.HTTPError(response=res)


if res.ok:
    data = res.json()
    get_status(status_uri=data["statusQueryGetUri"])
