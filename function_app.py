import logging
import json
import os
import tempfile
from typing import TypedDict

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
import papermill as pm

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(
    route="orchestrators/{functionName}/notebook_path/{notebookPath}", methods=["POST"]
)
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    function_name = req.route_params.get("functionName")
    if function_name is None:
        return func.HttpResponse(
            status_code=400, body="Function name is missing in request URL."
        )
    notebook_path = req.route_params.get("notebookPath")
    logging.debug("Notebook path %s", notebook_path)
    if notebook_path is None:
        return func.HttpResponse(
            status_code=400,
            body="Please specify the blob storage path from where you want to run your notebook, e.g. notebook_path=my-folder. The folder has to be a subdirectory of /jupyter-notebooks.",
        )
    notebook_name = req.params.get("notebook_name")
    if notebook_name is not None:
        notebook_path += f"/{notebook_name}"
    try:
        req_body = json.loads(req.get_json())
        data = req_body.get("data")
        logging.debug("data:\n\n%s", json.dumps(data, indent=4))
    except ValueError:
        return func.HttpResponse(
            status_code=400,
            body="Invalid body. Body could not be parsed to json. Make sure the json object contains a 'data' property.",
        )

    instance_id = await client.start_new(
        function_name,
        client_input=json.dumps(
            {"notebook_path": notebook_path, "data": data}, indent=4
        ),
    )
    response = client.create_check_status_response(req, instance_id)
    return response


# Orchestrator
@app.orchestration_trigger(context_name="context", orchestration="execute_notebook")
def notebook_orchestrator(context: df.DurableOrchestrationContext):
    json_input = context.get_input()
    logging.debug("orchestrator context input\n\n%s", json_input)
    if json_input is None:
        raise ValueError("The orchestrator expects a notebook path and data as input.")
    client_input = json.loads(json_input)
    if "notebook_path" not in client_input or "data" not in client_input:
        raise KeyError(f"Expected notebook_path and data, got {json_input}")
    notebook_path = client_input["notebook_path"]
    data = client_input["data"]

    nbs = yield context.call_activity("get_notebooks_from_blob_path", notebook_path)

    notebooks = json.loads(nbs)
    results = []
    for notebook in notebooks:
        output = yield context.call_activity(
            "execute_notebooks",
            {"notebook": notebook, "data": json.dumps(data, indent=4)},
        )
        result_value = yield context.call_activity("get_result_value", output)
        results.append(result_value)
    return results


@app.activity_trigger(input_name="path")
def get_notebooks_from_blob_path(path: str):
    connection_string = os.environ["BLOB_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("jupyter-notebooks")

    notebooks = []
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        if blob.name.startswith(path):
            data = container_client.download_blob(blob.name).readall()
            notebooks.append(data.decode("utf-8"))

    return json.dumps(notebooks, indent=4)


class ExecuteParams(TypedDict):
    notebook: str
    data: str


@app.activity_trigger(input_name="params")
def execute_notebooks(params: ExecuteParams):
    json_notebook = params.get("notebook")
    json_data = params.get("data")
    if json_notebook is None or json_data is None:
        raise KeyError(f"Expected notebook and data, got {params}")
    notebook = json.loads(json_notebook)
    data = json.loads(json_data)
    logging.debug("notebook input\n\n%s", json.dumps(notebook, indent=4))
    logging.debug("data input\n\n%s", json.dumps(data, indent=4))

    filename = ""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(notebook, f)
        filename = f.name
    notebook_output = pm.execute_notebook(
        input_path=filename,
        output_path=None,
        parameters={"params": json.dumps({"data": data}, indent=4)},
        kernel_name="python3",
    )
    logging.debug("notebook output\n\n%s", notebook_output)
    return notebook_output


@app.activity_trigger(input_name="nboutput")
def get_result_value(nboutput: dict):
    data = nboutput
    cells = data["cells"]

    def filter_by_result_value_tag(cell):
        metadata = cell["metadata"]
        tags = metadata["tags"]
        return len(tags) > 0 and "return_value" in tags

    # there should always be exactly one value
    result_value = list(filter(filter_by_result_value_tag, cells))[0]
    logging.debug("notebook return value\n\n%s", json.dumps(result_value, indent=4))
    return result_value
