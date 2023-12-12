import logging
import json
import os
import tempfile
from typing import TypedDict

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
import papermill as pm

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# TODO create a single generic endpoint that takes the Blob path name as a parameter and executes all notebooks in this path and returns all results
# TODO check if we can fetch data in ADF and use them as json in request


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}/{notebookPath}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    # TODO check if route params exist
    function_name = req.route_params.get("functionName")
    notebook_path = req.route_params.get("notebookPath")
    # TODO check if data param is populated in req body
    req_body = json.loads(req.get_json())
    data = req_body.get("data")
    instance_id = await client.start_new(
        function_name,
        client_input=json.dumps(
            {"notebook_path": notebook_path, "data": data}, indent=4
        ),
    )
    response = client.create_check_status_response(req, instance_id)
    return response


# Orchestrator
@app.orchestration_trigger(context_name="context")
def notebook_orchestrator(context: df.DurableOrchestrationContext):
    # TODO check input
    client_input = json.loads(context.get_input())
    notebook_path = client_input["notebook_path"]
    data = client_input["data"]
    notebooks = yield context.call_activity(
        "get_notebooks_from_blob_path", notebook_path
    )

    output = yield context.call_activity(
        "execute_notebooks",
        {"notebooks": notebooks, "data": json.dumps(data, indent=4)},
    )
    result_value = yield context.call_activity(
        "get_result_value", json.dumps(output, indent=4)
    )
    return [result_value]


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
    notebooks: str
    data: str


@app.activity_trigger(input_name="params")
def execute_notebooks(params: ExecuteParams):
    notebooks = json.loads(params["notebooks"])
    data = json.loads(params["data"])
    for notebook in notebooks:
        nb = json.loads(notebook)
        filename = ""
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(nb, f)
            filename = f.name
        returned_notebook = pm.execute_notebook(
            input_path=filename,
            output_path=None,
            parameters={"params": json.dumps({"data": data}, indent=4)},
            kernel_name="python3",
        )
        return returned_notebook


@app.activity_trigger(input_name="nboutput")
def get_result_value(nboutput: str):
    data = json.loads(nboutput)
    cells = data["cells"]

    def filter_by_result_value_tag(cell):
        metadata = cell["metadata"]
        tags = metadata["tags"]
        return len(tags) > 0 and "return_value" in tags

    # there should always be exactly one value
    result_value = list(filter(filter_by_result_value_tag, cells))[0]
    return result_value
