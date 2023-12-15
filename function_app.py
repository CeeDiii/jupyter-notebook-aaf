import logging
import json
import os
from typing import TypedDict

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
import nbformat
import papermill as pm

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    function_name = req.route_params.get("functionName")
    if function_name is None:
        return func.HttpResponse(
            status_code=400, body="Function name is missing in request URL."
        )

    notebook_path = req.params.get("notebook_path")
    logging.debug("Notebook path %s", notebook_path)
    if notebook_path is None:
        return func.HttpResponse(
            status_code=400,
            body="Please specify the blob storage path to the notebook that you want to run, e.g. notebook_path?my-folder/test.ipynb. The file has to be in the Blob storage container /jupyter-notebooks.",
        )
    try:
        req_body = req.get_json()
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

    notebook = yield context.call_activity("get_notebook_from_blob_path", notebook_path)
    output = yield context.call_activity(
        "execute_notebook",
        {"notebook": notebook, "data": json.dumps(data, indent=4)},
    )
    result_value = yield context.call_activity("get_result_value", output)
    return result_value


@app.activity_trigger(input_name="path")
def get_notebook_from_blob_path(path: str):
    connection_string = os.environ["BLOB_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("jupyter-notebooks")

    blob_list = container_client.list_blobs(name_starts_with=path)
    logging.info("blob list: %s", blob_list)
    try:
        blob = blob_list.next()
        logging.info("blob name: %s", blob.name)
    except:
        raise ValueError(f"Blob {path} does not exist.")
    data = container_client.download_blob(blob.name).readall()
    notebook = data.decode("utf-8")
    return notebook


class ExecuteParams(TypedDict):
    notebook: str
    data: str


@app.activity_trigger(input_name="params")
def execute_notebook(params: ExecuteParams):
    jupyter_node_as_str = params.get("notebook")
    json_data = params.get("data")
    if jupyter_node_as_str is None or json_data is None:
        raise KeyError(f"Expected notebook and data, got {params}")
    notebook = nbformat.reads(jupyter_node_as_str, as_version=nbformat.current_nbformat)
    data = json.loads(json_data)
    logging.debug("notebook input\n\n%s", json.dumps(notebook, indent=4))
    logging.debug("data input\n\n%s", json.dumps(data, indent=4))
    notebook_output = pm.execute_notebook(
        input_path=notebook,
        output_path=None,
        parameters={"params": json.dumps({"data": data}, indent=4)},
        kernel_name=None,  # kernelspec is defined in notebook metadata
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

    def extract_result_value(cell):
        output = cell["outputs"][0]  # we only extract the first output
        data = output["data"]["text/plain"].replace(
            "'", ""
        )  # the result value is wrapped in single quotes
        return json.loads(data)

    # there should always be exactly one value
    papermill_result_cell = list(filter(filter_by_result_value_tag, cells))[0]
    result_value = extract_result_value(papermill_result_cell)
    logging.info("notebook return value\n\n%s", json.dumps(result_value, indent=4))
    return result_value
