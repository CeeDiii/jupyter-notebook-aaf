import logging
import os
from typing import List

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
import nbformat
import papermill as pm

from models import (
    NotebookExecutionInput,
    NotebookExecutionParams,
    FunctionInput,
    PapermillCell,
    PapermillOutput,
)

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

DEFAULT_CONTAINER_PATH = "jupyter-notebooks"


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    """
    Function: http_start

    An HTTP-triggered function that starts a new durable orchestration for executing a Jupyter notebook.

    Parameters:
        - req (func.HttpRequest): HTTP request object.
        - client (df.DurableOrchestrationClient): Durable Functions orchestration client.

    Returns:
        - func.HttpResponse: HTTP response indicating the status of the orchestration initiation.

    Raises:
        - ValueError: If the function name or notebook path is missing in the request.
        - ValueError: If the request body is not a valid JSON.
    """
    function_name = req.route_params.get("functionName")
    if function_name is None:
        return func.HttpResponse(
            status_code=400, body="Function name is missing in request URL."
        )

    notebook_path = req.params.get("notebook_path")
    logging.info(
        "Trying to execute notebook from path '%s/%s'",
        DEFAULT_CONTAINER_PATH,
        notebook_path,
    )
    if notebook_path is None:
        return func.HttpResponse(
            status_code=400,
            body="Please specify the blob storage path to the notebook that you want to run, e.g. notebook_path?my-folder/test.ipynb. The file has to be in the Blob storage container /jupyter-notebooks.",
        )
    try:
        req_body = req.get_json()
        notebook_execution_params = NotebookExecutionParams.model_validate(req_body)
        logging.info(
            "User passed parameters:\n\n%s",
            notebook_execution_params.model_dump_json(indent=4),
        )
    except ValueError as err:
        logging.error(err)
        return func.HttpResponse(
            status_code=400,
            body="Invalid body. Body could not be parsed to json.",
        )

    notebook_params = FunctionInput(
        path_to_notebook=notebook_path, execution_params=notebook_execution_params
    )

    instance_id = await client.start_new(
        function_name,
        client_input=notebook_params.model_dump_json(indent=4),
    )
    response = client.create_check_status_response(req, instance_id)
    return response


# Orchestrator
@app.orchestration_trigger(context_name="context", orchestration="execute_notebook")
def notebook_orchestrator(context: df.DurableOrchestrationContext):
    """
    Function: notebook_orchestrator

    Orchestrator function that coordinates the execution of a Jupyter notebook.

    Parameters:
        - context (df.DurableOrchestrationContext): Durable Functions orchestration context.

    Returns:
        - Any: Result value extracted from the executed Jupyter notebook.

    Raises:
        - ValueError: If no input is passed to the orchestrator.
    """
    client_input = context.get_input()
    if client_input is None:
        raise ValueError("No input was passed to the orchestrator.")
    notebook_params = FunctionInput.model_validate_json(client_input)
    execution_params = notebook_params.execution_params

    notebook = yield context.call_activity(
        "get_notebook_from_blob_path", notebook_params.path_to_notebook
    )

    notebook_execution_input = NotebookExecutionInput(
        notebook_as_str=notebook, execution_params=execution_params
    )

    output = yield context.call_activity(
        "execute_notebook",
        notebook_execution_input.model_dump_json(),
    )

    result_value = yield context.call_activity("get_result_value", output)
    return result_value


@app.activity_trigger(input_name="path")
def get_notebook_from_blob_path(path: str):
    """
    Function: get_notebook_from_blob_path

    Retrieves a Jupyter notebook from Azure Blob Storage given its path.

    Parameters:
        - path (str): Path to the Jupyter notebook in Azure Blob Storage.

    Returns:
        - str: Content of the Jupyter notebook.

    Raises:
        - ValueError: If the specified blob path does not exist in Azure Blob Storage.
    """
    connection_string = os.environ["BLOB_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(DEFAULT_CONTAINER_PATH)

    blob_list = container_client.list_blobs(name_starts_with=path)
    try:
        blob = blob_list.next()
    except:
        raise ValueError(f"Blob {path} does not exist.")
    data = container_client.download_blob(blob.name).readall()
    notebook = data.decode("utf-8")
    return notebook


@app.activity_trigger(input_name="notebookExecutionInput")
def execute_notebook(notebookExecutionInput: str):
    """
    Function: execute_notebook

    Executes a Jupyter notebook using Papermill and returns the execution output.

    Parameters:
        - notebookExecutionInput (str): JSON representation of the notebook

    Returns:
        - str: JSON representation of the execution output.

    Raises:
        - ValueError: If the input JSON is not valid or lacks required properties.
    """
    execution_input = NotebookExecutionInput.model_validate_json(notebookExecutionInput)
    notebook = nbformat.reads(
        execution_input.notebook_as_str, as_version=nbformat.current_nbformat
    )
    notebook_output = pm.execute_notebook(
        input_path=notebook,
        output_path=None,
        parameters=execution_input.execution_params.model_dump(),
        kernel_name=None,  # kernelspec is defined in notebook metadata
    )
    output = PapermillOutput.model_validate(notebook_output)
    return output.model_dump_json(indent=4, by_alias=True)


@app.activity_trigger(input_name="nboutput")
def get_result_value(nboutput: str):
    """
    Function: get_result_value

    Extracts the result value from the execution output of a Jupyter notebook.

    Parameters:
        - nboutput (str): JSON representation of PapermillOutput.

    Returns:
        - str: Extracted result value.

    Raises:
        - ValueError: If the input JSON is not valid or lacks required properties.
        - ValueError: If the 'return_value' tag is not found in the notebook cell metadata.
    """
    data = PapermillOutput.model_validate_json(nboutput)
    cells = data.cells

    def filter_by_result_value_tag(cells: List[PapermillCell]):
        """
        search for the cell in the output notebook that has a tag 'return_value' and return it if found.
        raise a ValueError if it does not exist.
        """
        for cell in cells:
            metadata = cell.metadata
            tags = metadata.tags
            if tags is not None and len(tags) > 0 and "return_value" in tags:
                return cell
        raise ValueError(
            "Could not find a cell that is parametrized as return_value. Please check your notebook cell metadata."
        )

    def extract_result_value(result_value_cell: PapermillCell):
        """
        extract the result value from the cell with the 'result_value' tag.
        """
        outputs = result_value_cell.outputs
        if outputs is not None and len(outputs) > 0:
            output = outputs[0]  # we only extract the first output
            data = output.data.text_plain
            return data
        else:
            return { "message": "No output returned. Check your parameters if you expected a result."}

    # there should always be exactly one value
    papermill_result_cell = filter_by_result_value_tag(cells)
    result_value = extract_result_value(papermill_result_cell)
    return result_value
