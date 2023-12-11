import logging
import tempfile
import json
from typing import TypedDict

import azure.functions as func
import azure.durable_functions as df
import papermill as pm

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get("functionName")
    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)
    return response


class ExecuteParams(TypedDict):
    file_name: str
    data: str


# Orchestrator
@app.orchestration_trigger(context_name="context")
def notebook_orchestrator(context):
    file_name = yield context.call_activity("get_file_name_from_blob")
    data = yield context.call_activity("get_sql_data")
    output = yield context.call_activity(
        "execute_notebook_from_file", {"file_name": file_name, "data": data}
    )
    return [output]


# Activities
@app.blob_input(
    arg_name="notebook",
    path="jupyter-notebooks/test.ipynb",
    connection="BLOB_CONNECTION_STRING",
)
@app.activity_trigger(input_name="input")
def get_file_name_from_blob(input: str, notebook: str):
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        json.dump(json.loads(notebook), f)
        filename = f.name
    return filename


@app.generic_input_binding(
    arg_name="data",
    type="sql",
    command_text="SELECT * FROM etl.SalesTransactions st WHERE YEAR(st.PostingDate) > 2020",
    connection_string_setting="SQL_CONNECTION_STRING",
)
@app.activity_trigger(input_name="input")
def get_sql_data(input: str, data: func.SqlRowList):
    rows = list(map(lambda r: json.loads(r.to_json()), data))
    return json.dumps(rows)


@app.activity_trigger(input_name="params")
def execute_notebook_from_file(params: ExecuteParams):
    returned_notebook = pm.execute_notebook(
        input_path=params["file_name"],
        output_path=None,
        parameters={"sales_transactions": params["data"]},
        kernel_name="python3",
    )
    return returned_notebook
