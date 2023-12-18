from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from .papermill import PapermillCell, PapermillOutput


class NotebookExecutionParams(BaseModel):
    write_to_sql: bool
    debug: bool = False
    data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    kwargs: Optional[Dict[str, Any]] = None


class NotebookExecutionInput(BaseModel):
    notebook_as_str: str
    execution_params: NotebookExecutionParams = Field(serialization_alias="params")


class FunctionInput(BaseModel):
    path_to_notebook: str
    execution_params: NotebookExecutionParams
