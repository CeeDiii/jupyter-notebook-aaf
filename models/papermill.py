from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CellMetadata(BaseModel):
    tags: Optional[List[str]] = None
    papermill: Optional[Dict[str, Any]] = None
    execution: Optional[Dict[str, Any]] = None


class PapermillCellData(BaseModel):
    text_plain: str = Field(..., alias="text/plain")

    class Config:
        allow_population_by_field_name = True


class PapermillCellOutput(BaseModel):
    output_type: str
    metadata: CellMetadata
    data: PapermillCellData
    execution_count: Optional[int] = None


class PapermillCell(BaseModel):
    cell_type: str
    execution_count: Optional[int] = None
    metadata: CellMetadata
    outputs: List[PapermillCellOutput]
    source: str
    id: str


class PapermillOutput(BaseModel):
    cells: List[PapermillCell]
    metadata: Dict[str, Any]
    nbformat: int
    nbformat_minor: int
