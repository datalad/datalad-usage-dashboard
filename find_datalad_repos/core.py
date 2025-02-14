from __future__ import annotations
from typing import List
from pydantic import BaseModel, Field
from .gin import GINDataladRepo
from .github import GHDataladRepo
from .osf import OSFDataladRepo


class RepoRecord(BaseModel):
    github: List[GHDataladRepo] = Field(default_factory=list)
    osf: List[OSFDataladRepo] = Field(default_factory=list)
    gin: List[GINDataladRepo] = Field(default_factory=list)
