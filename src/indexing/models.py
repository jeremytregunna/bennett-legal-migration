"""API models for document indexing service."""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    """Request model for document search."""
    query: Optional[str] = Field(None, description="Full-text search query")
    project_name: Optional[str] = Field(None, description="Filter by project name")
    project_id: Optional[int] = Field(None, description="Filter by project ID")
    filename: Optional[str] = Field(None, description="Filter by filename")
    limit: int = Field(10, ge=1, le=100, description="Maximum number of results")


class DocumentResult(BaseModel):
    """Document search result."""
    id: str = Field(description="Document ID")
    filename: str = Field(description="Document filename")
    gcs_path: str = Field(description="Full GCS path")
    project_name: str = Field(description="Project name")
    project_id: int = Field(description="Project ID")
    gcs_url: str = Field(description="GCS URL (gs://)")
    public_url: str = Field(description="Public HTTPS URL")


class SearchResponse(BaseModel):
    """Response model for document search."""
    results: List[DocumentResult] = Field(description="Search results")
    total_found: int = Field(description="Total number of results found")
    query_info: Dict[str, Any] = Field(description="Query information")


class IndexStats(BaseModel):
    """Index statistics."""
    status: str = Field(description="Index status")
    sample_count: Optional[int] = Field(None, description="Sample document count")
    error: Optional[str] = Field(None, description="Error message if any")


class IndexRequest(BaseModel):
    """Request model for indexing documents."""
    batch_size: int = Field(100, ge=1, le=1000, description="Batch size for indexing")


class IndexResponse(BaseModel):
    """Response model for indexing operation."""
    status: str = Field(description="Operation status")
    indexed_files: int = Field(description="Number of files indexed")
    message: str = Field(description="Status message")