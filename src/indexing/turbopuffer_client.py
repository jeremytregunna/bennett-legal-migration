"""Turbopuffer client for document indexing."""

import os
from typing import List, Dict, Optional, Any
import turbopuffer
from ..migration.models.config import TurbopufferConfig


class TurbopufferClient:
    """Client for interacting with Turbopuffer search engine."""
    
    def __init__(self, config: TurbopufferConfig):
        self.config = config
        self.client = turbopuffer.Turbopuffer(
            api_key=config.api_key,
            region=config.region
        )
        self.namespace = self.client.namespace("document-index")
    
    def create_index(self) -> None:
        """Create the document index namespace if it doesn't exist."""
        # Namespace is created automatically on first write
        pass
    
    def index_document(self, document_data: Dict[str, Any]) -> None:
        """Index a single document."""
        # Create a dummy vector since we're only doing metadata search for now
        dummy_vector = [0.0] * 768  # Standard embedding dimension
        
        row = {
            'id': document_data['id'],
            'vector': dummy_vector,
            **document_data
        }
        
        schema = {
            'filename': {'type': 'string', 'full_text_search': True},
            'gcs_path': {'type': 'string', 'full_text_search': True}, 
            'project_name': {'type': 'string', 'full_text_search': True},
            'project_id': {'type': 'number'},
            'document_id': {'type': 'number'},
            'gcs_url': {'type': 'string'},
            'public_url': {'type': 'string'}
        }
        
        self.namespace.write(
            upsert_rows=[row],
            schema=schema
        )
    
    def batch_index_documents(self, documents: List[Dict[str, Any]]) -> None:
        """Index multiple documents in a single batch."""
        if not documents:
            return
            
        dummy_vector = [0.0] * 768
        
        rows = []
        for doc in documents:
            row = {
                'id': doc['id'],
                'vector': dummy_vector,
                **doc
            }
            rows.append(row)
        
        schema = {
            'filename': {'type': 'string', 'full_text_search': True},
            'gcs_path': {'type': 'string', 'full_text_search': True}, 
            'project_name': {'type': 'string', 'full_text_search': True},
            'project_id': {'type': 'number'},
            'document_id': {'type': 'number'},
            'gcs_url': {'type': 'string'},
            'public_url': {'type': 'string'}
        }
        
        self.namespace.write(
            upsert_rows=rows,
            schema=schema
        )
    
    def search_by_filename(self, filename: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search documents by filename."""
        results = self.namespace.query(
            top_k=limit,
            filters=("filename", "Like", f"%{filename}%"),
            include_attributes=['filename', 'gcs_path', 'project_name', 'project_id', 'gcs_url']
        )
        return [dict(result) for result in results]
    
    def search_by_project(self, project_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search documents by project name."""
        results = self.namespace.query(
            top_k=limit,
            filters=("project_name", "Like", f"%{project_name}%"),
            include_attributes=['filename', 'gcs_path', 'project_name', 'project_id', 'gcs_url']
        )
        return [dict(result) for result in results]
    
    def search_by_project_id(self, project_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Search documents by project ID."""
        results = self.namespace.query(
            top_k=limit,
            filters=("project_id", "Eq", project_id),
            include_attributes=['filename', 'gcs_path', 'project_name', 'project_id', 'gcs_url']
        )
        return [dict(result) for result in results]
    
    def full_text_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Perform full-text search across filename, path, and project name."""
        results = self.namespace.query(
            top_k=limit,
            rank_by=('filename', 'BM25', query),
            include_attributes=['filename', 'gcs_path', 'project_name', 'project_id', 'gcs_url']
        )
        return [dict(result) for result in results]
    
    def delete_document(self, document_id: str) -> None:
        """Delete a document from the index."""
        self.namespace.delete([document_id])
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get statistics about the index."""
        try:
            # Get a sample to understand index size
            sample_results = self.namespace.query(top_k=1)
            # For now, just return basic info
            return {"status": "active", "sample_count": len(sample_results)}
        except Exception as e:
            return {"status": "error", "error": str(e)}