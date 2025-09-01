"""Query interface for searching indexed documents."""

from typing import List, Dict, Any, Optional
from .turbopuffer_client import TurbopufferClient
from ..migration.models.config import IndexingConfig


class DocumentQueryInterface:
    """High-level interface for searching documents."""
    
    def __init__(self, config: IndexingConfig):
        self.config = config
        self.client = TurbopufferClient(config.turbopuffer)
    
    def search_documents(
        self,
        query: str = "",
        project_name: str = "",
        project_id: Optional[int] = None,
        filename: str = "",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Search documents with multiple filter options."""
        
        # If specific filters are provided, use them
        if project_id is not None:
            return self.client.search_by_project_id(project_id, limit)
        elif project_name:
            return self.client.search_by_project(project_name, limit)
        elif filename:
            return self.client.search_by_filename(filename, limit)
        elif query:
            return self.client.full_text_search(query, limit)
        else:
            # Return recent documents if no filters
            return self.client.namespace.query(
                top_k=limit,
                include_attributes=['filename', 'gcs_path', 'project_name', 'project_id', 'gcs_url']
            )
    
    def get_project_documents(self, project_id: int) -> List[Dict[str, Any]]:
        """Get all documents for a specific project."""
        return self.client.search_by_project_id(project_id, limit=1000)
    
    def get_document_by_filename(self, filename: str) -> List[Dict[str, Any]]:
        """Find documents by exact or partial filename match."""
        return self.client.search_by_filename(filename)
    
    def advanced_search(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Perform advanced search with multiple criteria."""
        # This would allow combining multiple filters
        # For now, implement basic logic
        
        limit = filters.get('limit', 10)
        
        if 'project_id' in filters:
            return self.client.search_by_project_id(filters['project_id'], limit)
        elif 'project_name' in filters:
            return self.client.search_by_project(filters['project_name'], limit)
        elif 'filename' in filters:
            return self.client.search_by_filename(filters['filename'], limit)
        elif 'query' in filters:
            return self.client.full_text_search(filters['query'], limit)
        
        return []
    
    def get_index_statistics(self) -> Dict[str, Any]:
        """Get statistics about the document index."""
        return self.client.get_index_stats()
    
    def format_search_results(self, results: List[Dict[str, Any]]) -> str:
        """Format search results for display."""
        if not results:
            return "No documents found."
        
        formatted = []
        formatted.append(f"Found {len(results)} document(s):\n")
        
        for i, result in enumerate(results, 1):
            formatted.append(f"{i}. {result.get('filename', 'Unknown')}")
            formatted.append(f"   Project: {result.get('project_name', 'Unknown')}")
            formatted.append(f"   Path: {result.get('gcs_path', 'Unknown')}")
            formatted.append(f"   URL: {result.get('gcs_url', 'Unknown')}")
            formatted.append("")
        
        return "\n".join(formatted)