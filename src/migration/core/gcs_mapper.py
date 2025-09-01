"""Project-to-GCS path mapping functionality."""

from collections import defaultdict
from typing import Dict, List, Optional
from google.cloud import storage
from ..models.config import GCSConfig
from ..models.migration import ProjectRecord, DocumentRecord


class ProjectGCSMapper:
    """Maps ProjectId to GCS paths with variant handling."""
    
    def __init__(self, config: GCSConfig):
        self.config = config
        self.client = storage.Client(project=config.project_id)
        self.bucket = self.client.bucket(config.bucket_name)
        self.project_path_map: Dict[int, str] = {}
    
    def sanitize_project_name(self, project_name: str) -> str:
        """Sanitize project name for GCS path by replacing invalid characters with underscores."""
        import re
        # Replace one or more consecutive quotes with a single underscore
        sanitized = re.sub(r'"+', '_', project_name)
        # Replace forward slashes (used as date separators) with underscores
        sanitized = sanitized.replace('/', '_')
        return sanitized
        
    def build_project_mapping(
        self, 
        projects: List[ProjectRecord], 
        doc_records: List[DocumentRecord]
    ) -> Dict[int, str]:
        """Create ProjectId -> GCS path mapping with variant handling."""
        
        # Group documents by ProjectId, only counting docs with filenames
        docs_by_project = defaultdict(list)
        docs_with_filenames_by_project = defaultdict(list)
        
        for doc in doc_records:
            if doc.project_id:
                docs_by_project[doc.project_id].append(doc)
                if doc.filename and doc.filename.strip():
                    docs_with_filenames_by_project[doc.project_id].append(doc)
        
        for project in projects:
            project_id = project.id
            project_name = project.project_name
            sanitized_name = self.sanitize_project_name(project_name)
            
            # Only try to map projects that have documents with filenames
            docs_with_files = docs_with_filenames_by_project.get(project_id, [])
            if not docs_with_files:
                continue  # Skip projects with no documents or no filenames
            
            # Generate all possible paths to check (avoid duplicate network requests)
            paths_to_check = [
                # Original name patterns
                f"docs/Bennett Legal/{project_name}",
                # Sanitized name patterns (quotes -> underscores)
                f"docs/Bennett Legal/{sanitized_name}",
                # Numbered variants for original name
                *[f"docs/Bennett Legal/{project_name} ({i})" for i in range(1, 10)],
                # Numbered variants for sanitized name  
                *[f"docs/Bennett Legal/{sanitized_name} ({i})" for i in range(1, 10)],
                # Solar patterns
                f"docs/Bennett Legal/Solar - {project_name}",
                f"docs/Bennett Legal/Solar - {sanitized_name}",
                # Solar - PNC patterns
                f"docs/Bennett Legal/Solar - PNC {project_name}",
                f"docs/Bennett Legal/Solar - PNC {sanitized_name}",
                # Ultimate fallback: mailroom directory
                "docs/Bennett Legal/zzz_mailroom_no_project_assigned",
            ]
            
            # Remove duplicates while preserving order (original name gets priority)
            seen = set()
            unique_paths = []
            for path in paths_to_check:
                if path not in seen:
                    seen.add(path)
                    unique_paths.append(path)
            
            # Check paths in order of preference (excluding mailroom for now)
            best_match_path = None
            mailroom_path = "docs/Bennett Legal/zzz_mailroom_no_project_assigned"
            
            for path in unique_paths:
                if path == mailroom_path:
                    continue  # Skip mailroom in first pass
                if self.gcs_path_exists(path):
                    best_match_path = path
                    break  # Take the first match (highest priority)
            
            # If no match found, check if documents exist in mailroom as ultimate fallback
            if not best_match_path:
                if self.check_documents_in_mailroom(docs_with_files):
                    best_match_path = mailroom_path
            
            if best_match_path:
                self.project_path_map[project_id] = best_match_path
        
        return self.project_path_map
    
    def gcs_path_exists(self, path: str) -> bool:
        """Check if GCS path exists by listing blobs with prefix."""
        try:
            blobs = self.bucket.list_blobs(prefix=path, max_results=1)
            return any(True for _ in blobs)
        except Exception:
            return False
    
    def check_documents_in_mailroom(self, docs_with_files: List) -> bool:
        """Check if any of the project's documents exist in the mailroom directory."""
        mailroom_prefix = "docs/Bennett Legal/zzz_mailroom_no_project_assigned/"
        
        try:
            # Get a sample of filenames to check (first 3 to avoid too many requests)
            sample_files = [doc.filename for doc in docs_with_files[:3] if doc.filename]
            
            if not sample_files:
                return False
            
            # Check if any of the sample files exist in mailroom
            for filename in sample_files:
                blob_path = f"{mailroom_prefix}{filename}"
                blob = self.bucket.blob(blob_path)
                if blob.exists():
                    return True  # Found at least one file in mailroom
            
            return False
        except Exception:
            return False
    
    def generate_document_url(self, project_id: int, filename: str) -> Optional[str]:
        """Generate GCS URL for a document."""
        if project_id not in self.project_path_map:
            return None
            
        gcs_path = f"{self.project_path_map[project_id]}/{filename}"
        return f"gs://{self.config.bucket_name}/{gcs_path}"
    
    def generate_public_url(self, project_id: int, filename: str) -> Optional[str]:
        """Generate public GCS URL for a document."""
        if project_id not in self.project_path_map:
            return None
            
        gcs_path = f"{self.project_path_map[project_id]}/{filename}"
        return f"https://storage.googleapis.com/{self.config.bucket_name}/{gcs_path}"
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about the project mapping."""
        return {
            "total_projects_mapped": len(self.project_path_map),
            "unique_paths": len(set(self.project_path_map.values()))
        }