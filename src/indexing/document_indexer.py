"""Document indexer that integrates with existing GCS mapping system."""

from typing import List, Dict, Any, Optional
from tqdm import tqdm
from google.cloud import storage

from ..migration.models.config import IndexingConfig  
from ..migration.core.gcs_mapper import ProjectGCSMapper
from ..migration.models.migration import ProjectRecord, DocumentRecord
from .turbopuffer_client import TurbopufferClient


class DocumentIndexer:
    """Indexes document metadata from GCS using existing project mapping."""
    
    def __init__(self, config: IndexingConfig):
        self.config = config
        self.gcs_mapper = ProjectGCSMapper(config.gcs)
        self.turbopuffer_client = TurbopufferClient(config.turbopuffer)
        self.gcs_client = storage.Client(project=config.gcs.project_id)
        self.bucket = self.gcs_client.bucket(config.gcs.bucket_name)
    
    def index_existing_documents(
        self, 
        projects: List[ProjectRecord], 
        documents: List[DocumentRecord],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """Index all existing documents from project mapping."""
        
        print("Building project-to-GCS path mapping...")
        project_path_map = self.gcs_mapper.build_project_mapping(projects, documents)
        
        print(f"Found {len(project_path_map)} projects with GCS paths")
        
        # Create project lookup for metadata
        project_lookup = {p.id: p for p in projects}
        
        indexed_count = 0
        skipped_count = 0
        batch_documents = []
        
        print("Indexing documents...")
        for document in tqdm(documents, desc="Processing documents"):
            if not document.project_id or document.project_id not in project_path_map:
                skipped_count += 1
                continue
                
            if not document.filename or not document.filename.strip():
                skipped_count += 1
                continue
            
            project = project_lookup.get(document.project_id)
            if not project:
                skipped_count += 1
                continue
            
            # Generate document metadata for indexing
            doc_data = self._create_document_data(document, project, project_path_map)
            if doc_data:
                batch_documents.append(doc_data)
                indexed_count += 1
                
                # Process batch when full
                if len(batch_documents) >= batch_size:
                    self.turbopuffer_client.batch_index_documents(batch_documents)
                    batch_documents = []
        
        # Process remaining documents in final batch
        if batch_documents:
            self.turbopuffer_client.batch_index_documents(batch_documents)
        
        return {
            "indexed_documents": indexed_count,
            "skipped_documents": skipped_count,
            "total_projects_mapped": len(project_path_map)
        }
    
    def index_new_document(
        self, 
        document: DocumentRecord, 
        project: ProjectRecord,
        gcs_path: str
    ) -> bool:
        """Index a single new document (for real-time indexing)."""
        
        doc_data = self._create_document_data_with_path(document, project, gcs_path)
        if doc_data:
            self.turbopuffer_client.index_document(doc_data)
            return True
        return False
    
    def scan_and_index_gcs_bucket(self, batch_size: int = 100) -> Dict[str, Any]:
        """Scan GCS bucket directly and index all files found."""
        
        print(f"Scanning GCS bucket: {self.config.gcs.bucket_name}")
        
        indexed_count = 0
        batch_documents = []
        
        # Scan all blobs in the docs/ directory
        blobs = self.bucket.list_blobs(prefix="docs/")
        
        for blob in tqdm(blobs, desc="Scanning GCS files"):
            # Skip directories/folders
            if blob.name.endswith('/'):
                continue
                
            doc_data = self._create_document_data_from_blob(blob)
            if doc_data:
                batch_documents.append(doc_data)
                indexed_count += 1
                
                # Process batch when full
                if len(batch_documents) >= batch_size:
                    self.turbopuffer_client.batch_index_documents(batch_documents)
                    batch_documents = []
        
        # Process remaining documents
        if batch_documents:
            self.turbopuffer_client.batch_index_documents(batch_documents)
        
        return {
            "indexed_files": indexed_count,
            "source": "gcs_bucket_scan"
        }
    
    def _create_document_data(
        self, 
        document: DocumentRecord, 
        project: ProjectRecord,
        project_path_map: Dict[int, str]
    ) -> Optional[Dict[str, Any]]:
        """Create document data for indexing from database records."""
        
        if document.project_id not in project_path_map:
            return None
            
        gcs_path = project_path_map[document.project_id]
        return self._create_document_data_with_path(document, project, gcs_path)
    
    def _create_document_data_with_path(
        self, 
        document: DocumentRecord, 
        project: ProjectRecord,
        gcs_path: str
    ) -> Optional[Dict[str, Any]]:
        """Create document data for indexing with explicit GCS path."""
        
        if not document.filename or not document.filename.strip():
            return None
        
        full_gcs_path = f"{gcs_path}/{document.filename}"
        gcs_url = f"gs://{self.config.gcs.bucket_name}/{full_gcs_path}"
        public_url = f"https://storage.googleapis.com/{self.config.gcs.bucket_name}/{full_gcs_path}"
        
        return {
            'id': f"doc_{document.id}",
            'filename': document.filename,
            'gcs_path': full_gcs_path,
            'project_name': project.project_name,
            'project_id': document.project_id,
            'document_id': document.id,
            'gcs_url': gcs_url,
            'public_url': public_url
        }
    
    def _create_document_data_from_blob(self, blob: storage.Blob) -> Optional[Dict[str, Any]]:
        """Create document data for indexing from GCS blob."""
        
        # Extract path components
        path_parts = blob.name.split('/')
        if len(path_parts) < 3:  # Need at least docs/Bennett Legal/project/file
            return None
            
        filename = path_parts[-1]
        if not filename:
            return None
            
        # Try to extract project name from path
        project_name = "Unknown"
        if len(path_parts) >= 4 and path_parts[1] == "Bennett Legal":
            project_name = path_parts[2]
        
        gcs_url = f"gs://{self.config.gcs.bucket_name}/{blob.name}"
        public_url = f"https://storage.googleapis.com/{self.config.gcs.bucket_name}/{blob.name}"
        
        return {
            'id': f"gcs_{hash(blob.name) % (10**10)}",  # Generate stable ID from path
            'filename': filename,
            'gcs_path': blob.name,
            'project_name': project_name,
            'project_id': 0,  # Unknown project ID from GCS scan
            'document_id': 0,  # Unknown document ID from GCS scan
            'gcs_url': gcs_url,
            'public_url': public_url
        }
    
    def remove_document_from_index(self, document_id: int) -> bool:
        """Remove a document from the index."""
        try:
            self.turbopuffer_client.delete_document(f"doc_{document_id}")
            return True
        except Exception:
            return False