"""Document URL generation and database updating functionality."""

import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from google.cloud import storage
from supabase import Client
from tqdm import tqdm
from asyncio_throttle import Throttler

from ..models.config import MigrationConfig
from ..models.migration import MigrationStats, MigrationLogEntry


class DocumentURLUpdater:
    """Updates document records with GCS URLs (no file transfers needed)."""
    
    def __init__(self, config: MigrationConfig, project_gcs_map: Dict[int, str]):
        self.config = config
        self.supabase: Client = Client(config.supabase.url, config.supabase.service_role_key)
        self.project_gcs_map = project_gcs_map  # ProjectId -> GCS path mapping
        self.bucket_name = config.gcs.bucket_name
        self.gcs_project = config.gcs.project_id
        self.throttler = Throttler(rate_limit=config.max_concurrent, period=1.0)
        self.stats = MigrationStats()
    
    async def update_all_document_urls(self) -> MigrationStats:
        """Update all document records with GCS URLs (no file transfers needed)."""
        
        print("Starting document URL updates")
        
        if self.config.dry_run:
            print("DRY RUN: Would update document URLs")
            return self.stats
        
        try:
            # Get all doc records from Supabase (without join)
            doc_records = self.supabase.table("doc").select("*").execute()
            
            if not doc_records.data:
                print("No documents found to update")
                return self.stats
            
            self.stats.total_docs = len(doc_records.data)
            print(f"Found {self.stats.total_docs} documents to process")
            
            # Process documents with progress bar
            with tqdm(total=self.stats.total_docs, desc="Updating document URLs") as pbar:
                for doc in doc_records.data:
                    async with self.throttler:
                        await self.update_single_document_url(doc)
                    pbar.update(1)
            
            print("Document URL updates completed")
            print(f"Total documents: {self.stats.total_docs}")
            print(f"Successfully updated: {self.stats.migrated_docs}")
            print(f"Errors: {self.stats.errors}")
            
        except Exception as e:
            error_msg = f"Error during document URL update: {e}"
            print(error_msg)
            self.stats.errors += 1
            await self.log_update_error(None, error_msg)
        
        return self.stats
    
    async def update_single_document_url(self, doc_record: Dict) -> None:
        """Generate GCS URL for existing file and update database."""
        
        # Handle both possible column name formats (id vs ID)
        doc_id = doc_record.get('id') or doc_record.get('ID')
        filename = doc_record.get('filename') or doc_record.get('Filename')
        project_id = doc_record.get('projectid') or doc_record.get('ProjectID') or doc_record.get('project_id')
        
        if not filename or not project_id:
            await self.log_update_error(
                doc_id, 
                f"Missing filename ({filename}) or project_id ({project_id})"
            )
            self.stats.errors += 1
            return
        
        # Get GCS path from ProjectId mapping
        if project_id not in self.project_gcs_map:
            await self.log_update_error(
                doc_id, 
                f"No GCS path mapping for project_id {project_id}"
            )
            self.stats.errors += 1
            return
            
        # Generate GCS URL - files already exist in bucket
        gcs_path = f"{self.project_gcs_map[project_id]}/{filename}"
        gcs_url = f"gs://{self.bucket_name}/{gcs_path}"
        public_url = f"https://storage.googleapis.com/{self.bucket_name}/{gcs_path}"
        
        try:
            # Update database record with GCS URLs
            update_result = self.supabase.table("doc").update({
                'gcs_url': gcs_url,
                'gcs_public_url': public_url,
                'gcs_path': gcs_path,
                'is_migrated': True,
                'migration_date': datetime.now().isoformat()
            }).eq('id', doc_id).execute()
            
            if update_result.data:
                self.stats.migrated_docs += 1
            else:
                error_msg = f"Failed to update database for document {doc_id}"
                print(error_msg)
                await self.log_update_error(doc_id, error_msg)
                self.stats.errors += 1
                
        except Exception as e:
            error_msg = f"Error updating document {doc_id}: {e}"
            print(error_msg)
            await self.log_update_error(doc_id, error_msg)
            self.stats.errors += 1
    
    async def update_documents_for_project(self, project_id: int) -> int:
        """Update document URLs for a specific project."""
        
        try:
            doc_records = self.supabase.table("doc").select("*").eq(
                'project_id', project_id
            ).execute()
            
            if not doc_records.data:
                print(f"No documents found for project {project_id}")
                return 0
            
            updated_count = 0
            for doc in doc_records.data:
                await self.update_single_document_url(doc)
                updated_count += 1
            
            return updated_count
            
        except Exception as e:
            error_msg = f"Error updating documents for project {project_id}: {e}"
            print(error_msg)
            await self.log_update_error(None, error_msg)
            return 0
    
    async def verify_gcs_file_exists(self, gcs_path: str) -> bool:
        """Optional: Verify file exists in GCS bucket before updating URL."""
        try:
            client = storage.Client(project=self.gcs_project)
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)
            return blob.exists()
        except Exception:
            return False
    
    async def get_migration_progress(self) -> Dict:
        """Get migration progress from database."""
        try:
            result = self.supabase.table("doc").select(
                "id, is_migrated, gcs_url"
            ).execute()
            
            if not result.data:
                return {
                    "total_docs": 0,
                    "migrated_docs": 0,
                    "pending_docs": 0,
                    "completion_percentage": 0.0
                }
            
            total_docs = len(result.data)
            migrated_docs = sum(1 for doc in result.data if doc.get('is_migrated'))
            pending_docs = total_docs - migrated_docs
            
            return {
                "total_docs": total_docs,
                "migrated_docs": migrated_docs,
                "pending_docs": pending_docs,
                "completion_percentage": (migrated_docs / total_docs) * 100 if total_docs > 0 else 0.0
            }
            
        except Exception as e:
            print(f"Error getting migration progress: {e}")
            return {}
    
    async def log_update_error(self, doc_id: Optional[str], error_message: str) -> None:
        """Log URL update errors."""
        if self.config.dry_run:
            print(f"DRY RUN: Would log error for doc {doc_id}: {error_message}")
            return
            
        try:
            self.supabase.table("migration_log").insert({
                'doc_id': doc_id,
                'log_type': 'url_update_error',
                'message': error_message,
                'created_at': datetime.now().isoformat()
            }).execute()
            
        except Exception as e:
            print(f"Failed to log update error: {e}")
    
    def get_stats(self) -> MigrationStats:
        """Get current update statistics."""
        return self.stats