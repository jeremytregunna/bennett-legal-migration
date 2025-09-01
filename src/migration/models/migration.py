"""Migration data models."""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel


class ProjectRecord(BaseModel):
    """Project record model."""
    id: int
    project_name: str
    
    
class DocumentRecord(BaseModel):
    """Document record model."""
    id: int
    project_id: Optional[int]
    filename: Optional[str]
    doc_key: Optional[str]
    size: Optional[int]
    uploader_id: Optional[int]
    upload_date: Optional[datetime]


class MigrationLogEntry(BaseModel):
    """Migration log entry model."""
    doc_id: Optional[str]
    log_type: str  # 'success', 'missing_file', 'migration_error', 'url_update_error'
    message: str
    created_at: datetime = datetime.now()


class MigrationStats(BaseModel):
    """Migration statistics model."""
    total_docs: int = 0
    migrated_docs: int = 0
    pending_docs: int = 0
    files_uploaded: int = 0
    errors: int = 0
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_docs == 0:
            return 0.0
        return (self.migrated_docs / self.total_docs) * 100