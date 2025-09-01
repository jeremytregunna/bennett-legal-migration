"""Configuration models for migration system."""

from typing import Optional
from pydantic import BaseModel, Field


class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    host: str
    port: int
    database: str
    username: str
    password: str
    
    def connection_string(self) -> str:
        """Generate connection string for the database."""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.username} password={self.password}"


class MSSQLConfig(BaseModel):
    """MSSQL connection configuration."""
    server: str
    database: str
    username: str
    password: str
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_server_certificate: bool = True
    
    def connection_string(self) -> str:
        """Generate ODBC connection string."""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'};"
        )


class SupabaseConfig(BaseModel):
    """Supabase configuration."""
    url: str
    key: str
    service_role_key: str


class GCSConfig(BaseModel):
    """Google Cloud Storage configuration."""
    project_id: str = "dataengineerng"
    bucket_name: str = "bennett_bucket1"
    credentials_path: Optional[str] = None


class TurbopufferConfig(BaseModel):
    """Turbopuffer configuration."""
    api_key: str
    region: str = "gcp-us-central1"


class MigrationConfig(BaseModel):
    """Complete migration configuration."""
    mssql: MSSQLConfig
    supabase: SupabaseConfig
    gcs: GCSConfig
    batch_size: int = Field(default=1000, description="Batch size for data migration")
    max_concurrent: int = Field(default=5, description="Maximum concurrent operations")
    dry_run: bool = Field(default=False, description="Run in dry-run mode without making changes")


class IndexingConfig(BaseModel):
    """Document indexing configuration."""
    gcs: GCSConfig
    turbopuffer: TurbopufferConfig