"""Configuration loading utilities."""

import os
from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv

from ..models.config import MigrationConfig, MSSQLConfig, SupabaseConfig, GCSConfig


def load_config_from_env(env_file: Optional[str] = None) -> MigrationConfig:
    """Load migration configuration from environment variables."""
    
    if env_file:
        load_dotenv(env_file)
    else:
        # Try to load from .env file in current directory
        env_path = Path(".env")
        if env_path.exists():
            load_dotenv(env_path)
    
    # MSSQL Configuration
    mssql_config = MSSQLConfig(
        server=os.getenv("MSSQL_SERVER", ""),
        database=os.getenv("MSSQL_DATABASE", "OrgExport_FVtoFV_Migration"),
        username=os.getenv("MSSQL_USERNAME", ""),
        password=os.getenv("MSSQL_PASSWORD", ""),
        driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("MSSQL_TRUST_CERT", "true").lower() == "true"
    )
    
    # Supabase Configuration
    supabase_config = SupabaseConfig(
        url=os.getenv("SUPABASE_URL", ""),
        key=os.getenv("SUPABASE_ANON_KEY", ""),
        service_role_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    )
    
    # GCS Configuration
    gcs_config = GCSConfig(
        project_id=os.getenv("GCS_PROJECT_ID", "dataengineerng"),
        bucket_name=os.getenv("GCS_BUCKET_NAME", "bennett_bucket1"),
        credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    
    # Migration settings
    batch_size = int(os.getenv("MIGRATION_BATCH_SIZE", "1000"))
    max_concurrent = int(os.getenv("MIGRATION_MAX_CONCURRENT", "5"))
    dry_run = os.getenv("MIGRATION_DRY_RUN", "false").lower() == "true"
    
    return MigrationConfig(
        mssql=mssql_config,
        supabase=supabase_config,
        gcs=gcs_config,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        dry_run=dry_run
    )


def create_sample_env_file(file_path: str = ".env.example") -> None:
    """Create a sample environment file with all required variables."""
    
    sample_content = """# MSSQL Database Configuration
MSSQL_SERVER=your-server.database.windows.net
MSSQL_DATABASE=OrgExport_FVtoFV_Migration
MSSQL_USERNAME=your-username
MSSQL_PASSWORD=your-password
MSSQL_DRIVER=ODBC Driver 18 for SQL Server
MSSQL_TRUST_CERT=true

# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# Google Cloud Storage Configuration
GCS_PROJECT_ID=dataengineerng
GCS_BUCKET_NAME=bennett_bucket1
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json

# Migration Settings
MIGRATION_BATCH_SIZE=1000
MIGRATION_MAX_CONCURRENT=5
MIGRATION_DRY_RUN=false
"""
    
    with open(file_path, "w") as f:
        f.write(sample_content)
    
    print(f"Sample environment file created: {file_path}")


def validate_config(config: MigrationConfig) -> List[str]:
    """Validate that all required configuration values are present."""
    
    errors = []
    
    # Check MSSQL config
    if not config.mssql.server:
        errors.append("MSSQL_SERVER is required")
    if not config.mssql.username:
        errors.append("MSSQL_USERNAME is required")
    if not config.mssql.password:
        errors.append("MSSQL_PASSWORD is required")
    
    # Check Supabase config
    if not config.supabase.url:
        errors.append("SUPABASE_URL is required")
    if not config.supabase.service_role_key:
        errors.append("SUPABASE_SERVICE_ROLE_KEY is required")
    
    # Check GCS credentials
    if config.gcs.credentials_path and not Path(config.gcs.credentials_path).exists():
        errors.append(f"GCS credentials file not found: {config.gcs.credentials_path}")
    
    return errors