"""Test configuration loading and validation."""

import os
import tempfile
from pathlib import Path
import pytest

from src.migration.utils.config_loader import (
    load_config_from_env, 
    validate_config, 
    create_sample_env_file
)
from src.migration.models.config import MigrationConfig


def test_load_config_with_minimal_env():
    """Test loading config with minimal required environment variables."""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
        f.write("""
MSSQL_SERVER=test-server
MSSQL_USERNAME=test-user
MSSQL_PASSWORD=test-pass
SUPABASE_URL=https://test.supabase.co
SUPABASE_SERVICE_ROLE_KEY=test-service-key
""")
        temp_file = f.name
    
    try:
        config = load_config_from_env(temp_file)
        
        assert config.mssql.server == "test-server"
        assert config.mssql.username == "test-user"
        assert config.mssql.password == "test-pass"
        assert config.supabase.url == "https://test.supabase.co"
        assert config.supabase.service_role_key == "test-service-key"
        assert config.gcs.project_id == "webapp-466015"  # default
        assert config.batch_size == 1000  # default
        
    finally:
        os.unlink(temp_file)


def test_validate_config_missing_required_fields():
    """Test config validation with missing required fields."""
    
    from src.migration.models.config import MSSQLConfig, SupabaseConfig, GCSConfig
    
    # Create config with missing fields
    config = MigrationConfig(
        mssql=MSSQLConfig(server="", database="test", username="", password=""),
        supabase=SupabaseConfig(url="", key="test", service_role_key=""),
        gcs=GCSConfig()
    )
    
    errors = validate_config(config)
    
    assert "MSSQL_SERVER is required" in errors
    assert "MSSQL_USERNAME is required" in errors
    assert "SUPABASE_URL is required" in errors
    assert "SUPABASE_SERVICE_ROLE_KEY is required" in errors


def test_validate_config_valid():
    """Test config validation with all required fields."""
    
    from src.migration.models.config import MSSQLConfig, SupabaseConfig, GCSConfig
    
    config = MigrationConfig(
        mssql=MSSQLConfig(
            server="test-server", 
            database="test", 
            username="user", 
            password="pass"
        ),
        supabase=SupabaseConfig(
            url="https://test.supabase.co", 
            key="key", 
            service_role_key="service-key"
        ),
        gcs=GCSConfig()
    )
    
    errors = validate_config(config)
    assert len(errors) == 0


def test_create_sample_env_file():
    """Test creating sample environment file."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        sample_file = Path(temp_dir) / "test.env"
        create_sample_env_file(str(sample_file))
        
        assert sample_file.exists()
        
        content = sample_file.read_text()
        assert "MSSQL_SERVER=" in content
        assert "SUPABASE_URL=" in content
        assert "GCS_PROJECT_ID=" in content