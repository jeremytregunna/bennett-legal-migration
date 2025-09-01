"""Test GCS mapper functionality."""

import pytest
from unittest.mock import Mock, patch

from src.migration.core.gcs_mapper import ProjectGCSMapper
from src.migration.models.config import GCSConfig
from src.migration.models.migration import ProjectRecord, DocumentRecord


@pytest.fixture
def gcs_config():
    """Create GCS config for testing."""
    return GCSConfig(
        project_id="test-project",
        bucket_name="test-bucket"
    )


@pytest.fixture
def mock_projects():
    """Create mock project records."""
    return [
        ProjectRecord(id=1, project_name="Project A"),
        ProjectRecord(id=2, project_name="Project B"),
        ProjectRecord(id=3, project_name="Project C"),
    ]


@pytest.fixture
def mock_documents():
    """Create mock document records."""
    return [
        DocumentRecord(id=1, project_id=1, filename="doc1.pdf"),
        DocumentRecord(id=2, project_id=1, filename="doc2.pdf"),
        DocumentRecord(id=3, project_id=2, filename="doc3.pdf"),
        DocumentRecord(id=4, project_id=None, filename="orphan.pdf"),  # No project
    ]


@patch('src.migration.core.gcs_mapper.storage.Client')
def test_gcs_path_exists_true(mock_storage_client, gcs_config):
    """Test GCS path existence check returns True."""
    
    # Mock the storage client and bucket
    mock_client = Mock()
    mock_bucket = Mock()
    mock_blobs = [Mock()]  # Non-empty iterator
    
    mock_storage_client.return_value = mock_client
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = mock_blobs
    
    mapper = ProjectGCSMapper(gcs_config)
    
    result = mapper.gcs_path_exists("test/path")
    
    assert result is True
    mock_bucket.list_blobs.assert_called_once_with(prefix="test/path", max_results=1)


@patch('src.migration.core.gcs_mapper.storage.Client')
def test_gcs_path_exists_false(mock_storage_client, gcs_config):
    """Test GCS path existence check returns False."""
    
    # Mock empty result
    mock_client = Mock()
    mock_bucket = Mock()
    mock_bucket.list_blobs.return_value = []  # Empty iterator
    
    mock_storage_client.return_value = mock_client
    mock_client.bucket.return_value = mock_bucket
    
    mapper = ProjectGCSMapper(gcs_config)
    
    result = mapper.gcs_path_exists("test/path")
    
    assert result is False


@patch('src.migration.core.gcs_mapper.storage.Client')
def test_generate_document_url(mock_storage_client, gcs_config, mock_projects):
    """Test document URL generation."""
    
    mapper = ProjectGCSMapper(gcs_config)
    mapper.project_path_map = {1: "docs/Bennett Legal/Project A"}
    
    url = mapper.generate_document_url(1, "test.pdf")
    
    expected = "gs://test-bucket/docs/Bennett Legal/Project A/test.pdf"
    assert url == expected


@patch('src.migration.core.gcs_mapper.storage.Client')
def test_generate_document_url_no_mapping(mock_storage_client, gcs_config):
    """Test document URL generation with no project mapping."""
    
    mapper = ProjectGCSMapper(gcs_config)
    
    url = mapper.generate_document_url(999, "test.pdf")
    
    assert url is None


@patch('src.migration.core.gcs_mapper.storage.Client')
def test_generate_public_url(mock_storage_client, gcs_config):
    """Test public URL generation."""
    
    mapper = ProjectGCSMapper(gcs_config)
    mapper.project_path_map = {1: "docs/Bennett Legal/Project A"}
    
    url = mapper.generate_public_url(1, "test.pdf")
    
    expected = "https://storage.googleapis.com/test-bucket/docs/Bennett Legal/Project A/test.pdf"
    assert url == expected


@patch('src.migration.core.gcs_mapper.storage.Client')
def test_get_mapping_stats(mock_storage_client, gcs_config):
    """Test mapping statistics."""
    
    mapper = ProjectGCSMapper(gcs_config)
    mapper.project_path_map = {
        1: "docs/Bennett Legal/Project A",
        2: "docs/Bennett Legal/Project B",
        3: "docs/Bennett Legal/Project A"  # Same path as project 1
    }
    
    stats = mapper.get_mapping_stats()
    
    assert stats["total_projects_mapped"] == 3
    assert stats["unique_paths"] == 2  # Only 2 unique paths