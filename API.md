# Document Indexer API Reference

## Base URL
```
http://localhost:8000
```

## Authentication
No authentication required for current implementation.

## Endpoints

### Service Information

#### GET /
Get basic service information and available endpoints.

**Response:**
```json
{
  "service": "Document Indexer API",
  "version": "1.0.0",
  "status": "running",
  "endpoints": {
    "search": "/search",
    "stats": "/stats",
    "index": "/index",
    "docs": "/docs"
  }
}
```

### Search Operations

#### POST /search
Search for documents with various filter options.

**Request Body:**
```json
{
  "query": "optional full-text search query",
  "project_name": "optional project name filter",
  "project_id": 123,
  "filename": "optional filename filter",
  "limit": 10
}
```

**Response:**
```json
{
  "results": [
    {
      "id": "doc_123",
      "filename": "contract.pdf",
      "gcs_path": "docs/Bennett Legal/Smith vs Jones/contract.pdf",
      "project_name": "Smith vs Jones",
      "project_id": 456,
      "gcs_url": "gs://filevine-backup/docs/Bennett Legal/Smith vs Jones/contract.pdf",
      "public_url": "https://storage.googleapis.com/filevine-backup/docs/Bennett Legal/Smith vs Jones/contract.pdf"
    }
  ],
  "total_found": 1,
  "query_info": {
    "query": "contract",
    "project_name": null,
    "project_id": null,
    "filename": null,
    "limit": 10
  }
}
```

**Example Requests:**

**Search by filename:**
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"filename": "contract.pdf", "limit": 10}'
```

**Search by project name:**
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"project_name": "Smith vs Jones", "limit": 20}'
```

**Full-text search:**
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"query": "settlement agreement", "limit": 15}'
```

**Search by project ID:**
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"project_id": 12345, "limit": 50}'
```

#### GET /search/project/{project_id}
Get all documents for a specific project.

**Parameters:**
- `project_id` (path): Project ID to search for
- `limit` (query, optional): Maximum results (default: 10)

**Example:**
```bash
curl "http://localhost:8000/search/project/12345?limit=25"
```

**Response:** Same format as POST /search

### Index Management

#### POST /index
Trigger indexing of GCS bucket documents (runs in background).

**Request Body:**
```json
{
  "batch_size": 100
}
```

**Response:**
```json
{
  "status": "started",
  "indexed_files": 0,
  "message": "Indexing started in background with batch size 100"
}
```

**Example:**
```bash
curl -X POST "http://localhost:8000/index" \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 200}'
```

#### GET /stats
Get index statistics.

**Response:**
```json
{
  "status": "active",
  "sample_count": 1,
  "error": null
}
```

**Example:**
```bash
curl "http://localhost:8000/stats"
```

### Health Monitoring

#### GET /health
Health check endpoint for service monitoring.

**Response:**
```json
{
  "status": "healthy",
  "index_status": "active"
}
```

**Example:**
```bash
curl "http://localhost:8000/health"
```

## Interactive Documentation

The API provides automatic interactive documentation:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Complete Indexing and Search Workflow

### 1. Start the Service
```bash
python document_indexer_daemon.py --reload
```

### 2. Trigger Initial Indexing
```bash
# Index all documents in GCS bucket
curl -X POST "http://localhost:8000/index" \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 100}'
```

### 3. Check Indexing Progress
```bash
# Monitor health and stats
curl "http://localhost:8000/health"
curl "http://localhost:8000/stats"
```

### 4. Search Documents
```bash
# Find PDFs
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"filename": ".pdf", "limit": 10}'

# Find documents by case name
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"project_name": "Bennett Legal", "limit": 20}'

# Search document content metadata
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"query": "motion summary judgment", "limit": 15}'
```

## Error Responses

### 500 Internal Server Error
```json
{
  "detail": "Search failed: connection timeout"
}
```

### 422 Validation Error
```json
{
  "detail": [
    {
      "loc": ["body", "limit"],
      "msg": "ensure this value is greater than 0",
      "type": "value_error.number.not_gt",
      "ctx": {"limit_value": 0}
    }
  ]
}
```

## Configuration

The service reads configuration from environment variables:

```bash
# Required
TURBOPUFFER_API_KEY=tpuf_your_api_key_here

# GCS Settings
GCS_PROJECT_ID=webapp-466015
GCS_BUCKET_NAME=filevine-backup
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Optional
TURBOPUFFER_REGION=gcp-us-central1
```

## Rate Limits

No rate limits are currently implemented. Consider implementing rate limiting for production use.

## Data Models

### SearchRequest
- `query` (string, optional): Full-text search query
- `project_name` (string, optional): Project name filter  
- `project_id` (integer, optional): Project ID filter
- `filename` (string, optional): Filename filter
- `limit` (integer, 1-100): Maximum results (default: 10)

### DocumentResult
- `id` (string): Document ID
- `filename` (string): Document filename
- `gcs_path` (string): Full GCS path
- `project_name` (string): Project name
- `project_id` (integer): Project ID
- `gcs_url` (string): GCS URL (gs://)
- `public_url` (string): Public HTTPS URL