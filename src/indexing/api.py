"""FastAPI web service for document indexing and search."""

import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.migration.models.config import GCSConfig, TurbopufferConfig, IndexingConfig
from src.indexing.document_indexer import DocumentIndexer
from src.indexing.query_interface import DocumentQueryInterface
from src.indexing.models import (
    SearchRequest, SearchResponse, DocumentResult,
    IndexStats, IndexRequest, IndexResponse
)


# Global instances
indexer: DocumentIndexer = None
query_interface: DocumentQueryInterface = None


def load_config() -> IndexingConfig:
    """Load configuration from environment variables."""
    load_dotenv()
    
    gcs_config = GCSConfig(
        project_id=os.getenv("GCS_PROJECT_ID", "webapp-466015"),
        bucket_name=os.getenv("GCS_BUCKET_NAME", "filevine-backup"),
        credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    
    turbopuffer_config = TurbopufferConfig(
        api_key=os.getenv("TURBOPUFFER_API_KEY", ""),
        region=os.getenv("TURBOPUFFER_REGION", "gcp-us-central1")
    )
    
    if not turbopuffer_config.api_key:
        raise ValueError("TURBOPUFFER_API_KEY environment variable is required")
    
    return IndexingConfig(
        gcs=gcs_config,
        turbopuffer=turbopuffer_config
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup."""
    global indexer, query_interface
    
    try:
        config = load_config()
        indexer = DocumentIndexer(config)
        query_interface = DocumentQueryInterface(config)
        print(f"Document indexer service initialized")
        print(f"GCS Bucket: {config.gcs.bucket_name}")
        print(f"Turbopuffer Region: {config.turbopuffer.region}")
    except Exception as e:
        print(f"Failed to initialize service: {e}")
        raise
    
    yield
    
    # Cleanup on shutdown
    indexer = None
    query_interface = None


app = FastAPI(
    title="Document Indexer API",
    description="Search and index documents stored in Google Cloud Storage",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware for web access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint with service information."""
    return {
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


@app.post("/search", response_model=SearchResponse)
async def search_documents(request: SearchRequest):
    """Search for documents with various filter options."""
    try:
        results = query_interface.search_documents(
            query=request.query or "",
            project_name=request.project_name or "",
            project_id=request.project_id,
            filename=request.filename or "",
            limit=request.limit
        )
        
        # Convert results to proper format
        document_results = []
        for result in results:
            document_results.append(DocumentResult(
                id=result.get('id', ''),
                filename=result.get('filename', ''),
                gcs_path=result.get('gcs_path', ''),
                project_name=result.get('project_name', ''),
                project_id=result.get('project_id', 0),
                gcs_url=result.get('gcs_url', ''),
                public_url=result.get('public_url', '')
            ))
        
        query_info = {
            "query": request.query,
            "project_name": request.project_name,
            "project_id": request.project_id,
            "filename": request.filename,
            "limit": request.limit
        }
        
        return SearchResponse(
            results=document_results,
            total_found=len(document_results),
            query_info=query_info
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/search/project/{project_id}")
async def search_by_project_id(project_id: int, limit: int = 10):
    """Search documents by project ID."""
    try:
        results = query_interface.get_project_documents(project_id)
        limited_results = results[:limit]
        
        document_results = []
        for result in limited_results:
            document_results.append(DocumentResult(
                id=result.get('id', ''),
                filename=result.get('filename', ''),
                gcs_path=result.get('gcs_path', ''),
                project_name=result.get('project_name', ''),
                project_id=result.get('project_id', 0),
                gcs_url=result.get('gcs_url', ''),
                public_url=result.get('public_url', '')
            ))
        
        return SearchResponse(
            results=document_results,
            total_found=len(document_results),
            query_info={"project_id": project_id, "limit": limit}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/stats", response_model=IndexStats)
async def get_index_stats():
    """Get index statistics."""
    try:
        stats = query_interface.get_index_statistics()
        return IndexStats(**stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@app.post("/index", response_model=IndexResponse)
async def trigger_indexing(request: IndexRequest, background_tasks: BackgroundTasks):
    """Trigger indexing of GCS bucket (runs in background)."""
    try:
        def run_indexing():
            try:
                results = indexer.scan_and_index_gcs_bucket(batch_size=request.batch_size)
                print(f"Indexing completed: {results}")
            except Exception as e:
                print(f"Indexing failed: {e}")
        
        background_tasks.add_task(run_indexing)
        
        return IndexResponse(
            status="started",
            indexed_files=0,
            message=f"Indexing started in background with batch size {request.batch_size}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start indexing: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Quick health check by getting stats
        stats = query_interface.get_index_statistics()
        return {"status": "healthy", "index_status": stats.get("status", "unknown")}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}