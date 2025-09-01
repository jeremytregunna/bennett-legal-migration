#!/usr/bin/env python3
"""Standalone document indexer CLI tool."""

import os
import sys
from pathlib import Path
import argparse
from typing import List, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dotenv import load_dotenv
from src.migration.models.config import GCSConfig, TurbopufferConfig, IndexingConfig
from src.migration.models.migration import ProjectRecord, DocumentRecord
from src.indexing.document_indexer import DocumentIndexer
from src.indexing.query_interface import DocumentQueryInterface


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
        print("Error: TURBOPUFFER_API_KEY environment variable is required")
        sys.exit(1)
    
    return IndexingConfig(
        gcs=gcs_config,
        turbopuffer=turbopuffer_config
    )


def cmd_index_gcs(args):
    """Index documents by scanning GCS bucket directly."""
    config = load_config()
    indexer = DocumentIndexer(config)
    
    print("Indexing documents from GCS bucket scan...")
    results = indexer.scan_and_index_gcs_bucket(batch_size=args.batch_size)
    
    print(f"Indexing completed!")
    print(f"Files indexed: {results['indexed_files']}")


def cmd_index_from_db(args):
    """Index documents from database records (requires migration system)."""
    print("This command requires database connection and migration system setup.")
    print("Use 'index-gcs' command instead to index directly from GCS bucket.")


def cmd_search(args):
    """Search for documents."""
    config = load_config()
    query_interface = DocumentQueryInterface(config)
    
    results = query_interface.search_documents(
        query=args.query or "",
        project_name=args.project or "",
        project_id=args.project_id,
        filename=args.filename or "",
        limit=args.limit
    )
    
    print(query_interface.format_search_results(results))


def cmd_stats(args):
    """Show index statistics."""
    config = load_config()
    query_interface = DocumentQueryInterface(config)
    
    stats = query_interface.get_index_statistics()
    print(f"Index Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Document Indexer - Index and search documents in Google Cloud Storage"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Index GCS command
    index_gcs_parser = subparsers.add_parser('index-gcs', help='Index documents by scanning GCS bucket')
    index_gcs_parser.add_argument(
        '--batch-size', 
        type=int, 
        default=100, 
        help='Batch size for indexing (default: 100)'
    )
    index_gcs_parser.set_defaults(func=cmd_index_gcs)
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for documents')
    search_parser.add_argument('--query', help='Full-text search query')
    search_parser.add_argument('--project', help='Filter by project name')
    search_parser.add_argument('--project-id', type=int, help='Filter by project ID')
    search_parser.add_argument('--filename', help='Filter by filename')
    search_parser.add_argument('--limit', type=int, default=10, help='Maximum results (default: 10)')
    search_parser.set_defaults(func=cmd_search)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show index statistics')
    stats_parser.set_defaults(func=cmd_stats)
    
    # Help command
    help_parser = subparsers.add_parser('help', help='Show help')
    help_parser.set_defaults(func=lambda args: parser.print_help())
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()