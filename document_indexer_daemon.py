#!/usr/bin/env python3
"""Document indexer daemon service."""

import os
import sys
import signal
import argparse
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))


def handle_signal(signum, frame):
    """Handle shutdown signals."""
    print(f"\nReceived signal {signum}, shutting down gracefully...")
    sys.exit(0)


def main():
    """Main daemon entry point."""
    parser = argparse.ArgumentParser(
        description="Document Indexer Daemon - FastAPI service for document search"
    )
    
    parser.add_argument(
        '--host', 
        default='0.0.0.0', 
        help='Host to bind to (default: 0.0.0.0)'
    )
    parser.add_argument(
        '--port', 
        type=int, 
        default=8000, 
        help='Port to bind to (default: 8000)'
    )
    parser.add_argument(
        '--workers', 
        type=int, 
        default=1, 
        help='Number of worker processes (default: 1)'
    )
    parser.add_argument(
        '--reload', 
        action='store_true', 
        help='Enable auto-reload for development'
    )
    parser.add_argument(
        '--log-level', 
        default='info', 
        choices=['debug', 'info', 'warning', 'error'],
        help='Log level (default: info)'
    )
    parser.add_argument(
        '--access-log', 
        action='store_true', 
        help='Enable access logging'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Check required environment variables
    if not os.getenv("TURBOPUFFER_API_KEY"):
        print("Error: TURBOPUFFER_API_KEY environment variable is required")
        sys.exit(1)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    print(f"Starting Document Indexer API daemon...")
    print(f"Host: {args.host}:{args.port}")
    print(f"Workers: {args.workers}")
    print(f"Log level: {args.log_level}")
    print(f"GCS Bucket: {os.getenv('GCS_BUCKET_NAME', 'filevine-backup')}")
    print(f"Turbopuffer Region: {os.getenv('TURBOPUFFER_REGION', 'gcp-us-central1')}")
    print("API Documentation available at: http://{}:{}/docs".format(args.host, args.port))
    
    # Configure uvicorn
    config = uvicorn.Config(
        "src.indexing.api:app",
        host=args.host,
        port=args.port,
        workers=args.workers,
        log_level=args.log_level,
        reload=args.reload,
        access_log=args.access_log
    )
    
    server = uvicorn.Server(config)
    
    try:
        server.run()
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()