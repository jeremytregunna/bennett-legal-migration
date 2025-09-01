#!/bin/bash

# Document Migration Runner
# This script runs only the document migration phase

echo "📁 Starting Document Migration"
echo "=============================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "Please create a .env file based on .env.example with your configuration"
    exit 1
fi

# Export GCS credentials if file exists
if [ -f "gcs-credentials.json" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gcs-credentials.json"
    echo "✅ GCS credentials file found"
else
    echo "⚠️  Warning: gcs-credentials.json not found - file copying will be skipped"
fi

echo ""
echo "Starting document migration only..."
echo "This will create folder structures and migrate document metadata"
echo ""

# Run the document migration
uv run python -m src.migration.cli migrate-documents

echo ""
echo "Document migration completed! Check the output above for results."