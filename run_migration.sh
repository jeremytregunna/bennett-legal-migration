#!/bin/bash

# Secondary Migration Runner
# This script makes it easy to run the Filevine to target schema migration

echo "🚀 Starting Filevine Secondary Migration"
echo "======================================"
echo ""

# Parse command line arguments
DRY_RUN=""
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN="--dry-run"
    echo "🧪 DRY RUN MODE - No data will be saved"
    echo ""
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "Please create a .env file based on .env.example with your configuration"
    exit 1
fi

# GCS will use application default credentials from environment
echo "✅ Using GCS application default credentials from environment"

echo ""
if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "Running migration simulation..."
    echo "This will analyze what would be migrated and report any potential errors"
else
    echo "Starting secondary migration..."
    echo "This will migrate data from public schema to org_31rnd2vorzuy4fszlzncmcgu5bi"
fi
echo ""

# Run the migration
uv run python -m src.migration.cli schema-migrate $DRY_RUN

echo ""
if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "Dry run completed! Review the summary above."
    echo "To run the actual migration, use: $0"
else
    echo "Migration completed! Check the output above for results."
fi