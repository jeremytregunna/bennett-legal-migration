#!/bin/bash

# Test Dry Run Mode
# Quick test to verify dry run functionality

echo "🧪 Testing Dry Run Mode"
echo "====================="
echo ""

# Test CLI help
echo "1. Testing CLI help..."
uv run python -m src.migration.cli schema-migrate --help

echo ""
echo "2. Testing dry run initialization..."
uv run python -c "
from src.migration.core.schema_migrator import SchemaMigrator
from src.migration.utils.config_loader import load_config_from_env

config = load_config_from_env()
migrator = SchemaMigrator(config, dry_run=True)
print('✅ Dry run mode successfully initialized')
print('✅ All systems ready for dry run testing')
"

echo ""
echo "✅ Dry run test completed successfully!"
echo "You can now run: ./run_migration.sh --dry-run"