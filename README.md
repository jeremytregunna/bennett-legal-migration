# Filevine to Supabase Migration Tool

A comprehensive migration system for transferring data from Filevine MSSQL database to Supabase PostgreSQL with documents stored in Google Cloud Storage.

## Features

- **Complete Data Migration**: Migrates all 719,562 records from 211 MSSQL tables to Supabase
- **GCS Document Integration**: Links existing documents in Google Cloud Storage to database records
- **Document Search API**: FastAPI service for indexing and searching documents using Turbopuffer
- **Project Path Mapping**: Intelligent mapping of ProjectId to GCS folder paths with variant handling
- **Batch Processing**: Configurable batch sizes for efficient data transfer
- **Progress Tracking**: Real-time progress monitoring with detailed logging
- **Dry Run Mode**: Test migrations without making actual changes
- **Modular Design**: Run individual migration phases independently

## Installation

1. Clone the repository and navigate to the project directory
2. Install Microsoft ODBC Driver 18 for SQL Server:

   **Arch Linux:**
   ```bash
   yay -S msodbcsql
   ```
   
   **Ubuntu/Debian:**
   ```bash
   curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
   curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
   sudo apt-get update
   sudo apt-get install -y msodbcsql18
   ```

3. Install Python dependencies using uv:

```bash
uv sync
```

## Configuration

1. Create configuration file from template:

```bash
uv run python -m src.migration.cli init
```

2. Edit `.env.example` and rename to `.env`:

```bash
cp .env.example .env
```

3. Update the configuration with your credentials:

```env
# MSSQL Database Configuration
MSSQL_SERVER=your-server.database.windows.net
MSSQL_DATABASE=OrgExport_FVtoFV_Migration
MSSQL_USERNAME=your-username
MSSQL_PASSWORD=your-password

# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# Google Cloud Storage Configuration
GCS_PROJECT_ID=webapp-466015
GCS_BUCKET_NAME=filevine-backup
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Migration Settings
MIGRATION_BATCH_SIZE=1000
MIGRATION_MAX_CONCURRENT=5
MIGRATION_DRY_RUN=false

# Document Search API Configuration (Optional)
TURBOPUFFER_API_KEY=your-turbopuffer-api-key
TURBOPUFFER_REGION=gcp-us-central1
```

## Usage

This tool provides **three distinct migration workflows**:

### 1. Primary Data Migration (MSSQL → Supabase)

**Complete Migration** (recommended):
```bash
uv run python -m src.migration.cli migrate
```

**Individual Migration Phases**:
```bash
# Analyze Project-to-GCS mapping
uv run python -m src.migration.cli analyze --export-csv

# Data migration only
uv run python -m src.migration.cli data

# Specific tables
uv run python -m src.migration.cli data --tables Project Doc Person

# Document URL updates only  
uv run python -m src.migration.cli urls

# Create tables only
uv run python -m src.migration.cli create-tables

# Retry failed batches
uv run python -m src.migration.cli retry --csv-file failures.csv
```

### 2. Schema Migration (Public → Target Org Schema)

Migrate data from public schema to organization-specific schema:

```bash
# Run schema migration
uv run python -m src.migration.cli schema-migrate

# Or use the convenience script
./run_migration.sh

# Dry run mode
./run_migration.sh --dry-run
```

### 3. Document Migration & Folder Structure

Migrate documents and create proper folder structures:

```bash
# Run document migration
uv run python -m src.migration.cli migrate-documents

# Or use the convenience script  
./run_document_migration.sh

# Dry run mode
uv run python -m src.migration.cli migrate-documents --dry-run
```

### 4. Custom Fields & Notes Migration

Migrate additional data like custom fields and notes:

```bash
# Migrate all extras (custom fields + notes)
uv run python -m src.migration.cli migrate-extras

# Custom fields only
uv run python -m src.migration.cli migrate-extras --skip-notes

# Notes only  
uv run python -m src.migration.cli migrate-extras --skip-custom-fields

# Dry run mode
uv run python -m src.migration.cli migrate-extras --dry-run
```

### 5. Dry Run Mode

Test any migration operation without making changes:

```bash
# Environment variable method
MIGRATION_DRY_RUN=true uv run python -m src.migration.cli migrate

# Command-line flag method (for newer commands)
uv run python -m src.migration.cli schema-migrate --dry-run
uv run python -m src.migration.cli migrate-documents --dry-run
uv run python -m src.migration.cli migrate-extras --dry-run
```

## Document Search API (Independent Tool)

The project includes a **standalone FastAPI service** for indexing and searching documents in Google Cloud Storage using Turbopuffer search engine. This tool is independent of the migration workflows above.

### Quick Start

1. **Configure Turbopuffer** (get API key from https://turbopuffer.com):
```bash
# Add to .env file
TURBOPUFFER_API_KEY=your-turbopuffer-api-key
TURBOPUFFER_REGION=gcp-us-central1
```

2. **Start the Search API**:
```bash
python document_indexer_daemon.py --reload
```

3. **Index Documents**:
```bash
curl -X POST "http://localhost:8000/index" \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 100}'
```

4. **Search Documents**:
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"filename": "contract.pdf", "limit": 10}'
```

### API Documentation

- **Interactive Docs**: http://localhost:8000/docs
- **API Reference**: See `API.md` for complete endpoint documentation
- **Service Guide**: See `INDEXER_README.md` for detailed setup

### Search Examples

**Find PDFs:**
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"filename": ".pdf", "limit": 20}'
```

**Find documents by project:**
```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{"project_name": "Smith vs Jones", "limit": 15}'
```

**Get all documents for a project:**
```bash
curl "http://localhost:8000/search/project/12345?limit=50"
```

## Architecture

### Core Components

- **ProjectGCSMapper**: Maps ProjectId to GCS folder paths with variant handling
- **SupabaseMigrator**: Handles data transfer from MSSQL to Supabase
- **DocumentURLUpdater**: Updates document records with GCS URLs
- **DocumentIndexer**: FastAPI service for searching documents using Turbopuffer
- **MigrationConfig**: Configuration management and validation

### Migration Phases

1. **Project-GCS Path Mapping**: Build mapping between ProjectId and GCS folder structure
2. **Schema Migration**: Convert MSSQL tables to Supabase PostgreSQL format
3. **Data Migration**: Transfer all 719,562 records in configurable batches
4. **Document URL Generation**: Update document records with GCS URLs

### Data Flow

```
MSSQL Database � SupabaseMigrator � Supabase PostgreSQL
       �
Project/Document Records � ProjectGCSMapper � GCS Path Mapping
       �
GCS URLs � DocumentURLUpdater � Existing GCS Files
```

## Testing

Run the test suite:

```bash
uv run pytest tests/ -v
```

## Migration Plan Details

This tool implements the migration plan described in `migration-plan-supabase.md`:

- **Source**: OrgExport_FVtoFV_Migration MSSQL database (27.5 GB, 719,562 records)
- **Target**: Supabase managed PostgreSQL with real-time features  
- **Documents**: Already migrated to GCS bucket `filevine-backup`
- **Timeline**: 3-4 weeks (files already in GCS)

### Key Statistics

- 211 tables to migrate
- 719,562 total records
- 139,642 documents with 114,284 revisions
- 637 projects with intelligent path mapping
- Files already in GCS - no file transfers needed

## Error Handling

- All errors are logged to `migration_log` table in Supabase
- Progress tracking with completion percentages
- Detailed error messages for troubleshooting
- Automatic retry logic with exponential backoff

## Security

- Service role keys for Supabase access
- Row-level security policies on all migrated tables
- Secure credential management via environment variables
- No secrets stored in code or configuration files