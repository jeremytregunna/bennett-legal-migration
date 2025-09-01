"""Command-line interface for the migration tool."""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import List, Optional

from .core.gcs_mapper import ProjectGCSMapper
from .core.supabase_migrator import SupabaseMigrator
from .core.document_updater import DocumentURLUpdater
from .utils.config_loader import load_config_from_env, create_sample_env_file, validate_config
from .models.migration import ProjectRecord, DocumentRecord


class MigrationCLI:
    """Main CLI interface for migration operations."""
    
    def __init__(self, config_file: Optional[str] = None):
        try:
            self.config = load_config_from_env(config_file)
            
            # Validate configuration
            config_errors = validate_config(self.config)
            if config_errors:
                print("Configuration errors:")
                for error in config_errors:
                    print(f"  - {error}")
                sys.exit(1)
                
        except Exception as e:
            print(f"Failed to load configuration: {e}")
            sys.exit(1)
    
    async def run_full_migration(self) -> None:
        """Run the complete migration process."""
        
        print("=== Starting Full Migration Process ===")
        print(f"Dry run mode: {self.config.dry_run}")
        
        try:
            # Phase 1: Build project-to-GCS mapping
            print("\n1. Building project-to-GCS path mapping...")
            gcs_mapper = ProjectGCSMapper(self.config.gcs)
            
            # Get projects and documents from MSSQL
            migrator = SupabaseMigrator(self.config)
            projects = await self.get_projects_from_mssql()
            documents = await self.get_documents_from_mssql()
            
            project_gcs_map = gcs_mapper.build_project_mapping(projects, documents)
            mapping_stats = gcs_mapper.get_mapping_stats()
            
            print(f"Projects mapped to GCS paths: {mapping_stats['total_projects_mapped']}")
            print(f"Unique GCS paths: {mapping_stats['unique_paths']}")
            
            # Phase 2: Migrate table data
            print("\n2. Migrating table data to Supabase...")
            table_names = await migrator.get_table_list()
            print(f"Found {len(table_names)} tables to migrate")
            
            migration_stats = await migrator.migrate_all_tables(table_names)
            
            # Phase 3: Update document URLs
            print("\n3. Updating document URLs with GCS paths...")
            url_updater = DocumentURLUpdater(self.config, project_gcs_map)
            update_stats = await url_updater.update_all_document_urls()
            
            print("\n=== Migration Complete ===")
            print(f"Total tables migrated: {len(table_names)}")
            print(f"Total records migrated: {migration_stats.migrated_docs}")
            print(f"Documents with URLs updated: {update_stats.migrated_docs}")
            print(f"Total errors: {migration_stats.errors + update_stats.errors}")
            
        except Exception as e:
            print(f"Migration failed: {e}")
            sys.exit(1)
    
    async def run_mapping_analysis(self, export_csv: bool = False) -> None:
        """Analyze project-to-GCS path mapping without migration."""
        
        print("=== Project-to-GCS Mapping Analysis ===")
        
        try:
            gcs_mapper = ProjectGCSMapper(self.config.gcs)
            
            # Get projects and documents from MSSQL  
            projects = await self.get_projects_from_mssql()
            documents = await self.get_documents_from_mssql()
            
            print(f"Found {len(projects)} projects and {len(documents)} documents")
            
            # Group documents by project for stats
            from collections import defaultdict
            docs_by_project = defaultdict(list)
            docs_with_filenames_by_project = defaultdict(list)
            
            for doc in documents:
                if doc.project_id:
                    docs_by_project[doc.project_id].append(doc)
                    if doc.filename and doc.filename.strip():
                        docs_with_filenames_by_project[doc.project_id].append(doc)
            
            # Calculate project categories
            projects_with_docs_and_filenames = set(docs_with_filenames_by_project.keys())
            projects_with_docs_no_filenames = set()
            projects_with_no_docs = set()
            
            for project in projects:
                project_id = project.id
                if project_id not in docs_by_project:
                    projects_with_no_docs.add(project_id)
                elif project_id not in docs_with_filenames_by_project:
                    projects_with_docs_no_filenames.add(project_id)
            
            project_gcs_map = gcs_mapper.build_project_mapping(projects, documents)
            mapping_stats = gcs_mapper.get_mapping_stats()
            
            print(f"Projects with documents & filenames: {len(projects_with_docs_and_filenames)}")
            print(f"Projects successfully mapped: {mapping_stats['total_projects_mapped']}")
            print(f"Unique GCS paths: {mapping_stats['unique_paths']}")
            print(f"Projects with no documents: {len(projects_with_no_docs)}")
            print(f"Projects with docs but no filenames: {len(projects_with_docs_no_filenames)}")
            
            # Show unmapped projects (only those that have docs with filenames but no GCS folder)
            mapped_project_ids = set(project_gcs_map.keys())
            truly_unmapped_ids = projects_with_docs_and_filenames - mapped_project_ids
            
            if truly_unmapped_ids:
                print(f"Truly unmapped projects (have docs+filenames but no GCS folder): {len(truly_unmapped_ids)}")
                
                # Collect unmapped project details
                unmapped_projects = []
                for project in projects:
                    if project.id in truly_unmapped_ids:
                        project_docs = docs_with_filenames_by_project.get(project.id, [])
                        doc_count = len(project_docs)
                        
                        # Get sample document filenames (first 3)
                        sample_files = []
                        total_size = 0
                        for doc in project_docs[:3]:
                            if doc.filename:
                                sample_files.append(doc.filename)
                            if doc.size:
                                total_size += doc.size
                        
                        # Generate sanitized name for variants
                        import re
                        sanitized_name = re.sub(r'"+', '_', project.project_name)
                        sanitized_name = sanitized_name.replace('/', '_')
                        variants = [
                            f"{project.project_name} (1)",
                            f"{sanitized_name} (1)", 
                            f"Solar - {project.project_name}",
                            f"Solar - {sanitized_name}",
                            f"Solar - PNC {project.project_name}",
                            f"Solar - PNC {sanitized_name}",
                            "zzz_mailroom_no_project_assigned (fallback)"
                        ]
                        
                        unmapped_projects.append({
                            'project_id': project.id,
                            'project_name': project.project_name,
                            'sanitized_name': sanitized_name,
                            'document_count': doc_count,
                            'total_size_bytes': total_size,
                            'sample_filenames': '; '.join(sample_files),
                            'expected_gcs_path': f"docs/Bennett Legal/{project.project_name}",
                            'sanitized_gcs_path': f"docs/Bennett Legal/{sanitized_name}",
                            'possible_variants': '; '.join(variants),
                            'unmapped_reason': 'has_docs_with_filenames_but_no_gcs_folder'
                        })
                        
                        if not export_csv:
                            print(f"  - {project.id}: {project.project_name}")
                
                # Also collect projects with docs but no filenames for CSV export
                if export_csv:
                    for project in projects:
                        if project.id in projects_with_docs_no_filenames:
                            project_docs = docs_by_project.get(project.id, [])
                            doc_count = len(project_docs)
                            
                            import re
                            sanitized_name = re.sub(r'"+', '_', project.project_name)
                            sanitized_name = sanitized_name.replace('/', '_')
                            variants = [
                                f"{project.project_name} (1)",
                                f"{sanitized_name} (1)", 
                                f"Solar - {project.project_name}",
                                f"Solar - {sanitized_name}",
                                f"Solar - PNC {project.project_name}",
                                f"Solar - PNC {sanitized_name}",
                                "zzz_mailroom_no_project_assigned (fallback)"
                            ]
                            
                            unmapped_projects.append({
                                'project_id': project.id,
                                'project_name': project.project_name,
                                'sanitized_name': sanitized_name,
                                'document_count': doc_count,
                                'total_size_bytes': 0,
                                'sample_filenames': '',
                                'expected_gcs_path': f"docs/Bennett Legal/{project.project_name}",
                                'sanitized_gcs_path': f"docs/Bennett Legal/{sanitized_name}",
                                'possible_variants': '; '.join(variants),
                                'unmapped_reason': 'has_docs_but_no_filenames'
                            })
                
                # Export to CSV if requested
                if export_csv:
                    import csv
                    from datetime import datetime
                    
                    csv_filename = f"unmapped_projects_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    
                    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = [
                            'project_id', 'project_name', 'sanitized_name', 'document_count', 
                            'total_size_bytes', 'sample_filenames', 
                            'expected_gcs_path', 'sanitized_gcs_path', 'possible_variants', 'unmapped_reason'
                        ]
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(unmapped_projects)
                    
                    print(f"Exported {len(unmapped_projects)} projects with mapping issues to: {csv_filename}")
                else:
                    # Show first 15 for console output
                    shown = 0
                    for project in projects:
                        if project.id in truly_unmapped_ids and shown < 15:
                            print(f"  - {project.id}: {project.project_name}")
                            shown += 1
                    if len(truly_unmapped_ids) > 15:
                        print(f"  ... and {len(truly_unmapped_ids) - 15} more (use --export-csv to see all)")
            else:
                print("All projects with documents successfully mapped to GCS folders!")
            
        except Exception as e:
            print(f"Mapping analysis failed: {e}")
            sys.exit(1)
    
    async def run_data_migration_only(self, table_names: Optional[List[str]] = None) -> None:
        """Run only the data migration phase."""
        
        print("=== Data Migration Only ===")
        
        try:
            migrator = SupabaseMigrator(self.config)
            
            if not table_names:
                table_names = await migrator.get_table_list()
                print(f"Found {len(table_names)} tables to migrate")
            else:
                print(f"Migrating specified tables: {', '.join(table_names)}")
            
            stats = await migrator.migrate_all_tables(table_names)
            
            print(f"Migration completed: {stats.migrated_docs} records, {stats.errors} errors")
            
        except Exception as e:
            print(f"Data migration failed: {e}")
            sys.exit(1)
    
    async def run_url_updates_only(self) -> None:
        """Run only the document URL update phase."""
        
        print("=== Document URL Updates Only ===")
        
        try:
            # Need project mapping first
            gcs_mapper = ProjectGCSMapper(self.config.gcs)
            projects = await self.get_projects_from_mssql()
            documents = await self.get_documents_from_mssql()
            project_gcs_map = gcs_mapper.build_project_mapping(projects, documents)
            
            # Update URLs
            url_updater = DocumentURLUpdater(self.config, project_gcs_map)
            stats = await url_updater.update_all_document_urls()
            
            print(f"URL updates completed: {stats.migrated_docs} documents, {stats.errors} errors")
            
        except Exception as e:
            print(f"URL updates failed: {e}")
            sys.exit(1)
    
    async def run_create_tables_only(self) -> None:
        """Create Supabase tables based on MSSQL schema."""
        
        print("=== Creating Supabase Tables ===")
        
        try:
            migrator = SupabaseMigrator(self.config)
            table_names = await migrator.get_table_list()
            print(f"Found {len(table_names)} tables to create")
            
            await migrator.create_supabase_tables(table_names)
            
            print("Table creation completed")
            
        except Exception as e:
            print(f"Table creation failed: {e}")
            sys.exit(1)
    
    async def run_retry_failures(self, csv_file: Optional[str] = None) -> None:
        """Retry failed migrations from CSV file or recent failures."""
        
        print("=== Retrying Failed Migrations ===")
        
        try:
            migrator = SupabaseMigrator(self.config)
            
            if csv_file:
                # Load failures from CSV file
                print(f"Loading failures from: {csv_file}")
                await self.load_failures_from_csv(migrator, csv_file)
            
            # Retry all failed batches
            retry_count = await migrator.retry_failed_batches()
            
            # Export any remaining failures
            if migrator.failed_batches:
                failure_file = migrator.export_failures_to_csv()
                if failure_file:
                    print(f"Remaining failures exported to: {failure_file}")
            
            print(f"Retry completed: {retry_count} records successfully inserted")
            
        except Exception as e:
            print(f"Retry failed: {e}")
            sys.exit(1)
    
    async def load_failures_from_csv(self, migrator: SupabaseMigrator, csv_file: str) -> None:
        """Load failed batches from CSV file into migrator."""
        import csv
        import json
        
        try:
            # Increase CSV field size limit to handle large batch data
            csv.field_size_limit(10 * 1024 * 1024)  # 10MB limit
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                loaded_count = 0
                
                for row in reader:
                    try:
                        batch_data = json.loads(row['full_batch_json'])
                        failure_record = {
                            'table_name': row['table_name'],
                            'error_message': row['error_message'],
                            'batch_size': int(row['batch_size']),
                            'failed_at': row['failed_at'],
                            'batch_data': batch_data
                        }
                        migrator.failed_batches.append(failure_record)
                        loaded_count += 1
                        
                    except json.JSONDecodeError as je:
                        print(f"Failed to parse JSON for row {loaded_count + 1}: {je}")
                        continue
                    except Exception as re:
                        print(f"Failed to process row {loaded_count + 1}: {re}")
                        continue
            
            print(f"Loaded {loaded_count} failed batches from CSV")
            
        except Exception as e:
            print(f"Failed to load CSV file: {e}")
            raise
    
    async def get_projects_from_mssql(self) -> List[ProjectRecord]:
        """Get project records from MSSQL database."""
        import pyodbc
        
        conn = pyodbc.connect(self.config.mssql.connection_string())
        cursor = conn.cursor()
        
        cursor.execute("SELECT ID, ProjectName FROM [Project]")
        projects = []
        
        for row in cursor.fetchall():
            projects.append(ProjectRecord(id=row[0], project_name=row[1]))
        
        cursor.close()
        conn.close()
        
        return projects
    
    async def get_documents_from_mssql(self) -> List[DocumentRecord]:
        """Get document records from MSSQL database."""
        import pyodbc
        
        conn = pyodbc.connect(self.config.mssql.connection_string())
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ID, ProjectID, Filename, DocKey, Size, UploaderID, UploadDate
            FROM [Doc]
        """)
        
        documents = []
        for row in cursor.fetchall():
            documents.append(DocumentRecord(
                id=row[0],
                project_id=row[1],
                filename=row[2],
                doc_key=row[3],
                size=row[4],
                uploader_id=row[5],
                upload_date=row[6]
            ))
        
        cursor.close()
        conn.close()
        
        return documents


def main():
    """Main CLI entry point."""
    
    parser = argparse.ArgumentParser(description="Filevine to Supabase Migration Tool")
    parser.add_argument(
        "--config", "-c", 
        type=str, 
        help="Path to configuration file (defaults to .env in current directory)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Full migration command
    subparsers.add_parser("migrate", help="Run complete migration process")
    
    # Mapping analysis command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze project-to-GCS path mapping")
    analyze_parser.add_argument(
        "--export-csv", 
        action="store_true",
        help="Export unmapped projects to CSV file with detailed information"
    )
    
    # Data migration only
    data_parser = subparsers.add_parser("data", help="Run data migration only")
    data_parser.add_argument(
        "--tables", "-t", 
        nargs="+", 
        help="Specific tables to migrate (default: all tables)"
    )
    
    # URL updates only
    subparsers.add_parser("urls", help="Update document URLs only")
    
    # Create tables only
    subparsers.add_parser("create-tables", help="Create Supabase tables based on MSSQL schema")
    
    # Retry failures command
    retry_parser = subparsers.add_parser("retry", help="Retry failed migrations from CSV file")
    retry_parser.add_argument(
        "--csv-file", "-f",
        type=str,
        help="CSV file containing failed batches to retry"
    )
    
    # Create sample config
    subparsers.add_parser("init", help="Create sample configuration file")
    
    # Schema migration command
    schema_parser = subparsers.add_parser("schema-migrate", help="Migrate Filevine data to target org schema")
    schema_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate migration without saving data - shows what would happen and any errors"
    )
    
    # Document migration command  
    docs_parser = subparsers.add_parser("migrate-documents", help="Migrate documents and create folder structure")
    docs_parser.add_argument(
        "--dry-run",
        action="store_true", 
        help="Simulate document migration without saving data"
    )
    
    # Custom fields and notes migration command
    extras_parser = subparsers.add_parser("migrate-extras", help="Migrate custom fields and notes")
    extras_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate extras migration without saving data"
    )
    extras_parser.add_argument(
        "--skip-custom-fields",
        action="store_true",
        help="Skip custom fields migration, only migrate notes"
    )
    extras_parser.add_argument(
        "--skip-notes",
        action="store_true",
        help="Skip notes migration, only migrate custom fields"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == "init":
        create_sample_env_file()
        print("Sample configuration created. Edit .env.example and rename to .env")
        return
    
    # Initialize CLI with config
    cli = MigrationCLI(args.config)
    
    # Run appropriate command
    if args.command == "migrate":
        asyncio.run(cli.run_full_migration())
    elif args.command == "analyze":
        asyncio.run(cli.run_mapping_analysis(export_csv=args.export_csv))
    elif args.command == "data":
        asyncio.run(cli.run_data_migration_only(args.tables))
    elif args.command == "urls":
        asyncio.run(cli.run_url_updates_only())
    elif args.command == "create-tables":
        asyncio.run(cli.run_create_tables_only())
    elif args.command == "retry":
        asyncio.run(cli.run_retry_failures(args.csv_file))
    elif args.command == "schema-migrate":
        from .core.schema_migrator import SchemaMigrator
        from .utils.config_loader import load_config_from_env
        
        config = load_config_from_env()
        dry_run = getattr(args, 'dry_run', False)
        migrator = SchemaMigrator(config, dry_run=dry_run)
        
        async def run_schema_migration():
            if dry_run:
                print("🧪 DRY RUN MODE - No data will be saved to Supabase")
                print("This will simulate the migration and report any potential errors\n")
            
            # Create migration user
            await migrator.create_migration_user()
            
            # Show additional table creation SQL
            await migrator.create_additional_tables()
            
            # Run migrations in order
            contact_type_map = await migrator.check_missing_contact_types()
            contact_id_map = await migrator.migrate_contacts()
            case_id_map = await migrator.migrate_cases(contact_id_map)
            file_id_map = await migrator.migrate_documents(case_id_map)
            
            print("=== Schema Migration Complete ===")
            print(f"Contact types: {len(contact_type_map)}")
            print(f"Contacts: {len(contact_id_map)}")
            print(f"Cases: {len(case_id_map)}")
            print(f"Documents: {len(file_id_map)}")
            
            # Print dry run summary if in dry run mode
            migrator.print_dry_run_summary()
        
        asyncio.run(run_schema_migration())
    elif args.command == "migrate-documents":
        from .core.schema_migrator import SchemaMigrator
        from .utils.config_loader import load_config_from_env
        
        config = load_config_from_env()
        dry_run = getattr(args, 'dry_run', False)
        migrator = SchemaMigrator(config, dry_run=dry_run)
        
        async def run_document_migration():
            print("=== Document Migration ===")
            
            if dry_run:
                print("🧪 DRY RUN MODE - No data will be saved to Supabase")
                print("This will simulate document migration and report any potential errors\n")
            
            # Create migration user
            await migrator.create_migration_user()
            
            # Need existing case mappings first
            print("Getting existing case mappings...")
            
            # Get all existing cases with their full details
            response = migrator.get_table("cases").select("id, full_name, created_at, claimant_id").execute()
            existing_cases = response.data
            print(f"Found {len(existing_cases)} existing cases")
            
            # Get all projects to map against cases
            response = migrator.get_source_table("project").select("id, projectname, createdate, clientid").execute()
            projects = response.data
            print(f"Found {len(projects)} projects to map")
            
            # Build case mapping using multiple strategies
            case_id_map = {}
            
            # Strategy 1: Match by project name and client
            for project in projects:
                project_id = str(project['id'])
                project_name = project.get('projectname', '').strip()
                project_client = str(project.get('clientid', '')) if project.get('clientid') else None
                
                # Look for exact name match first
                for case in existing_cases:
                    if (case.get('full_name', '').strip() == project_name and 
                        project_name and 
                        project_id not in case_id_map):
                        case_id_map[project_id] = case['id']
                        break
            
            # Strategy 2: Match by creation date for unmatched projects
            for project in projects:
                project_id = str(project['id'])
                if project_id not in case_id_map:
                    project_create_date = project.get('createdate')
                    if project_create_date:
                        # Find cases with same creation date
                        for case in existing_cases:
                            if case.get('created_at') == project_create_date:
                                case_id_map[project_id] = case['id']
                                break
            
            print(f"Successfully mapped {len(case_id_map)} projects to cases")
            
            # Run document migration
            file_id_map = await migrator.migrate_documents(case_id_map)
            
            print("=== Document Migration Complete ===")
            print(f"Documents migrated: {len(file_id_map)}")
            
            # Print dry run summary if in dry run mode
            migrator.print_dry_run_summary()
        
        asyncio.run(run_document_migration())
    elif args.command == "migrate-extras":
        from .core.schema_migrator import SchemaMigrator
        from .utils.config_loader import load_config_from_env
        
        config = load_config_from_env()
        dry_run = getattr(args, 'dry_run', False)
        skip_custom_fields = getattr(args, 'skip_custom_fields', False)
        skip_notes = getattr(args, 'skip_notes', False)
        migrator = SchemaMigrator(config, dry_run=dry_run)
        
        async def run_extras_migration():
            # Validate arguments
            if skip_custom_fields and skip_notes:
                print("Error: Cannot skip both custom fields and notes. Nothing to migrate!")
                return
            
            migration_items = []
            if not skip_custom_fields:
                migration_items.append("custom fields")
            if not skip_notes:
                migration_items.append("notes")
            
            print(f"=== {' and '.join(migration_items).title()} Migration ===")
            
            if dry_run:
                print("🧪 DRY RUN MODE - No data will be saved to Supabase")
                print("This will simulate extras migration and report any potential errors\n")
            
            # Get entity ID mappings from existing data
            print("Loading existing entity mappings...")
            contact_id_map = await migrator.get_existing_contact_mapping()
            case_id_map = await migrator.get_existing_case_mapping()
            
            print(f"Found {len(contact_id_map)} contacts and {len(case_id_map)} cases")
            
            custom_fields_count = 0
            notes_count = 0
            
            # Migrate custom fields (if not skipped)
            if not skip_custom_fields:
                custom_fields_count = await migrator.migrate_custom_fields(contact_id_map, case_id_map)
            else:
                print("⏭️  Skipping custom fields migration")
            
            # Migrate notes (if not skipped)
            if not skip_notes:
                notes_count = await migrator.migrate_notes(contact_id_map, case_id_map)
            else:
                print("⏭️  Skipping notes migration")
            
            print("=== Extras Migration Complete ===")
            if not skip_custom_fields:
                print(f"Custom fields migrated: {custom_fields_count}")
            if not skip_notes:
                print(f"Notes migrated: {notes_count}")
            
            # Print dry run summary if in dry run mode
            migrator.print_dry_run_summary()
        
        asyncio.run(run_extras_migration())


if __name__ == "__main__":
    main()