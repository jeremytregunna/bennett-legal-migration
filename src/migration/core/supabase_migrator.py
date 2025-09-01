"""Supabase data migration functionality."""

import asyncio
from typing import List, Dict, Any, Optional
import pyodbc
from supabase import create_client, Client
from tqdm import tqdm
from asyncio_throttle import Throttler

from ..models.config import MigrationConfig
from ..models.migration import MigrationStats, MigrationLogEntry


class SupabaseMigrator:
    """Migrates data from MSSQL to Supabase."""
    
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.supabase: Client = create_client(
            config.supabase.url, 
            config.supabase.service_role_key
        )
        self.throttler = Throttler(rate_limit=config.max_concurrent, period=1.0)
        self.stats = MigrationStats()
        self.failed_batches = []  # Store failed batches for CSV export
        
    async def migrate_table(
        self, 
        table_name: str, 
        batch_size: Optional[int] = None
    ) -> MigrationStats:
        """Migrate a table from MSSQL to Supabase."""
        
        if batch_size is None:
            batch_size = self.config.batch_size
            
        print(f"Starting migration of table: {table_name}")
        
        if self.config.dry_run:
            print(f"DRY RUN: Would migrate table {table_name}")
            return self.stats
        
        try:
            # Get data from MSSQL
            mssql_conn = pyodbc.connect(self.config.mssql.connection_string())
            cursor = mssql_conn.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = ? AND table_type = 'BASE TABLE'
            """, table_name)
            
            if cursor.fetchone()[0] == 0:
                print(f"Table {table_name} does not exist, skipping...")
                cursor.close()
                mssql_conn.close()
                return self.stats
            
            # Get total record count for progress tracking
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            total_records = cursor.fetchone()[0]
            self.stats.total_docs += total_records
            
            # Get all data
            cursor.execute(f"SELECT * FROM [{table_name}]")
            
            # Process in batches with progress bar
            with tqdm(total=total_records, desc=f"Migrating {table_name}") as pbar:
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                        
                    # Convert to dict format
                    columns = [column[0] for column in cursor.description]
                    batch_data = [dict(zip(columns, row)) for row in rows]
                    
                    # Convert data types for Supabase
                    converted_batch = self.convert_data_types(batch_data)
                    
                    # Insert into Supabase with throttling
                    async with self.throttler:
                        await self.insert_batch(table_name.lower(), converted_batch)
                    
                    self.stats.migrated_docs += len(converted_batch)
                    pbar.update(len(converted_batch))
            
            cursor.close()
            mssql_conn.close()
            
            print(f"Successfully migrated {table_name}")
            
        except Exception as e:
            error_msg = f"Error migrating table {table_name}: {e}"
            print(error_msg)
            self.stats.errors += 1
            await self.log_error(None, "table_migration_error", error_msg)
            
        return self.stats
    
    async def insert_batch(self, table_name: str, batch_data: List[Dict[str, Any]]) -> None:
        """Insert a batch of data into Supabase."""
        
        # Check if this is a 404 table not found error pattern
        if await self.is_table_missing_error(table_name):
            print(f"Table {table_name} does not exist in Supabase, skipping batch...")
            await self.log_batch_failure(table_name, batch_data, f"Table {table_name} does not exist in Supabase")
            return
        
        try:
            # Prepare batch data, handling id fields properly
            clean_batch_data = []
            for record in batch_data:
                clean_record = {}
                has_valid_id = False
                
                for k, v in record.items():
                    if k.lower() == 'id':
                        # Keep original id if it exists and is not None/empty
                        if v is not None and str(v).strip() and str(v) != '0':
                            clean_record[k] = v
                            has_valid_id = True
                        # Otherwise skip the id field to let Supabase auto-generate
                    else:
                        clean_record[k] = v
                
                # If no valid ID was found but the table needs one, generate a UUID
                if not has_valid_id:
                    import uuid
                    clean_record['id'] = str(uuid.uuid4())
                
                clean_batch_data.append(clean_record)
            
            # Use direct postgrest client to avoid auto-upsert behavior
            result = self.supabase.postgrest.from_(table_name).insert(clean_batch_data).execute()
            
            if result.data:
                print(f"Inserted {len(result.data)} records into {table_name}")
                self.stats.migrated_docs += len(result.data)
            else:
                await self.log_batch_failure(table_name, batch_data, "No data returned from insert")
                
        except Exception as e:
            error_str = str(e)
            
            # Check for table not found errors
            if "404" in error_str and "JSON could not be generated" in error_str:
                print(f"Table {table_name} does not exist in Supabase, skipping...")
                await self.log_batch_failure(table_name, batch_data, f"Table {table_name} not found in Supabase")
                return
            
            # For conflict errors or constraint violations, try inserting records individually
            if ("unique" in error_str.lower() or "duplicate" in error_str.lower() or 
                "42P10" in error_str or "ON CONFLICT" in error_str):
                print(f"Batch insert failed due to constraint issues, trying individual inserts for {table_name}")
                await self.insert_individual_records(table_name, batch_data)
            else:
                error_msg = f"Insert failed for {table_name}: {e}"
                print(error_msg)
                self.stats.errors += 1
                await self.log_batch_failure(table_name, batch_data, error_msg)
    
    async def insert_individual_records(self, table_name: str, batch_data: List[Dict[str, Any]]) -> None:
        """Insert records individually using raw SQL to avoid upsert behavior."""
        successful_inserts = 0
        
        for record in batch_data:
            try:
                # Handle id field properly - keep if valid, otherwise generate UUID
                clean_record = {}
                has_valid_id = False
                
                for k, v in record.items():
                    if k.lower() == 'id':
                        # Keep original id if it exists and is not None/empty
                        if v is not None and str(v).strip() and str(v) != '0':
                            clean_record[k] = v
                            has_valid_id = True
                        # Otherwise skip the id field
                    else:
                        clean_record[k] = v
                
                # If no valid ID was found, generate a UUID
                if not has_valid_id:
                    import uuid
                    clean_record['id'] = str(uuid.uuid4())
                
                # Use raw SQL INSERT to bypass Supabase client's auto-upsert behavior
                success = await self.raw_sql_insert(table_name, clean_record)
                if success:
                    successful_inserts += 1
                    self.stats.migrated_docs += 1
                    
            except Exception as e:
                print(f"Failed to insert individual record: {e}")
                self.stats.errors += 1
        
        if successful_inserts > 0:
            print(f"Successfully inserted {successful_inserts}/{len(batch_data)} individual records into {table_name}")
    
    async def raw_sql_insert(self, table_name: str, record: Dict[str, Any]) -> bool:
        """Insert a record using direct postgrest client to avoid auto-upsert."""
        try:
            # Use the underlying postgrest client directly to control the request
            # This bypasses Supabase's automatic upsert behavior
            response = self.supabase.postgrest.from_(table_name).insert(record).execute()
            return response.data is not None and len(response.data) > 0
        except Exception as e:
            # Let all errors bubble up so we can see what's happening
            raise e
    
    async def is_table_missing_error(self, table_name: str) -> bool:
        """Check if a table exists in Supabase by trying a simple query."""
        try:
            # Try a simple count query to check if table exists
            result = self.supabase.table(table_name).select("*", count="exact").limit(1).execute()
            return False  # Table exists
        except Exception as e:
            error_str = str(e)
            return "404" in error_str and "JSON could not be generated" in error_str
    
    def convert_data_types(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MSSQL data types to Supabase-compatible format."""
        from datetime import datetime, date
        from decimal import Decimal
        import uuid
        converted = []
        
        for record in data:
            converted_record = {}
            
            for key, value in record.items():
                # Convert column names to lowercase (Supabase convention)
                new_key = key.lower()
                
                # Handle specific data type conversions
                if value is None:
                    converted_record[new_key] = None
                elif isinstance(value, datetime):
                    # Convert datetime to ISO format string
                    converted_record[new_key] = value.isoformat()
                elif isinstance(value, date):
                    # Convert date to ISO format string
                    converted_record[new_key] = value.isoformat()
                elif isinstance(value, Decimal):
                    # Convert Decimal to float or string
                    converted_record[new_key] = float(value)
                elif isinstance(value, uuid.UUID):
                    # Convert UUID to string
                    converted_record[new_key] = str(value)
                elif isinstance(value, bytes):
                    # Convert binary data to hex string
                    converted_record[new_key] = value.hex()
                else:
                    converted_record[new_key] = value
            
            converted.append(converted_record)
            
        return converted
    
    async def migrate_all_tables(self, table_names: List[str]) -> MigrationStats:
        """Migrate multiple tables in sequence."""
        
        print(f"Starting migration of {len(table_names)} tables")
        
        for table_name in table_names:
            await self.migrate_table(table_name)
            
        print("Migration completed")
        print(f"Total records migrated: {self.stats.migrated_docs}")
        print(f"Total errors: {self.stats.errors}")
        
        # Export failures to CSV if any occurred
        if self.failed_batches:
            failure_file = self.export_failures_to_csv()
            if failure_file:
                print(f"Failed batches exported to: {failure_file}")
        
        return self.stats
    
    async def get_table_list(self) -> List[str]:
        """Get list of all tables from MSSQL database."""
        try:
            mssql_conn = pyodbc.connect(self.config.mssql.connection_string())
            cursor = mssql_conn.cursor()
            
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            mssql_conn.close()
            
            return tables
            
        except Exception as e:
            print(f"Error getting table list: {e}")
            return []
    
    async def create_supabase_tables(self, table_names: List[str]) -> None:
        """Create all tables in Supabase based on MSSQL schema."""
        print(f"Creating {len(table_names)} tables in Supabase...")
        
        # First, generate all SQL statements and save to file
        all_sql_statements = []
        
        for table_name in table_names:
            sql_statement = await self.generate_table_sql(table_name)
            if sql_statement:
                all_sql_statements.append(sql_statement)
        
        # Save all SQL to a file
        if all_sql_statements:
            sql_file = "supabase_tables.sql"
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write("-- Auto-generated Supabase table creation script\n")
                f.write("-- Run this in Supabase SQL Editor\n\n")
                f.write('\n\n'.join(all_sql_statements))
            
            print(f"Generated SQL file: {sql_file}")
            print(f"Please run this SQL file in your Supabase SQL Editor to create all {len(all_sql_statements)} tables.")
            print("Then re-run the data migration.")
            
        # Skip programmatic creation since Supabase doesn't have exec_sql
        print("Note: Programmatic table creation is not supported by Supabase.")
        print("Please use the generated SQL file in Supabase SQL Editor.")
    
    async def create_single_table(self, table_name: str) -> None:
        """Create a single table in Supabase based on MSSQL schema."""
        try:
            # Get MSSQL table schema
            mssql_conn = pyodbc.connect(self.config.mssql.connection_string())
            cursor = mssql_conn.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = ? AND table_type = 'BASE TABLE'
            """, table_name)
            
            if cursor.fetchone()[0] == 0:
                print(f"Table {table_name} does not exist in MSSQL, skipping...")
                cursor.close()
                mssql_conn.close()
                return
            
            # Get column information
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, table_name)
            
            columns = cursor.fetchall()
            cursor.close()
            mssql_conn.close()
            
            if not columns:
                print(f"No columns found for table {table_name}")
                return
            
            # Generate CREATE TABLE SQL for PostgreSQL/Supabase
            create_sql = self.generate_create_table_sql(table_name, columns)
            
            # Execute SQL using Supabase's query method
            if self.config.dry_run:
                print(f"DRY RUN: Would create table {table_name}")
                print(create_sql)
            else:
                print(f"Creating table: {table_name}")
                try:
                    # Use the raw SQL execution method
                    result = self.supabase.postgrest.session.post(
                        f"{self.supabase.supabase_url}/rest/v1/rpc/exec_sql",
                        json={"sql": create_sql},
                        headers=self.supabase.postgrest.headers
                    )
                    
                    if result.status_code == 200:
                        print(f"Successfully created table: {table_name}")
                    else:
                        print(f"Failed to create table {table_name}: {result.status_code} - {result.text}")
                        
                except Exception as e:
                    print(f"Error creating table {table_name}: {e}")
                    print("Trying alternative method...")
                    
                    # Alternative: Create table using SQL query if exec_sql doesn't exist
                    try:
                        # Split the SQL into individual statements
                        statements = [stmt.strip() for stmt in create_sql.split(';') if stmt.strip()]
                        
                        for statement in statements:
                            if statement.upper().startswith('CREATE TABLE'):
                                # Try using raw query execution
                                import requests
                                response = requests.post(
                                    f"{self.config.supabase.url}/rest/v1/query",
                                    headers={
                                        "apikey": self.config.supabase.service_role_key,
                                        "Authorization": f"Bearer {self.config.supabase.service_role_key}",
                                        "Content-Type": "application/sql"
                                    },
                                    data=statement
                                )
                                
                                if response.status_code not in [200, 201]:
                                    print(f"Failed to execute: {statement[:100]}...")
                                    print(f"Response: {response.status_code} - {response.text}")
                                    
                        print(f"Attempted to create table: {table_name}")
                        
                    except Exception as e2:
                        print(f"Both methods failed for table {table_name}: {e2}")
                        print("You may need to create this table manually in Supabase.")
                    
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
    
    def generate_create_table_sql(self, table_name: str, columns: List) -> str:
        """Generate PostgreSQL CREATE TABLE SQL from MSSQL column info."""
        
        # Data type mappings from MSSQL to PostgreSQL
        type_mapping = {
            'varchar': 'text',
            'nvarchar': 'text', 
            'char': 'text',
            'nchar': 'text',
            'text': 'text',
            'ntext': 'text',
            'int': 'integer',
            'bigint': 'bigint',
            'smallint': 'smallint',
            'tinyint': 'smallint',
            'bit': 'boolean',
            'decimal': 'numeric',
            'numeric': 'numeric',
            'float': 'real',
            'real': 'real',
            'money': 'numeric(19,4)',
            'smallmoney': 'numeric(10,4)',
            'datetime': 'timestamptz',
            'datetime2': 'timestamptz',
            'smalldatetime': 'timestamptz',
            'date': 'date',
            'time': 'time',
            'timestamp': 'bytea',
            'binary': 'bytea',
            'varbinary': 'bytea',
            'image': 'bytea',
            'uniqueidentifier': 'uuid',
            'xml': 'text',
            'sql_variant': 'text'
        }
        
        column_definitions = []
        
        for col in columns:
            col_name = col[0].lower()  # Lowercase for Supabase
            data_type = col[1].lower()
            is_nullable = col[2] == 'YES'
            max_length = col[3]
            precision = col[4]
            scale = col[5]
            default_value = col[6]
            
            # Map data type
            pg_type = type_mapping.get(data_type, 'text')
            
            # Handle precision/scale for numeric types
            if data_type in ['decimal', 'numeric'] and precision and scale:
                pg_type = f"numeric({precision},{scale})"
            
            # Build column definition
            col_def = f'"{col_name}" {pg_type}'
            
            # Add NOT NULL constraint
            if not is_nullable:
                col_def += ' NOT NULL'
            
            # Add default value (simplified)
            if default_value and default_value not in ['NULL', '(NULL)']:
                # Clean up MSSQL default syntax
                clean_default = default_value.strip("()")
                if clean_default.startswith("'") and clean_default.endswith("'"):
                    col_def += f' DEFAULT {clean_default}'
                elif clean_default.lower() in ['getdate()', 'getutcdate()']:
                    col_def += ' DEFAULT now()'
                elif clean_default.lower() == 'newid()':
                    col_def += ' DEFAULT gen_random_uuid()'
            
            column_definitions.append(col_def)
        
        # Add id column as primary key if not exists
        has_id = any('id' in col[0].lower() for col in columns)
        has_primary_key = any('primary key' in col_def.lower() for col_def in column_definitions)
        
        if not has_id:
            column_definitions.insert(0, 'id uuid DEFAULT gen_random_uuid() PRIMARY KEY')
        elif not has_primary_key:
            # Find the id column and make it primary key
            for i, col_def in enumerate(column_definitions):
                if '"id"' in col_def.lower():
                    column_definitions[i] = col_def + ' PRIMARY KEY'
                    break
        
        table_name_lower = table_name.lower()
        create_sql = f'''
CREATE TABLE IF NOT EXISTS "{table_name_lower}" (
    {',\n    '.join(column_definitions)}
);

-- Enable Row Level Security
ALTER TABLE "{table_name_lower}" ENABLE ROW LEVEL SECURITY;

-- Create policy for authenticated users
CREATE POLICY "Users can access {table_name_lower}" ON "{table_name_lower}"
    FOR ALL USING (auth.role() = 'authenticated');
'''
        
        return create_sql
    
    async def generate_table_sql(self, table_name: str) -> Optional[str]:
        """Generate CREATE TABLE SQL for a single table."""
        try:
            # Get MSSQL table schema
            mssql_conn = pyodbc.connect(self.config.mssql.connection_string())
            cursor = mssql_conn.cursor()
            
            # Check if table exists first
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = ? AND table_type = 'BASE TABLE'
            """, table_name)
            
            if cursor.fetchone()[0] == 0:
                cursor.close()
                mssql_conn.close()
                return None
            
            # Get column information
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, table_name)
            
            columns = cursor.fetchall()
            cursor.close()
            mssql_conn.close()
            
            if not columns:
                return None
            
            # Generate CREATE TABLE SQL for PostgreSQL/Supabase
            return self.generate_create_table_sql(table_name, columns)
            
        except Exception as e:
            print(f"Error generating SQL for table {table_name}: {e}")
            return None
    
    async def log_error(
        self, 
        doc_id: Optional[str], 
        log_type: str, 
        message: str
    ) -> None:
        """Log an error to the migration log."""
        if self.config.dry_run:
            print(f"DRY RUN: Would log error - {log_type}: {message}")
            return
            
        try:
            log_entry = MigrationLogEntry(
                doc_id=doc_id,
                log_type=log_type,
                message=message
            )
            
            self.supabase.table("migration_log").insert({
                "doc_id": log_entry.doc_id,
                "log_type": log_entry.log_type,
                "message": log_entry.message,
                "created_at": log_entry.created_at.isoformat()
            }).execute()
            
        except Exception as e:
            print(f"Failed to log error: {e}")
    
    async def log_batch_failure(
        self, 
        table_name: str, 
        batch_data: List[Dict[str, Any]], 
        error_message: str
    ) -> None:
        """Log a failed batch for CSV export and retry."""
        from datetime import datetime
        
        failure_record = {
            'table_name': table_name,
            'error_message': error_message,
            'batch_size': len(batch_data),
            'failed_at': datetime.now().isoformat(),
            'batch_data': batch_data
        }
        
        self.failed_batches.append(failure_record)
        
        # Also log to migration_log if possible
        await self.log_error(None, "batch_failure", f"{table_name}: {error_message}")
    
    def export_failures_to_csv(self, filename: Optional[str] = None) -> Optional[str]:
        """Export all failed batches to CSV files for retry."""
        if not self.failed_batches:
            print("No failures to export")
            return None
        
        from datetime import datetime
        import csv
        import json
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"migration_failures_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'table_name', 'error_message', 'batch_size', 
                    'failed_at', 'sample_record', 'full_batch_json'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for failure in self.failed_batches:
                    # Get sample record for easier analysis
                    sample_record = failure['batch_data'][0] if failure['batch_data'] else {}
                    
                    writer.writerow({
                        'table_name': failure['table_name'],
                        'error_message': failure['error_message'],
                        'batch_size': failure['batch_size'],
                        'failed_at': failure['failed_at'],
                        'sample_record': json.dumps(sample_record, default=str),
                        'full_batch_json': json.dumps(failure['batch_data'], default=str)
                    })
            
            print(f"Exported {len(self.failed_batches)} failed batches to: {filename}")
            return filename
            
        except Exception as e:
            print(f"Failed to export failures to CSV: {e}")
            return None
    
    async def retry_failed_batches(self) -> int:
        """Retry all failed batches, attempting individual record insertion."""
        if not self.failed_batches:
            print("No failed batches to retry")
            return 0
        
        print(f"Retrying {len(self.failed_batches)} failed batches...")
        retry_count = 0
        
        for failure in self.failed_batches[:]:  # Copy list to modify during iteration
            table_name = failure['table_name']
            batch_data = failure['batch_data']
            
            print(f"Retrying {len(batch_data)} records for table {table_name}...")
            
            # Try inserting records individually
            successful_records = 0
            for record in batch_data:
                try:
                    result = self.supabase.table(table_name).upsert([record], on_conflict="id").execute()
                    if result.data:
                        successful_records += 1
                except Exception as e:
                    print(f"Failed to insert individual record: {e}")
            
            if successful_records > 0:
                print(f"Successfully inserted {successful_records}/{len(batch_data)} records for {table_name}")
                retry_count += successful_records
                
                # Remove this failure from the list if all records succeeded
                if successful_records == len(batch_data):
                    self.failed_batches.remove(failure)
        
        print(f"Retry completed: {retry_count} additional records inserted")
        return retry_count
    
    def get_migration_stats(self) -> MigrationStats:
        """Get current migration statistics."""
        return self.stats