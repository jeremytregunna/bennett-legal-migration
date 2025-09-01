#!/usr/bin/env python3
"""
Generate ALTER TABLE statements to add primary key constraints for tables in retry file.
"""

import csv
import json
import sys
from pathlib import Path


def extract_table_errors_from_retry_file(csv_file_path: str) -> dict:
    """Extract table names and their error codes from the retry CSV file."""
    table_errors = {}
    
    # Set CSV field size limit to handle large fields
    csv.field_size_limit(10 * 1024 * 1024)  # 10MB limit
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                table_name = row.get('table_name', '').strip()
                error_message = row.get('error_message', '').strip()
                
                if table_name and error_message:
                    # Check for specific error codes in the error message
                    if '42P10' in error_message:
                        table_errors[table_name] = '42P10'  # No unique constraint
                    elif '42703' in error_message:
                        table_errors[table_name] = '42703'  # Column doesn't exist
                    else:
                        # Default to 42P10 if we can't determine the error
                        table_errors[table_name] = '42P10'
                    
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {}
    
    return table_errors


def generate_alter_table_sql(table_errors: dict) -> str:
    """Generate ALTER TABLE SQL statements based on error codes."""
    sql_statements = []
    
    sql_statements.append("-- ALTER TABLE statements to fix primary key issues")
    sql_statements.append("-- Run these in Supabase SQL Editor")
    sql_statements.append("")
    
    for table_name in sorted(table_errors.keys()):
        error_code = table_errors[table_name]
        
        sql_statements.append(f"-- Table: {table_name} (Error: {error_code})")
        
        if error_code == '42703':
            # Column doesn't exist - ADD id column with PRIMARY KEY
            sql_statements.append(f"ALTER TABLE \"{table_name}\" ADD COLUMN id uuid DEFAULT gen_random_uuid() PRIMARY KEY;")
        else:
            # 42P10 or other - ALTER existing id to be UNIQUE (nullable)
            sql_statements.append(f"ALTER TABLE \"{table_name}\" DROP CONSTRAINT IF EXISTS {table_name}_id_unique;")
            sql_statements.append(f"ALTER TABLE \"{table_name}\" ADD CONSTRAINT {table_name}_id_unique UNIQUE (id);")
        
        sql_statements.append("")
    
    return '\n'.join(sql_statements)


def main():
    if len(sys.argv) != 2:
        print("Usage: python generate_alter_tables.py <retry_csv_file>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    
    if not Path(csv_file_path).exists():
        print(f"Error: File {csv_file_path} does not exist")
        sys.exit(1)
    
    print(f"Analyzing retry file: {csv_file_path}")
    
    # Extract table names and error codes from retry file
    table_errors = extract_table_errors_from_retry_file(csv_file_path)
    
    if not table_errors:
        print("No table errors found in the retry file")
        sys.exit(1)
    
    print(f"Found {len(table_errors)} tables with errors:")
    for table_name, error_code in sorted(table_errors.items()):
        print(f"  {table_name}: {error_code}")
    print()
    
    # Generate ALTER TABLE SQL
    sql = generate_alter_table_sql(table_errors)
    
    # Output SQL to console
    print(sql)
    
    # Also save to file
    output_file = "alter_tables_for_retry.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(sql)
    
    print(f"\nSQL also saved to: {output_file}")


if __name__ == "__main__":
    main()