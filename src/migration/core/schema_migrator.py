#!/usr/bin/env python3
"""
Schema and data migration from Filevine Supabase tables to target org schema.
"""

import asyncio
import uuid
import json
import re
import hashlib
import os
import mimetypes
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from supabase import Client, create_client
from supabase.client import ClientOptions
from google.cloud import storage

from ..models.config import MigrationConfig
from ..utils.config_loader import load_config_from_env


class SchemaMigrator:
    """Migrates data from Filevine tables to target org schema."""
    
    def __init__(self, config: MigrationConfig, dry_run: bool = False):
        self.config = config
        self.target_org = "org_31rnd2vorzuy4fszlzncmcgu5bi"
        
        # Create Supabase client with custom schema for target tables
        client_options = ClientOptions(
            schema=self.target_org
        )
        self.supabase = create_client(
            config.supabase.url, 
            config.supabase.service_role_key,
            options=client_options
        )
        
        # Create separate client for source tables in public schema
        self.supabase_public = create_client(
            config.supabase.url, 
            config.supabase.service_role_key
        )
        
        # Create separate client for private schema tables
        private_client_options = ClientOptions(schema="private")
        self.supabase_private = create_client(
            config.supabase.url, 
            config.supabase.service_role_key,
            options=private_client_options
        )
        
        # Store schema for table references
        # Note: Supabase client doesn't directly support schema switching, 
        # so we'll need to use .from_() with schema qualified table names
        self.migration_user_id = None
        self.dry_run = dry_run
        self.dry_run_errors = []  # Collect errors during dry run
        self.dry_run_stats = {  # Track what would be created/updated
            'contacts_created': 0,
            'contacts_duplicates': 0,
            'cases_created': 0,
            'folders_created': 0,
            'files_created': 0,
            'tables_created': 0,
            'users_created': 0,
            'custom_fields_created': 0,
            'notes_created': 0
        }
        
        # Load case type mappings from database
        self.case_type_ids = self._load_case_type_ids()
        
        # Load contact type mappings from database
        self.contact_type_ids = self._load_contact_type_ids()
        
        # Set up GCS clients for file copying
        self.source_gcs_project = "webapp-466015"
        self.source_bucket_name = "filevine-backup"
        self.dest_gcs_project = config.gcs.project_id  # dataengineerng
        self.dest_bucket_name = config.gcs.bucket_name  # bennett_bucket1
        self.local_data_path = Path("../data")  # Local sync of filevine-backup
        
        # GCS clients - need credentials path for proper setup
        if config.gcs.credentials_path and os.path.exists(config.gcs.credentials_path):
            self.dest_storage_client = storage.Client.from_service_account_json(
                config.gcs.credentials_path, project=self.dest_gcs_project
            )
            # For source project, we'll use default credentials
            self.source_storage_client = storage.Client(project=self.source_gcs_project)
        else:
            # Use default credentials for both
            self.dest_storage_client = storage.Client(project=self.dest_gcs_project)
            self.source_storage_client = storage.Client(project=self.source_gcs_project)
        
        self.dest_bucket = self.dest_storage_client.bucket(self.dest_bucket_name)
        self.source_bucket = self.source_storage_client.bucket(self.source_bucket_name)
        
        # Filevine to target case type mapping
        self.filevine_case_type_map = {
            'Personal Injury Litigation': 'litigation',
            'Litigation-Hourly': 'litigation',
            'Solar Arbitration': 'solar',
            'Individual MVA': 'IMVA'
        }
        
        # Document categorization patterns
        self.document_categories = {
            'correspondence': ['letter', 'email', 'correspondence', 'memo'],
            'expert-reports': ['expert', 'report', 'evaluation', 'analysis'],
            'medical-records': ['medical', 'health', 'doctor', 'hospital', 'mri', 'ct', 'treatment'],
            'billing': ['bill', 'invoice', 'statement', 'payment', 'receipt'],
            'documents': []  # default category
        }
        
        # GCS client for file operations (initialize with error handling)
        try:
            # Use project ID from config if available
            if hasattr(config, 'gcs') and hasattr(config.gcs, 'project_id'):
                self.gcs_client = storage.Client(project=config.gcs.project_id)
            else:
                # Fallback to default credentials
                self.gcs_client = storage.Client()
        except Exception as e:
            print(f"Warning: GCS client initialization failed: {e}")
            self.gcs_client = None
    
    def get_table(self, table_name: str):
        """Get a Supabase table reference with the configured schema."""
        return self.supabase.table(table_name)
    
    def get_source_table(self, table_name: str):
        """Get a source table reference from the public schema."""
        return self.supabase_public.table(table_name)
    
    def _load_case_type_ids(self) -> Dict[str, str]:
        """Load case type IDs from the target database."""
        try:
            result = self.get_table('case_types').select('id, name').execute()
            case_type_map = {}
            for case_type in result.data:
                case_type_map[case_type['name']] = case_type['id']
            return case_type_map
        except Exception as e:
            print(f"Warning: Could not load case types from database: {e}")
            # Return empty dict as fallback
            return {}
    
    def _load_contact_type_ids(self) -> Dict[str, str]:
        """Load contact type IDs from the target database."""
        try:
            result = self.get_table('contact_types').select('id, name').execute()
            contact_type_map = {}
            for contact_type in result.data:
                contact_type_map[contact_type['name']] = contact_type['id']
            return contact_type_map
        except Exception as e:
            print(f"Warning: Could not load contact types from database: {e}")
            # Return empty dict as fallback
            return {}
    
    def _add_dry_run_error(self, operation: str, error: str, data: dict = None):
        """Add an error that would occur during actual migration."""
        self.dry_run_errors.append({
            'operation': operation,
            'error': error,
            'data': data or {}
        })
    
    def _execute_org_sql(self, sql: str, params: list = None):
        """Execute raw SQL for org schema operations (bypasses public. prefix issue)."""
        try:
            if params:
                result = self.supabase.rpc('exec_sql', {'sql': sql, 'params': params}).execute()
            else:
                # For now, use table operations but we'll need to handle the schema issue differently
                # This is a placeholder - the real fix needs raw SQL execution
                pass
            return result
        except Exception as e:
            raise e
    
    def _simulate_insert(self, table: str, data: dict, operation: str) -> bool:
        """Simulate a Supabase insert operation."""
        if self.dry_run:
            # Validate required fields exist
            try:
                if not isinstance(data, dict):
                    raise ValueError("Data must be a dictionary")
                if 'id' not in data:
                    raise ValueError("ID field is required")
                # Add more validation as needed
                return True
            except Exception as e:
                self._add_dry_run_error(operation, str(e), data)
                return False
        else:
            # Actual insert
            result = self.supabase.table(table).insert(data).execute()
            return bool(result.data)
    
    def _simulate_select(self, table: str, operation: str = "select") -> list:
        """Simulate a Supabase select operation."""
        try:
            result = self.supabase.table(table).select("*").execute()
            return result.data if result.data else []
        except Exception as e:
            if self.dry_run:
                self._add_dry_run_error(operation, str(e))
            return []
    
    async def create_migration_user(self) -> str:
        """Get hardcoded migration user ID and ensure it exists in organization_members."""
        if self.migration_user_id:
            return self.migration_user_id
            
        # Use different IDs for different foreign key references
        self.migration_user_id = "cb83e1aa-2c0b-4f82-85c3-0d47ef8876c8"  # private.users.id for storage_folders.created_by
        self.migration_org_user_id = "07193efb-ffa1-4ee9-95ec-bb4784f20954"  # organization_members.id for storage_files.uploaded_by
        
        if self.dry_run:
            print(f"[DRY RUN] Would use existing migration user: {self.migration_user_id}")
        else:
            print(f"✓ Using existing migration user: {self.migration_user_id}")
            
            # Ensure the user exists in private.users and private.organization_members
            try:
                # Check if user exists in private.users
                existing_user = self.supabase_private.table("users").select("id").eq("id", self.migration_user_id).execute()
                
                if not existing_user.data:
                    print(f"⚠️  Migration user not found in private.users")
                    print(f"Please ensure user with ID {self.migration_user_id} exists in private.users")
                else:
                    print(f"✓ Migration user found in private.users")
                    
                    # Also check organization_members
                    existing_member = self.supabase_private.table("organization_members").select("user_id").eq("user_id", self.migration_user_id).execute()
                    if existing_member.data:
                        print(f"✓ Migration user found in organization_members") 
                    
            except Exception as e:
                print(f"Could not verify users: {e}")
            
        return self.migration_user_id
    
    async def create_additional_tables(self):
        """Create custom_fields and notes tables."""
        
        custom_fields_sql = f'''
        CREATE TABLE IF NOT EXISTS "{self.target_org}".custom_fields (
            id uuid NOT NULL DEFAULT gen_random_uuid(),
            entity_type character varying(50) NOT NULL,
            entity_id uuid,
            field_name character varying(255) NOT NULL,
            field_data jsonb DEFAULT '{{}}'::jsonb,
            source_table character varying(100),
            source_id text,
            case_id uuid,
            contact_id uuid,
            created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT custom_fields_pkey PRIMARY KEY (id),
            CONSTRAINT custom_fields_case_id_fkey FOREIGN KEY (case_id) 
                REFERENCES "{self.target_org}".cases (id),
            CONSTRAINT custom_fields_contact_id_fkey FOREIGN KEY (contact_id) 
                REFERENCES "{self.target_org}".contacts (id)
        );
        '''
        
        notes_sql = f'''
        CREATE TABLE IF NOT EXISTS "{self.target_org}".notes (
            id uuid NOT NULL DEFAULT gen_random_uuid(),
            case_id uuid,
            contact_id uuid,
            note_type character varying(50) NOT NULL DEFAULT 'note',
            title character varying(500),
            content text,
            is_private boolean DEFAULT false,
            author_name character varying(255),
            source_table character varying(50),
            source_id text,
            original_date timestamp with time zone,
            created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT notes_pkey PRIMARY KEY (id),
            CONSTRAINT notes_case_id_fkey FOREIGN KEY (case_id) 
                REFERENCES "{self.target_org}".cases (id),
            CONSTRAINT notes_contact_id_fkey FOREIGN KEY (contact_id) 
                REFERENCES "{self.target_org}".contacts (id)
        );
        '''
        
        # Create indexes
        custom_fields_indexes = f'''
        CREATE INDEX IF NOT EXISTS idx_custom_fields_entity ON "{self.target_org}".custom_fields (entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_custom_fields_case_id ON "{self.target_org}".custom_fields (case_id);
        CREATE INDEX IF NOT EXISTS idx_custom_fields_source ON "{self.target_org}".custom_fields (source_table, source_id);
        '''
        
        notes_indexes = f'''
        CREATE INDEX IF NOT EXISTS idx_notes_case_id ON "{self.target_org}".notes (case_id);
        CREATE INDEX IF NOT EXISTS idx_notes_type ON "{self.target_org}".notes (note_type);
        CREATE INDEX IF NOT EXISTS idx_notes_date ON "{self.target_org}".notes (original_date);
        '''
        
        try:
            if self.dry_run:
                print("[DRY RUN] Would create additional tables")
                print("- custom_fields table with indexes")
                print("- notes table with indexes")
                self.dry_run_stats['tables_created'] += 2
            else:
                # Execute table creation (these would need to be run in Supabase SQL editor)
                print("=== SQL to create additional tables ===")
                print(custom_fields_sql)
                print(notes_sql)
                print(custom_fields_indexes) 
                print(notes_indexes)
                print("=== Please run the above SQL in Supabase SQL editor ===")
            
        except Exception as e:
            if self.dry_run:
                self._add_dry_run_error("create_additional_tables", str(e))
            else:
                print(f"Note: Please create additional tables manually in Supabase: {e}")
    
    async def check_missing_contact_types(self):
        """Check for any contact types missing from pre-existing set."""
        print("=== Checking for Missing Contact Types ===")
        
        # Get source data from Filevine tables
        source_data = self.get_source_table("persontype").select("*").execute()
        
        new_contact_types = []
        found_types = set()
        
        for record in source_data.data:
            type_name = (record.get('persontypename') or '').strip()
            if not type_name:
                continue
                
            found_types.add(type_name)
            
            # Check if this type already exists
            if type_name not in self.contact_type_ids:
                contact_type = {
                    'id': str(uuid.uuid4()),
                    'name': type_name,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                new_contact_types.append(contact_type)
                print(f"Found new contact type: {type_name}")
        
        # Insert any new contact types
        if new_contact_types:
            try:
                result = self.get_table("contact_types").insert(new_contact_types).execute()
                print(f"Added {len(result.data)} new contact types")
                
                # Update our mapping
                for ct in new_contact_types:
                    self.contact_type_ids[ct['name']] = ct['id']
                    
            except Exception as e:
                print(f"Error adding new contact types: {e}")
        else:
            print("No new contact types found")
        
        print(f"Total contact types available: {len(self.contact_type_ids)}")
        return self.contact_type_ids
    
    async def find_existing_contact(self, first_name: str, middle_name: str, last_name: str) -> Optional[str]:
        """Find existing contact by name combinations to avoid duplicates."""
        
        # Skip duplicate detection if table doesn't exist yet
        try:
            # Quick check if contacts table exists
            self.get_table("contacts").select("id").limit(1).execute()
        except Exception as e:
            if "does not exist" in str(e):
                # Table doesn't exist yet, so no duplicates possible
                return None
            # Other error, continue with duplicate detection
        
        # Clean up names
        first = first_name.strip() if first_name else ""
        middle = middle_name.strip() if middle_name else ""
        last = last_name.strip() if last_name else ""
        
        # Try various name combinations
        search_combinations = []
        
        if first and last:
            search_combinations.append(f"{first} {last}")
        if first and middle and last:
            search_combinations.append(f"{first} {middle} {last}")
        if middle and last:
            search_combinations.append(f"{middle} {last}")
        if first:
            search_combinations.append(first)
        if last:
            search_combinations.append(last)
        
        # Search for existing contacts
        for full_name_combo in search_combinations:
            try:
                result = self.get_table("contacts").select("id").eq("full_name", full_name_combo).limit(1).execute()
                if result.data:
                    return result.data[0]['id']
            except Exception:
                continue
                
        # Also try individual field matches
        try:
            result = self.get_table("contacts").select("id").eq("first_name", first).eq("last_name", last).limit(1).execute()
            if result.data:
                return result.data[0]['id']
        except Exception:
            pass
            
        return None
    
    async def migrate_contacts(self):
        """Migrate person -> contacts with JSONB fields and deduplication."""
        print("=== Migrating Contacts ===")
        
        # First, get total count of contacts
        try:
            total_response = self.get_source_table("person").select("id").execute()
            total_contacts = len(total_response.data)
            print(f"Total contacts to process: {total_contacts}")
        except Exception as e:
            print(f"Warning: Could not get total count: {e}")
            total_contacts = None
        
        # Get source data in batches
        batch_size = 1000
        offset = 0
        contact_id_map = {}
        duplicates_found = 0
        new_contacts_created = 0
        processed_total = 0
        
        # For now, just process all contacts in a single batch to avoid pagination issues
        print(f"Fetching all contacts in single batch")
        source_data = self.get_source_table("person").select("*").execute()
        
        if not source_data.data:
            print("No contact data found")
            return contact_id_map
        
        print(f"Got {len(source_data.data)} contacts to process")
        
        # Process as a single batch
        batches_to_process = [(source_data.data, 0)]
        
        for batch_data, batch_offset in batches_to_process:
                
            contacts_to_insert = []
            
            processed_count = 0
            for record in batch_data:
                processed_count += 1
                if processed_count % 50 == 0:
                    print(f"  Processing contact {processed_count}/{len(batch_data)}")
                
                # Parse name components with null safety
                first_name = (record.get('firstname') or '').strip()
                middle_name = (record.get('middlename') or '').strip()
                last_name = (record.get('lastname') or '').strip()
                full_name = f"{first_name} {middle_name} {last_name}".strip()
                
                # Clean up full_name
                full_name = ' '.join(full_name.split())  # Remove extra spaces
                if not full_name:
                    full_name = record.get('displayname', 'Unknown Contact')
                
                # Check for existing contact
                existing_id = await self.find_existing_contact(first_name, middle_name, last_name)
                if existing_id:
                    contact_id_map[record['id']] = existing_id
                    duplicates_found += 1
                    print(f"Found duplicate contact: {full_name} -> {existing_id}")
                    continue
                
                # Build JSONB fields
                addresses = []
                emails = []
                phones = []
                
                # Collect contact info from record
                address_fields = ['address1', 'address2', 'city', 'state', 'zip', 'country']
                if any(record.get(field) for field in address_fields):
                    address = {
                        'type': 'primary',
                        'address1': record.get('address1', ''),
                        'address2': record.get('address2', ''),
                        'city': record.get('city', ''),
                        'state': record.get('state', ''),
                        'zip': record.get('zip', ''),
                        'country': record.get('country', '')
                    }
                    addresses.append(address)
                
                if record.get('email'):
                    emails.append({
                        'type': 'primary',
                        'email': record.get('email'),
                        'is_primary': True
                    })
                
                phone_fields = ['phone', 'mobilephone', 'businessphone']
                for phone_type, field in zip(['primary', 'mobile', 'business'], phone_fields):
                    if record.get(field):
                        phones.append({
                            'type': phone_type,
                            'phone': record.get(field),
                            'is_primary': phone_type == 'primary'
                        })
                
                new_contact_id = str(uuid.uuid4())
                contact = {
                    'id': new_contact_id,
                    'first_name': first_name or 'Unknown',
                    'middle_name': middle_name or None,
                    'last_name': last_name or '',
                    'full_name': full_name,
                    'is_individual': True,
                    'addresses': addresses,
                    'emails': emails,
                    'phones': phones,
                    'contact_type_ids': [],  # Will populate later based on relationships
                    'tags_v2': [],
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                
                contacts_to_insert.append(contact)
                contact_id_map[record['id']] = new_contact_id
                new_contacts_created += 1
            
            # Insert batch
            if contacts_to_insert:
                if self.dry_run:
                    print(f"[DRY RUN] Would insert batch of {len(contacts_to_insert)} new contacts")
                    self.dry_run_stats['contacts_created'] += len(contacts_to_insert)
                    # Validate contact data structure
                    for contact in contacts_to_insert:
                        try:
                            if not contact.get('id') or not contact.get('full_name'):
                                raise ValueError(f"Missing required fields in contact: {contact.get('id', 'unknown')}")
                        except Exception as e:
                            self._add_dry_run_error("insert_contacts", str(e), contact)
                else:
                    try:
                        print(f"Attempting to insert {len(contacts_to_insert)} contacts...")
                        if len(contacts_to_insert) > 0:
                            print(f"Sample contact data: {contacts_to_insert[0]}")
                        result = self.get_table("contacts").insert(contacts_to_insert).execute()
                        print(f"Inserted batch of {len(result.data)} new contacts")
                    except Exception as e:
                        print(f"Error inserting contacts batch: {e}")
                        print(f"Exception type: {type(e)}")
                        if hasattr(e, '__dict__'):
                            print(f"Exception details: {e.__dict__}")
                        if self.dry_run:
                            self._add_dry_run_error("insert_contacts", str(e), {"batch_size": len(contacts_to_insert)})
        
        if self.dry_run:
            self.dry_run_stats['contacts_duplicates'] = duplicates_found
            print(f"[DRY RUN] Contact migration summary:")
            print(f"  - Would create new contacts: {new_contacts_created}")
            print(f"  - Duplicates found: {duplicates_found}")
            print(f"  - Total mapped: {len(contact_id_map)}")
        else:
            print(f"Contact migration complete:")
            print(f"  - New contacts created: {new_contacts_created}")
            print(f"  - Duplicates found: {duplicates_found}")
            print(f"  - Total mapped: {len(contact_id_map)}")
        
        return contact_id_map
    
    async def migrate_cases(self, contact_id_map: Dict[str, str]):
        """Migrate project -> cases with UUID case numbers."""
        print("=== Migrating Cases ===")
        
        # Get source data
        source_data = self.get_source_table("project").select("*").execute()
        
        cases_to_insert = []
        case_id_map = {}
        
        for record in source_data.data:
            case_id = str(uuid.uuid4())
            case_number = str(uuid.uuid4())  # UUID-based case number
            
            # Map case type
            filevine_type = record.get('customprojecttypename', '')
            target_case_type = self.filevine_case_type_map.get(filevine_type, 'litigation')  # Default to litigation
            case_type_id = self.case_type_ids.get(target_case_type)
            
            # Map claimant (client)
            claimant_id = None
            if record.get('clientid'):
                claimant_id = contact_id_map.get(str(record['clientid']))
            
            case = {
                'id': case_id,
                'case_number': case_number,
                'full_name': record.get('projectname', 'Unknown Case'),
                'case_type_id': case_type_id,
                'claimant_id': claimant_id,
                'status': 'active',  # Default status
                'created_at': record.get('createdate', datetime.now().isoformat()),
                'updated_at': datetime.now().isoformat()
            }
            
            cases_to_insert.append(case)
            case_id_map[record['id']] = case_id
            
            if len(cases_to_insert) % 100 == 0:
                print(f"Processed {len(cases_to_insert)} cases...")
        
        # Insert all cases
        if cases_to_insert:
            if self.dry_run:
                print(f"[DRY RUN] Would insert {len(cases_to_insert)} cases")
                self.dry_run_stats['cases_created'] = len(cases_to_insert)
                # Validate case data structure
                for case in cases_to_insert:
                    try:
                        if not case.get('id') or not case.get('case_number'):
                            raise ValueError(f"Missing required fields in case: {case.get('id', 'unknown')}")
                    except Exception as e:
                        self._add_dry_run_error("insert_cases", str(e), case)
            else:
                try:
                    print(f"Attempting to insert {len(cases_to_insert)} cases...")
                    if len(cases_to_insert) > 0:
                        print(f"Sample case data: {cases_to_insert[0]}")
                    result = self.get_table("cases").insert(cases_to_insert).execute()
                    print(f"Successfully migrated {len(result.data)} cases")
                except Exception as e:
                    print(f"Error migrating cases: {e}")
                    print(f"Exception type: {type(e)}")
                    if hasattr(e, '__dict__'):
                        print(f"Exception details: {e.__dict__}")
                    return {}
        
        if self.dry_run:
            print(f"[DRY RUN] Case migration summary: Would create {len(case_id_map)} cases")
        else:
            print(f"Case migration complete. Mapped {len(case_id_map)} cases.")
        return case_id_map

    def categorize_document(self, filename: str) -> str:
        """Categorize document based on filename patterns."""
        filename_lower = filename.lower()
        
        for category, patterns in self.document_categories.items():
            if category == 'documents':  # Skip default category
                continue
            for pattern in patterns:
                if pattern in filename_lower:
                    return category
        
        return 'documents'  # Default category

    async def create_case_folder_structure(self, case_id: str, case_name: str) -> Dict[str, str]:
        """Create folder structure for a case and return folder IDs."""
        folders_to_create = [
            ('Documents', 'documents'),
            ('Correspondence', 'correspondence'),
            ('Expert Reports', 'expert-reports'),
            ('Medical Records', 'medical-records'),
            ('Billing', 'billing')
        ]
        
        folder_id_map = {}
        
        # Create root folder for case
        root_folder_id = str(uuid.uuid4())
        root_folder = {
            'id': root_folder_id,
            'name': case_name,
            'parent_folder_id': None,
            'case_id': case_id,
            'path': f'/{case_name}',  # Root case folder path
            'created_by': self.migration_user_id,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        if self.dry_run:
            print(f"[DRY RUN] Would create root folder: {case_name}")
            folder_id_map['root'] = root_folder_id
            self.dry_run_stats['folders_created'] += 1
        else:
            try:
                self.get_table("storage_folders").insert(root_folder).execute()
                folder_id_map['root'] = root_folder_id
            except Exception as e:
                print(f"Error creating root folder for case {case_id}: {e}")
                return {}
        
        # Create subfolders
        for folder_name, category in folders_to_create:
            folder_id = str(uuid.uuid4())
            folder = {
                'id': folder_id,
                'name': folder_name,
                'parent_folder_id': root_folder_id,
                'case_id': case_id,
                'path': f'/{category}/{case_id}',  # Bucket hierarchy: category/caseId/
                'created_by': self.migration_user_id,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            if self.dry_run:
                print(f"[DRY RUN] Would create subfolder: {folder_name}")
                folder_id_map[category] = folder_id
                self.dry_run_stats['folders_created'] += 1
            else:
                try:
                    self.get_table("storage_folders").insert(folder).execute()
                    folder_id_map[category] = folder_id
                except Exception as e:
                    print(f"Error creating folder {folder_name} for case {case_id}: {e}")
        
        return folder_id_map

    async def migrate_documents(self, case_id_map: Dict[str, str]) -> Dict[str, str]:
        """Migrate documents from Filevine to target schema."""
        print("Starting document migration...")
        
        # Get all documents from public.doc table
        try:
            response = self.get_source_table("doc").select("*").execute()
            documents = response.data
            print(f"Found {len(documents)} documents to migrate")
        except Exception as e:
            print(f"Error fetching documents: {e}")
            return {}
        
        # Get all document revisions
        try:
            response = self.get_source_table("docrevision").select("*").execute()
            revisions = response.data
            print(f"Found {len(revisions)} document revisions")
        except Exception as e:
            print(f"Error fetching document revisions: {e}")
            revisions = []
        
        # Create revision map for latest versions
        revision_map = {}
        for revision in revisions:
            doc_id = str(revision.get('docid', ''))
            revision_number = revision.get('revisionnumber', 0) or 0
            
            if doc_id and (doc_id not in revision_map or revision_number > revision_map[doc_id].get('revisionnumber', 0)):
                revision_map[doc_id] = revision
        
        # Get projects to map document locations
        try:
            response = self.get_source_table("project").select("id, projectname").execute()
            projects = {str(p['id']): p['projectname'] for p in response.data}
        except Exception as e:
            print(f"Error fetching projects: {e}")
            projects = {}
        
        file_id_map = {}
        folders_created = set()
        
        for doc in documents:
            doc_id = str(doc['id'])
            project_id = str(doc.get('projectid', ''))
            
            # Skip if no associated case
            if project_id not in case_id_map:
                continue
            
            case_id = case_id_map[project_id]
            project_name = projects.get(project_id, f"Project_{project_id}")
            
            # Create folder structure for case if not already created
            if case_id not in folders_created:
                folder_map = await self.create_case_folder_structure(case_id, project_name)
                folders_created.add(case_id)
            else:
                # Get existing folder structure
                try:
                    response = self.get_table("storage_folders").select("*").eq("case_id", case_id).execute()
                    folders = response.data
                    folder_map = {}
                    for folder in folders:
                        if folder['parent_folder_id'] is None:
                            folder_map['root'] = folder['id']
                        else:
                            # Map by folder name to category
                            name_to_category = {
                                'Documents': 'documents',
                                'Correspondence': 'correspondence', 
                                'Expert Reports': 'expert-reports',
                                'Medical Records': 'medical-records',
                                'Billing': 'billing'
                            }
                            category = name_to_category.get(folder['name'])
                            if category:
                                folder_map[category] = folder['id']
                except Exception as e:
                    print(f"Error getting folder structure for case {case_id}: {e}")
                    continue
            
            # Get document filename and categorize
            filename = doc.get('filename', f"document_{doc_id}")
            category = self.categorize_document(filename)
            folder_id = folder_map.get(category, folder_map.get('documents'))
            
            if not folder_id:
                print(f"No folder found for document {doc_id} in case {case_id}")
                continue
            
            # Generate new file paths
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_filename = f"{timestamp}_{filename}"
            gcs_blob_name = f"{category}/{case_id}/{new_filename}"
            
            # Find actual folder name and create source paths
            actual_folder_name = self.find_actual_folder_name(project_name)
            source_blob_name = f"docs/Bennett Legal/{actual_folder_name}/{filename}"
            
            # Get file metadata from local file
            local_file_path = self.local_data_path / source_blob_name
            file_size = self.get_file_size(local_file_path)
            checksum = self.compute_file_checksum(local_file_path)
            mime_type = self.get_mime_type(filename)
            
            # Get latest revision info if available (fallback)
            revision_info = revision_map.get(doc_id, {})
            if file_size == 0:  # Use revision size if local file not found
                file_size = revision_info.get('filesize', 0)
            
            # Use fallback checksum if local file not found
            if checksum in ['file-not-found', 'checksum-error']:
                checksum = f"missing-{doc_id}"  # Unique placeholder for missing files
            
            # Create file record
            file_id = str(uuid.uuid4())
            file_record = {
                'id': file_id,
                'name': filename,
                'original_name': filename,
                'folder_id': folder_id,
                'gcs_blob_name': gcs_blob_name,
                'gcs_blob_url': f"gs://{self.dest_bucket_name}/{gcs_blob_name}",
                'mime_type': mime_type,
                'size_bytes': file_size,
                'checksum': checksum,
                'version': 1,
                'uploaded_by': self.migration_org_user_id,
                'created_at': doc.get('createdate', datetime.now().isoformat()),
                'updated_at': datetime.now().isoformat(),
                'is_encrypted': False,  # Files from migration are not encrypted initially
                'encryption_key_version': None
            }
            
            if self.dry_run:
                file_id_map[doc_id] = file_id
                self.dry_run_stats['files_created'] += 1
            else:
                try:
                    # First copy the file to the destination bucket
                    copy_success = self.copy_file_to_destination_bucket(source_blob_name, gcs_blob_name)
                    
                    if copy_success:
                        # Only create database record if file copy succeeded
                        self.get_table("storage_files").insert(file_record).execute()
                        file_id_map[doc_id] = file_id
                        print(f"✓ Migrated document {filename}")
                    else:
                        print(f"⚠️ Skipped database record for {filename} (file copy failed)")
                    
                except Exception as e:
                    print(f"Error creating file record for document {doc_id}: {e}")
            
            if len(file_id_map) % 100 == 0:
                if self.dry_run:
                    print(f"[DRY RUN] Would process {len(file_id_map)} documents...")
                else:
                    print(f"Processed {len(file_id_map)} documents...")
        
        print(f"Document migration complete. Migrated {len(file_id_map)} documents.")
        return file_id_map

    def get_mime_type(self, filename: str) -> str:
        """Get MIME type from filename extension."""
        mime_type, _ = mimetypes.guess_type(filename)
        return mime_type or 'application/octet-stream'
    
    def compute_file_checksum(self, file_path: Path) -> str:
        """Compute SHA256 checksum of a local file."""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except FileNotFoundError:
            print(f"Warning: Local file not found: {file_path}")
            return "file-not-found"
        except Exception as e:
            print(f"Error computing checksum for {file_path}: {e}")
            return "checksum-error"
    
    def get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes from local file."""
        try:
            return file_path.stat().st_size
        except FileNotFoundError:
            print(f"Warning: Local file not found: {file_path}")
            return 0
        except Exception as e:
            print(f"Error getting file size for {file_path}: {e}")
            return 0
    
    def find_actual_folder_name(self, project_name: str) -> str:
        """Find the actual folder name for a project, handling naming variations."""
        base_path = self.local_data_path / "docs" / "Bennett Legal"
        
        # Try exact match first
        exact_path = base_path / project_name
        if exact_path.exists():
            return project_name
        
        # Try common variations
        variations = [
            f"{project_name} (1)",
            f"{project_name} CLOSED",
            f"{project_name} (CLOSED)",
            f"({project_name.split()[-1] if ' ' in project_name else project_name}) {project_name}",  # (LastName) FullName pattern
        ]
        
        for variation in variations:
            var_path = base_path / variation
            if var_path.exists():
                return variation
        
        # If no variation found, return original (will fail but shows what was tried)
        return project_name

    def copy_file_to_destination_bucket(self, source_blob_name: str, dest_blob_name: str) -> bool:
        """Copy file from source bucket to destination bucket."""
        if self.dry_run:
            print(f"[DRY RUN] Would copy {self.source_bucket_name}/{source_blob_name} -> {self.dest_bucket_name}/{dest_blob_name}")
            return True
        
        try:
            # Debug: Check if bucket objects are properly initialized
            if isinstance(self.source_bucket, str):
                print(f"Error: source_bucket is string, not bucket object: {self.source_bucket}")
                return False
            if isinstance(self.dest_bucket, str):
                print(f"Error: dest_bucket is string, not bucket object: {self.dest_bucket}")
                return False
            
            # Check if source blob exists
            source_blob = self.source_bucket.blob(source_blob_name)
            if not source_blob.exists():
                print(f"Warning: Source file not found in GCS: {source_blob_name}")
                return False
            
            # Copy blob from source to destination
            dest_blob = self.dest_bucket.blob(dest_blob_name)
            dest_blob.rewrite(source_blob)
            print(f"✓ Copied {source_blob_name} -> {dest_blob_name}")
            return True
            
        except Exception as e:
            print(f"Error copying file {source_blob_name}: {e}")
            print(f"Debug - source_bucket type: {type(self.source_bucket)}")
            print(f"Debug - dest_bucket type: {type(self.dest_bucket)}")
            return False

    async def copy_gcs_file(self, source_blob_name: str, target_blob_name: str) -> bool:
        """Copy file from source bucket to target bucket."""
        if not self.gcs_client:
            print(f"Warning: GCS client not available, skipping file copy: {source_blob_name}")
            return False
            
        try:
            source_bucket = self.gcs_client.bucket(self.source_bucket)
            target_bucket = self.gcs_client.bucket(self.target_bucket)
            
            source_blob = source_bucket.blob(source_blob_name)
            if not source_blob.exists():
                print(f"Source file not found: gs://{self.source_bucket}/{source_blob_name}")
                return False
            
            # Copy blob to target bucket
            target_bucket.copy_blob(source_blob, target_bucket, target_blob_name)
            print(f"Copied: gs://{self.source_bucket}/{source_blob_name} -> gs://{self.target_bucket}/{target_blob_name}")
            return True
            
        except Exception as e:
            print(f"Error copying file {source_blob_name}: {e}")
            return False

    def print_dry_run_summary(self):
        """Print summary of what would happen in dry run mode."""
        if not self.dry_run:
            return
            
        print("\n" + "="*50)
        print("🧪 DRY RUN SUMMARY")
        print("="*50)
        
        print(f"📊 OPERATIONS THAT WOULD BE PERFORMED:")
        print(f"  • Users created: {self.dry_run_stats['users_created']}")
        print(f"  • Tables created: {self.dry_run_stats['tables_created']}")
        print(f"  • Contacts created: {self.dry_run_stats['contacts_created']}")
        print(f"  • Contact duplicates: {self.dry_run_stats['contacts_duplicates']}")
        print(f"  • Cases created: {self.dry_run_stats['cases_created']}")
        print(f"  • Folders created: {self.dry_run_stats['folders_created']}")
        print(f"  • Files created: {self.dry_run_stats['files_created']}")
        print(f"  • Custom fields created: {self.dry_run_stats['custom_fields_created']}")
        print(f"  • Notes created: {self.dry_run_stats['notes_created']}")
        
        if self.dry_run_errors:
            print(f"\n❌ ERRORS THAT WOULD OCCUR ({len(self.dry_run_errors)}):")
            for i, error in enumerate(self.dry_run_errors[:10], 1):  # Show first 10
                print(f"  {i}. {error['operation']}: {error['error']}")
            if len(self.dry_run_errors) > 10:
                print(f"  ... and {len(self.dry_run_errors) - 10} more errors")
        else:
            print(f"\n✅ NO ERRORS DETECTED - Migration should proceed smoothly!")
        
        print("="*50)

    async def get_existing_contact_mapping(self) -> Dict[str, str]:
        """Get mapping of source person IDs to target contact IDs."""
        print("Building contact ID mapping from existing data...")
        
        try:
            # Get all existing contacts
            contacts_result = self.get_table('contacts').select('id, first_name, last_name, full_name').execute()
            existing_contacts = contacts_result.data
            
            # Get all source persons
            persons_result = self.get_source_table('person').select('id, firstname, lastname, fullname').execute()
            source_persons = persons_result.data
            
            contact_id_map = {}
            
            for person in source_persons:
                person_id = str(person['id'])
                first_name = (person.get('firstname') or '').strip()
                last_name = (person.get('lastname') or '').strip()
                full_name = (person.get('fullname') or '').strip()
                
                # Try to find matching contact
                for contact in existing_contacts:
                    contact_first = (contact.get('first_name') or '').strip()
                    contact_last = (contact.get('last_name') or '').strip()
                    contact_full = (contact.get('full_name') or '').strip()
                    
                    # Match by name combinations
                    if ((first_name and last_name and 
                         contact_first == first_name and contact_last == last_name) or
                        (full_name and contact_full == full_name)):
                        contact_id_map[person_id] = contact['id']
                        break
            
            print(f"Mapped {len(contact_id_map)} persons to existing contacts")
            return contact_id_map
            
        except Exception as e:
            print(f"Error building contact mapping: {e}")
            return {}
    
    async def get_existing_case_mapping(self) -> Dict[str, str]:
        """Get mapping of source project IDs to target case IDs."""
        print("Building case ID mapping from existing data...")
        
        try:
            # Get all existing cases
            cases_result = self.get_table('cases').select('id, case_number, full_name').execute()
            existing_cases = cases_result.data
            
            # Get all source projects
            projects_result = self.get_source_table('project').select('id, projectname').execute()
            source_projects = projects_result.data
            
            case_id_map = {}
            
            for project in source_projects:
                project_id = str(project['id'])
                project_name = (project.get('projectname') or '').strip()
                
                # Try to find matching case
                for case in existing_cases:
                    case_name = (case.get('full_name') or '').strip()
                    
                    # Match by name
                    if project_name and case_name == project_name:
                        case_id_map[project_id] = case['id']
                        break
            
            print(f"Mapped {len(case_id_map)} projects to existing cases")
            return case_id_map
            
        except Exception as e:
            print(f"Error building case mapping: {e}")
            return {}
    
    async def _create_notes_table(self):
        """Show SQL to create the notes table in the target schema."""
        notes_sql = f'''
CREATE TABLE IF NOT EXISTS "{self.target_org}".notes (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    case_id uuid,
    contact_id uuid,
    note_type character varying(50) NOT NULL DEFAULT 'note',
    title character varying(500),
    content text,
    is_private boolean DEFAULT false,
    author_name character varying(255),
    source_table character varying(50),
    source_id text,
    original_date timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT notes_pkey PRIMARY KEY (id),
    CONSTRAINT notes_case_id_fkey FOREIGN KEY (case_id) 
        REFERENCES "{self.target_org}".cases (id),
    CONSTRAINT notes_contact_id_fkey FOREIGN KEY (contact_id) 
        REFERENCES "{self.target_org}".contacts (id)
);

CREATE INDEX IF NOT EXISTS idx_notes_case_id ON "{self.target_org}".notes (case_id);
CREATE INDEX IF NOT EXISTS idx_notes_type ON "{self.target_org}".notes (note_type);
CREATE INDEX IF NOT EXISTS idx_notes_date ON "{self.target_org}".notes (original_date);
'''
        
        print("=== SQL to create notes table ===")
        print(notes_sql)
        print("=== Please run the above SQL in Supabase SQL editor ===")
        
        raise Exception("Notes table does not exist. Please create it using the SQL above, then re-run the migration.")
    
    async def migrate_custom_fields(self, contact_id_map: Dict[str, str], case_id_map: Dict[str, str]) -> int:
        """Migrate custom fields from customfield table."""
        print("=== Migrating Custom Fields ===")
        
        try:
            # Get all custom fields from source
            result = self.get_source_table('customfield').select('*').execute()
            custom_fields = result.data
            print(f"Found {len(custom_fields)} custom fields to migrate")
            
            migrated_count = 0
            fields_to_insert = []
            
            for field in custom_fields:
                # Map the custom field data
                field_record = {
                    'id': str(uuid.uuid4()),
                    'entity_type': 'custom_field',  # Generic type for Filevine custom fields
                    'field_name': field.get('name', 'Unknown Field'),
                    'field_data': {
                        'internal_name': field.get('internalname'),
                        'field_type': field.get('customfieldtype'),
                        'notes': field.get('notes'),
                        'row': field.get('row'),
                        'order': field.get('orderinrow'),
                        'field_selector': field.get('fieldselector'),
                        'action_button_type': field.get('customactionbuttontype'),
                        'section_id': field.get('customsectionid')
                    },
                    'source_table': 'customfield',
                    'source_id': str(field['id']),
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                
                fields_to_insert.append(field_record)
                migrated_count += 1
            
            # Insert in batches
            if fields_to_insert:
                if self.dry_run:
                    print(f"[DRY RUN] Would insert {len(fields_to_insert)} custom fields")
                    self.dry_run_stats['custom_fields_created'] = len(fields_to_insert)
                else:
                    result = self.get_table('custom_fields').insert(fields_to_insert).execute()
                    print(f"Successfully migrated {len(result.data)} custom fields")
            
            return migrated_count
            
        except Exception as e:
            print(f"Error migrating custom fields: {e}")
            if self.dry_run:
                self._add_dry_run_error("migrate_custom_fields", str(e))
            return 0
    
    async def migrate_notes(self, contact_id_map: Dict[str, str], case_id_map: Dict[str, str]) -> int:
        """Migrate notes from note table."""
        print("=== Migrating Notes ===")
        
        # Check if notes table exists, create if needed
        try:
            self.get_table('notes').select('id').limit(1).execute()
        except Exception as e:
            if "does not exist" in str(e):
                print("Notes table doesn't exist. Creating it...")
                await self._create_notes_table()
            else:
                print(f"Warning: Error checking notes table: {e}")
        
        try:
            # Get all notes from source
            result = self.get_source_table('note').select('*').execute()
            notes = result.data
            print(f"Found {len(notes)} notes to migrate")
            
            migrated_count = 0
            notes_to_insert = []
            
            for note in notes:
                # Map note data
                note_record = {
                    'id': str(uuid.uuid4()),
                    'title': f"Note from {note.get('typetag', 'Filevine')}",
                    'content': note.get('body', ''),
                    'note_type': note.get('typetag', 'note'),
                    'author_name': f"User {note.get('authorid', 'Unknown')}",  # Would need to map to actual user
                    'is_private': False,  # Filevine doesn't have this concept
                    'source_table': 'note',
                    'source_id': str(note['id']),
                    'original_date': note.get('createdat'),
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                
                # Map to case if project ID exists (projects become cases)
                project_id = str(note.get('projectid')) if note.get('projectid') else None
                if project_id and project_id in case_id_map:
                    note_record['case_id'] = case_id_map[project_id]
                    # Don't set project_id since projects table doesn't exist - it's now cases
                
                notes_to_insert.append(note_record)
                migrated_count += 1
            
            # Insert in batches
            if notes_to_insert:
                if self.dry_run:
                    print(f"[DRY RUN] Would insert {len(notes_to_insert)} notes")
                    self.dry_run_stats['notes_created'] = len(notes_to_insert)
                else:
                    print(f"Attempting to insert {len(notes_to_insert)} notes...")
                    if len(notes_to_insert) > 0:
                        print(f"Sample note data: {notes_to_insert[0]}")
                    result = self.get_table('notes').insert(notes_to_insert).execute()
                    print(f"Successfully migrated {len(result.data)} notes")
            
            return migrated_count
            
        except Exception as e:
            print(f"Error migrating notes: {e}")
            if self.dry_run:
                self._add_dry_run_error("migrate_notes", str(e))
            return 0


async def main():
    """Run the schema migration."""
    config = load_config_from_env()
    migrator = SchemaMigrator(config)
    
    print("=== Starting Schema Migration ===")
    
    # Create migration user
    await migrator.create_migration_user()
    
    # Show additional table creation SQL
    await migrator.create_additional_tables()
    
    # Run migrations in order
    contact_type_map = await migrator.check_missing_contact_types()
    contact_id_map = await migrator.migrate_contacts()
    case_id_map = await migrator.migrate_cases(contact_id_map)
    file_id_map = await migrator.migrate_documents(case_id_map)
    
    print("=== Migration Complete ===")
    print(f"Contact types: {len(contact_type_map)}")
    print(f"Contacts: {len(contact_id_map)}")
    print(f"Cases: {len(case_id_map)}")
    print(f"Documents: {len(file_id_map)}")
    
    # Print dry run summary if in dry run mode
    migrator.print_dry_run_summary()


if __name__ == "__main__":
    asyncio.run(main())