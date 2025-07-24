#!/usr/bin/env python3
"""
Script to delete extracted data from Supabase database
Covers ALL tables: vehicles, staging_vehicles, processed_urls
"""

import asyncio
import sys
import os
from datetime import datetime
# Add the project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.db import DatabaseHandler
from config.supabase_config import SUPABASE_CONFIG

# Define all tables in the database (in deletion order to respect foreign keys)
ALL_TABLES = [
    'processed_urls',      # Delete first (references vehicles)
    'vehicle_details',     # Delete second (references vehicles)
    'staging_vehicles',    # Delete third (independent)
    'vehicles'            # Delete last (referenced by processed_urls and vehicle_details)
]

def print_banner():
    """Print script banner"""
    print("=" * 70)
    print("🗑️  SUPABASE DATA DELETION SCRIPT")
    print("=" * 70)
    print("This script will delete extracted auction data from your Supabase database.")
    print("⚠️  WARNING: This action cannot be undone!")
    print("=" * 70)

def get_user_confirmation(message: str) -> bool:
    """Get user confirmation for destructive operations"""
    while True:
        response = input(f"{message} (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no', '']:
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no.")

async def get_table_counts(db_handler: DatabaseHandler) -> dict:
    """Get record counts for all tables"""
    counts = {}
    
    for table in ALL_TABLES:
        try:
            result = db_handler.supabase_client.table(table).select("count").execute()
            counts[table] = result.data[0]['count'] if result.data else 0
        except Exception as e:
            print(f"⚠️  Error getting count for {table}: {e}")
            counts[table] = 0
    
    return counts

def display_table_counts(counts: dict):
    """Display current table counts"""
    print("\n📊 CURRENT DATABASE CONTENTS:")
    print("-" * 50)
    print(f"🔗 Processed URLs: {counts.get('processed_urls', 0):,} records")
    print(f"📋 Vehicle Details: {counts.get('vehicle_details', 0):,} records")
    print(f"📋 Staging Vehicles: {counts.get('staging_vehicles', 0):,} records")
    print(f"🚗 Vehicles: {counts.get('vehicles', 0):,} records")
    print("-" * 50)
    total = sum(counts.values())
    print(f"📈 TOTAL RECORDS: {total:,}")

async def delete_table_data(db_handler: DatabaseHandler, table_name: str) -> int:
    """Delete all data from a specific table"""
    try:
        print(f"🗑️  Deleting all data from {table_name}...")
        
        # First, get the count of records
        count_result = db_handler.supabase_client.table(table_name).select("count").execute()
        total_records = count_result.data[0]['count'] if count_result.data else 0
        
        if total_records == 0:
            print(f"📭 No records found in {table_name}")
            return 0
        
        # Delete all records from the table using a more reliable method
        result = db_handler.supabase_client.table(table_name).delete().gte("id", 0).execute()
        
        deleted_count = len(result.data) if result.data else 0
        print(f"✅ Deleted {deleted_count:,} records from {table_name}")
        
        return deleted_count
        
    except Exception as e:
        print(f"❌ Error deleting from {table_name}: {e}")
        # Try alternative deletion method
        try:
            print(f"🔄 Trying alternative deletion method for {table_name}...")
            result = db_handler.supabase_client.table(table_name).delete().execute()
            deleted_count = len(result.data) if result.data else 0
            print(f"✅ Alternative method deleted {deleted_count:,} records from {table_name}")
            return deleted_count
        except Exception as e2:
            print(f"❌ Alternative deletion also failed for {table_name}: {e2}")
            return 0

async def delete_all_data(db_handler: DatabaseHandler) -> int:
    """Delete all data from ALL tables in correct order to respect foreign keys"""
    print("\n🗑️  DELETING ALL DATA FROM ALL TABLES...")
    print("=" * 50)
    print("📋 Deletion order (respecting foreign key constraints):")
    print("   1. processed_urls (references vehicles)")
    print("   2. vehicle_details (references vehicles)")
    print("   3. staging_vehicles (independent)")
    print("   4. vehicles (referenced by processed_urls and vehicle_details)")
    print("=" * 50)
    
    total_deleted = 0
    
    # Delete in order to respect foreign key constraints
    for table in ALL_TABLES:
        deleted = await delete_table_data(db_handler, table)
        total_deleted += deleted
    
    print("=" * 50)
    print(f"🎉 DELETION COMPLETE!")
    print(f"📊 Total records deleted: {total_deleted:,}")
    
    return total_deleted

async def delete_specific_site_data(db_handler: DatabaseHandler, site_name: str) -> int:
    """Delete data for a specific auction site from ALL tables"""
    try:
        print(f"🗑️  Deleting data for site: {site_name}")
        print("-" * 40)
        
        total_deleted = 0
        
        # Delete in order to respect foreign key constraints
        for table in ALL_TABLES:
            try:
                result = db_handler.supabase_client.table(table).delete().eq("site_name", site_name).execute()
                deleted_count = len(result.data) if result.data else 0
                total_deleted += deleted_count
                print(f"  📋 {table}: {deleted_count:,} deleted")
            except Exception as e:
                print(f"  ❌ Error deleting from {table}: {e}")
        
        print("-" * 40)
        print(f"✅ Total deleted for {site_name}: {total_deleted:,} records")
        return total_deleted
        
    except Exception as e:
        print(f"❌ Error deleting data for {site_name}: {e}")
        return 0

async def delete_old_data(db_handler: DatabaseHandler, days_old: int) -> int:
    """Delete data older than specified days from ALL tables"""
    try:
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cutoff_str = cutoff_date.isoformat()
        
        print(f"🗑️  Deleting data older than {days_old} days (before {cutoff_date.strftime('%Y-%m-%d')})")
        print("-" * 60)
        
        total_deleted = 0
        
        # Delete in order to respect foreign key constraints
        for table in ALL_TABLES:
            try:
                result = db_handler.supabase_client.table(table).delete().lt("created_at", cutoff_str).execute()
                deleted_count = len(result.data) if result.data else 0
                total_deleted += deleted_count
                print(f"  📋 {table}: {deleted_count:,} deleted")
            except Exception as e:
                print(f"  ❌ Error deleting from {table}: {e}")
        
        print("-" * 60)
        print(f"✅ Total deleted: {total_deleted:,} records")
        return total_deleted
        
    except Exception as e:
        print(f"❌ Error deleting old data: {e}")
        return 0

async def main():
    """Main deletion script"""
    print_banner()
    
    # Initialize database connection
    try:
        # Try with regular key first
        db_handler = DatabaseHandler(use_service_role=False)
        db_handler.connect()
        print("✅ Connected to Supabase database (using regular key)")
    except Exception as e:
        print(f"⚠️  Regular key failed: {e}")
        try:
            # Try with service role key as fallback
            db_handler = DatabaseHandler(use_service_role=True)
            db_handler.connect()
            print("✅ Connected to Supabase database (using service role key)")
        except Exception as e2:
            print(f"❌ Both connection methods failed:")
            print(f"   Regular key error: {e}")
            print(f"   Service role key error: {e2}")
            return
    
    # Get current table counts
    counts = await get_table_counts(db_handler)
    display_table_counts(counts)
    
    # Check if there's any data to delete
    total_records = sum(counts.values())
    if total_records == 0:
        print("\n📭 No data found in database. Nothing to delete.")
        return
    
    # Show deletion options
    print("\n🗑️  DELETION OPTIONS:")
    print("1. Delete ALL extracted data (all tables)")
    print("2. Delete specific table data")
    print("3. Delete data for specific auction site")
    print("4. Delete old data (by date)")
    print("5. Exit without deleting")
    
    while True:
        choice = input("\nSelect option (1-5): ").strip()
        
        if choice == "1":
            # Delete all data
            print(f"\n⚠️  This will delete ALL data from ALL {len(ALL_TABLES)} tables:")
            for table in ALL_TABLES:
                print(f"   - {table}: {counts.get(table, 0):,} records")
            
            if get_user_confirmation("Are you sure you want to delete ALL extracted data?"):
                if get_user_confirmation("This will delete ALL data from ALL tables. Final confirmation?"):
                    await delete_all_data(db_handler)
                else:
                    print("❌ Deletion cancelled.")
            else:
                print("❌ Deletion cancelled.")
            break
                
        elif choice == "2":
            # Delete specific table
            print("\n📋 Available tables:")
            for i, table in enumerate(ALL_TABLES, 1):
                print(f"{i}. {table} ({counts.get(table, 0):,} records)")
            
            try:
                table_choice = int(input(f"Select table (1-{len(ALL_TABLES)}): ").strip())
                if 1 <= table_choice <= len(ALL_TABLES):
                    table_name = ALL_TABLES[table_choice - 1]
                    if get_user_confirmation(f"Delete all data from {table_name} ({counts.get(table_name, 0):,} records)?"):
                        await delete_table_data(db_handler, table_name)
                    else:
                        print("❌ Deletion cancelled.")
                else:
                    print("❌ Invalid choice.")
            except ValueError:
                print("❌ Please enter a valid number.")
            break
                
        elif choice == "3":
            # Delete specific site data
            site_name = input("Enter auction site name (e.g., AutoPacific, Zen Autoworks): ").strip()
            if site_name:
                if get_user_confirmation(f"Delete all data for site '{site_name}' from all tables?"):
                    await delete_specific_site_data(db_handler, site_name)
                else:
                    print("❌ Deletion cancelled.")
            else:
                print("❌ No site name provided.")
            break
                
        elif choice == "4":
            # Delete old data
            try:
                days = int(input("Delete data older than how many days? "))
                if days > 0:
                    if get_user_confirmation(f"Delete data older than {days} days from all tables?"):
                        await delete_old_data(db_handler, days)
                    else:
                        print("❌ Deletion cancelled.")
                else:
                    print("❌ Days must be greater than 0.")
            except ValueError:
                print("❌ Please enter a valid number.")
            break
                
        elif choice == "5":
            print("❌ Deletion cancelled.")
            break
            
        else:
            print("❌ Invalid choice. Please select 1-5.")
    
    # Show final counts
    print("\n📊 FINAL DATABASE CONTENTS:")
    final_counts = await get_table_counts(db_handler)
    display_table_counts(final_counts)
    
    # Close database connection
    db_handler.close()
    print("\n✅ Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main()) 