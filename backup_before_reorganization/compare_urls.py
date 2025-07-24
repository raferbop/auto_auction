#!/usr/bin/env python3
"""
URL Comparison Script
Compares lot_link in vehicles table with url in processed_urls table
to identify discrepancies and duplicate records.
"""

import asyncio
import os
from datetime import datetime
from typing import Dict, List, Set, Tuple
from collections import defaultdict, Counter

# Import database handler
from db import DatabaseHandler

class URLComparator:
    def __init__(self):
        self.db_handler = DatabaseHandler()
        self.report = {
            'summary': {},
            'discrepancies': {
                'vehicles_only': [],
                'processed_urls_only': [],
                'duplicates': {
                    'vehicles': [],
                    'processed_urls': []
                },
                'mismatched_urls': []
            }
        }
    
    def connect(self):
        """Connect to database"""
        try:
            self.db_handler.connect()
            print("✅ Connected to database successfully")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            raise
    
    def get_table_counts(self) -> Dict[str, int]:
        """Get record counts for both tables"""
        try:
            # Count vehicles
            vehicles_result = self.db_handler.supabase_client.table("vehicles").select("count", count="exact").execute()
            vehicles_count = vehicles_result.count if hasattr(vehicles_result, 'count') else len(vehicles_result.data)
            
            # Count processed_urls
            urls_result = self.db_handler.supabase_client.table("processed_urls").select("count", count="exact").execute()
            urls_count = urls_result.count if hasattr(urls_result, 'count') else len(urls_result.data)
            
            return {
                'vehicles': vehicles_count,
                'processed_urls': urls_count
            }
        except Exception as e:
            print(f"❌ Error getting table counts: {e}")
            return {'vehicles': 0, 'processed_urls': 0}
    
    def get_vehicles_data(self) -> List[Dict]:
        """Get all vehicles with lot_link"""
        try:
            result = self.db_handler.supabase_client.table("vehicles").select(
                "id, site_name, lot_number, lot_link"
            ).not_.is_("lot_link", "null").execute()
            
            print(f"📊 Retrieved {len(result.data)} vehicles with lot_link")
            return result.data
        except Exception as e:
            print(f"❌ Error getting vehicles data: {e}")
            return []
    
    def get_processed_urls_data(self) -> List[Dict]:
        """Get all processed_urls records"""
        try:
            result = self.db_handler.supabase_client.table("processed_urls").select(
                "id, site_name, url, vehicle_id, processed"
            ).execute()
            
            print(f"📊 Retrieved {len(result.data)} processed_urls records")
            return result.data
        except Exception as e:
            print(f"❌ Error getting processed_urls data: {e}")
            return []
    
    def analyze_discrepancies(self):
        """Analyze discrepancies between tables"""
        print("\n🔍 Analyzing discrepancies...")
        
        # Get data from both tables
        vehicles = self.get_vehicles_data()
        processed_urls = self.get_processed_urls_data()
        
        # Create lookup dictionaries
        vehicles_by_id = {v['id']: v for v in vehicles}
        vehicles_by_lot_link = defaultdict(list)
        for v in vehicles:
            if v.get('lot_link'):
                vehicles_by_lot_link[v['lot_link']].append(v)
        
        processed_urls_by_id = {p['id']: p for p in processed_urls}
        processed_urls_by_url = defaultdict(list)
        for p in processed_urls:
            if p.get('url'):
                processed_urls_by_url[p['url']].append(p)
        
        # Find vehicles with duplicate lot_links
        duplicate_lot_links = {url: records for url, records in vehicles_by_lot_link.items() if len(records) > 1}
        
        # Find processed_urls with duplicate URLs
        duplicate_urls = {url: records for url, records in processed_urls_by_url.items() if len(records) > 1}
        
        # Find vehicles that don't have corresponding processed_urls
        vehicles_only = []
        for vehicle in vehicles:
            if vehicle.get('lot_link'):
                # Check if this lot_link exists in processed_urls
                matching_urls = processed_urls_by_url.get(vehicle['lot_link'], [])
                if not matching_urls:
                    vehicles_only.append(vehicle)
        
        # Find processed_urls that don't have corresponding vehicles
        processed_urls_only = []
        for url_record in processed_urls:
            if url_record.get('url'):
                # Check if this URL exists in vehicles
                matching_vehicles = vehicles_by_lot_link.get(url_record['url'], [])
                if not matching_vehicles:
                    processed_urls_only.append(url_record)
        
        # Find mismatched URLs (same vehicle_id but different URLs)
        mismatched_urls = []
        for vehicle in vehicles:
            if vehicle.get('lot_link'):
                # Find processed_urls with this vehicle_id
                for url_record in processed_urls:
                    if url_record.get('vehicle_id') == vehicle['id']:
                        if url_record.get('url') != vehicle.get('lot_link'):
                            mismatched_urls.append({
                                'vehicle': vehicle,
                                'processed_url': url_record
                            })
        
        # Store results
        self.report['discrepancies']['vehicles_only'] = vehicles_only
        self.report['discrepancies']['processed_urls_only'] = processed_urls_only
        self.report['discrepancies']['duplicates']['vehicles'] = duplicate_lot_links
        self.report['discrepancies']['duplicates']['processed_urls'] = duplicate_urls
        self.report['discrepancies']['mismatched_urls'] = mismatched_urls
        
        print(f"✅ Analysis complete")
    
    def generate_report(self):
        """Generate comprehensive report"""
        print("\n" + "="*80)
        print("🔍 URL DISCREPANCY ANALYSIS REPORT")
        print("="*80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Table counts
        counts = self.get_table_counts()
        print(f"\n📊 TABLE COUNTS:")
        print(f"   Vehicles: {counts['vehicles']:,}")
        print(f"   Processed URLs: {counts['processed_urls']:,}")
        print(f"   Difference: {counts['processed_urls'] - counts['vehicles']:,}")
        
        # Summary statistics
        discrepancies = self.report['discrepancies']
        
        print(f"\n📈 DISCREPANCY SUMMARY:")
        print(f"   Vehicles without URLs: {len(discrepancies['vehicles_only']):,}")
        print(f"   URLs without vehicles: {len(discrepancies['processed_urls_only']):,}")
        print(f"   Duplicate lot_links in vehicles: {len(discrepancies['duplicates']['vehicles']):,}")
        print(f"   Duplicate URLs in processed_urls: {len(discrepancies['duplicates']['processed_urls']):,}")
        print(f"   Mismatched URLs: {len(discrepancies['mismatched_urls']):,}")
        
        # Detailed breakdown
        if discrepancies['vehicles_only']:
            print(f"\n🚗 VEHICLES WITHOUT CORRESPONDING URLS ({len(discrepancies['vehicles_only'])}):")
            for i, vehicle in enumerate(discrepancies['vehicles_only'][:10], 1):  # Show first 10
                print(f"   {i}. ID: {vehicle['id']}, Site: {vehicle['site_name']}, Lot: {vehicle['lot_number']}, URL: {vehicle.get('lot_link', 'N/A')}")
            if len(discrepancies['vehicles_only']) > 10:
                print(f"   ... and {len(discrepancies['vehicles_only']) - 10} more")
        
        if discrepancies['processed_urls_only']:
            print(f"\n🔗 URLS WITHOUT CORRESPONDING VEHICLES ({len(discrepancies['processed_urls_only'])}):")
            for i, url_record in enumerate(discrepancies['processed_urls_only'][:10], 1):  # Show first 10
                print(f"   {i}. ID: {url_record['id']}, Site: {url_record['site_name']}, URL: {url_record.get('url', 'N/A')}, Vehicle ID: {url_record.get('vehicle_id', 'N/A')}")
            if len(discrepancies['processed_urls_only']) > 10:
                print(f"   ... and {len(discrepancies['processed_urls_only']) - 10} more")
        
        if discrepancies['duplicates']['vehicles']:
            print(f"\n🔄 DUPLICATE LOT_LINKS IN VEHICLES ({len(discrepancies['duplicates']['vehicles'])}):")
            for url, vehicles in list(discrepancies['duplicates']['vehicles'].items())[:5]:  # Show first 5
                print(f"   URL: {url}")
                for vehicle in vehicles:
                    print(f"     - ID: {vehicle['id']}, Site: {vehicle['site_name']}, Lot: {vehicle['lot_number']}")
            if len(discrepancies['duplicates']['vehicles']) > 5:
                print(f"   ... and {len(discrepancies['duplicates']['vehicles']) - 5} more duplicate URLs")
        
        if discrepancies['duplicates']['processed_urls']:
            print(f"\n🔄 DUPLICATE URLS IN PROCESSED_URLS ({len(discrepancies['duplicates']['processed_urls'])}):")
            for url, url_records in list(discrepancies['duplicates']['processed_urls'].items())[:5]:  # Show first 5
                print(f"   URL: {url}")
                for url_record in url_records:
                    print(f"     - ID: {url_record['id']}, Site: {url_record['site_name']}, Vehicle ID: {url_record.get('vehicle_id', 'N/A')}")
            if len(discrepancies['duplicates']['processed_urls']) > 5:
                print(f"   ... and {len(discrepancies['duplicates']['processed_urls']) - 5} more duplicate URLs")
        
        if discrepancies['mismatched_urls']:
            print(f"\n⚠️  MISMATCHED URLS ({len(discrepancies['mismatched_urls'])}):")
            for i, mismatch in enumerate(discrepancies['mismatched_urls'][:5], 1):  # Show first 5
                vehicle = mismatch['vehicle']
                url_record = mismatch['processed_url']
                print(f"   {i}. Vehicle ID: {vehicle['id']}")
                print(f"      Vehicle URL: {vehicle.get('lot_link', 'N/A')}")
                print(f"      Processed URL: {url_record.get('url', 'N/A')}")
            if len(discrepancies['mismatched_urls']) > 5:
                print(f"   ... and {len(discrepancies['mismatched_urls']) - 5} more mismatches")
        
        print("\n" + "="*80)
        print("📋 RECOMMENDATIONS:")
        
        if discrepancies['vehicles_only']:
            print("   • Run populate_processed_urls() to add missing URL records")
        
        if discrepancies['processed_urls_only']:
            print("   • Clean up orphaned URL records that don't correspond to vehicles")
        
        if discrepancies['duplicates']['vehicles'] or discrepancies['duplicates']['processed_urls']:
            print("   • Investigate and remove duplicate records")
        
        if discrepancies['mismatched_urls']:
            print("   • Fix mismatched URLs between vehicles and processed_urls tables")
        
        print("="*80)
    
    def close(self):
        """Close database connection"""
        try:
            self.db_handler.close()
            print("✅ Database connection closed")
        except Exception as e:
            print(f"❌ Error closing database connection: {e}")

def main():
    """Main execution function"""
    print("🔍 Starting URL Discrepancy Analysis...")
    
    comparator = URLComparator()
    
    try:
        # Connect to database
        comparator.connect()
        
        # Analyze discrepancies
        comparator.analyze_discrepancies()
        
        # Generate and display report
        comparator.generate_report()
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
    finally:
        comparator.close()

if __name__ == "__main__":
    main() 