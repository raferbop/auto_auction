#!/usr/bin/env python3
"""
Extract Missing URLs Script

This script extracts the missing URLs from the URL comparison report
and saves them in a clean format for easy processing.
"""

import re
import sys
from datetime import datetime

def extract_missing_urls_from_report(report_filename):
    """Extract missing URLs from the report file"""
    missing_urls = []
    
    try:
        with open(report_filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the "MISSING URLS FROM VEHICLE_DETAILS" section
        start_marker = "MISSING URLS FROM VEHICLE_DETAILS"
        end_marker = "RECOMMENDATIONS"
        
        start_idx = content.find(start_marker)
        if start_idx == -1:
            print("Could not find missing URLs section in report")
            return []
        
        end_idx = content.find(end_marker, start_idx)
        if end_idx == -1:
            end_idx = len(content)
        
        section = content[start_idx:end_idx]
        
        # Extract URLs using regex
        url_pattern = r'https://[^\s]+'
        urls = re.findall(url_pattern, section)
        
        # Remove duplicates and clean up
        unique_urls = list(set(urls))
        unique_urls.sort()
        
        return unique_urls
        
    except Exception as e:
        print(f"Error reading report file: {e}")
        return []

def get_missing_records_from_database():
    """Get the complete missing records from the database"""
    from supabase import create_client
    import os
    
    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL", "https://lszhrgyiwekndcsnibcj.supabase.co")
    supabase_key = os.getenv("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxzemhyZ3lpd2VrbmRjc25pYmNqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI0MzgwNzQsImV4cCI6MjA2ODAxNDA3NH0.zoXJswUFoa7wagv9Y8t19bDYyB3N166NH1Pg6cmeGXs")
    
    client = create_client(supabase_url, supabase_key)
    
    try:
        # Get all processed_urls records
        print("Fetching processed_urls records...")
        processed_urls = []
        page_size = 1000
        offset = 0
        
        while True:
            result = client.table("processed_urls").select("*").range(offset, offset + page_size - 1).execute()
            if not result.data:
                break
            processed_urls.extend(result.data)
            offset += page_size
            print(f"Fetched {len(processed_urls)} processed_urls records so far...")
            
            if len(result.data) < page_size:
                break
        
        # Get all vehicle_details records
        print("Fetching vehicle_details records...")
        vehicle_details = []
        offset = 0
        
        while True:
            result = client.table("vehicle_details").select("*").range(offset, offset + page_size - 1).execute()
            if not result.data:
                break
            vehicle_details.extend(result.data)
            offset += page_size
            print(f"Fetched {len(vehicle_details)} vehicle_details records so far...")
            
            if len(result.data) < page_size:
                break
        
        # Create sets of URLs for comparison
        processed_urls_set = {record.get('url', '').strip() for record in processed_urls}
        vehicle_details_set = {record.get('url', '').strip() for record in vehicle_details}
        
        # Find missing URLs
        missing_urls_set = processed_urls_set - vehicle_details_set
        
        # Get the complete records for missing URLs
        missing_records = []
        for record in processed_urls:
            if record.get('url', '').strip() in missing_urls_set:
                missing_records.append(record)
        
        print(f"Found {len(missing_records)} missing records")
        return missing_records
        
    except Exception as e:
        print(f"Error fetching from database: {e}")
        return []
        
    except Exception as e:
        print(f"Error reading report file: {e}")
        return []

def save_records_to_file(records, output_filename):
    """Save complete records to a file"""
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(f"# Missing records from vehicle_details table\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# Total records: {len(records)}\n\n")
            
            for i, record in enumerate(records, 1):
                f.write(f"Record {i}:\n")
                f.write(f"  ID: {record.get('id', 'N/A')}\n")
                f.write(f"  Site: {record.get('site_name', 'N/A')}\n")
                f.write(f"  URL: {record.get('url', 'N/A')}\n")
                f.write(f"  Processed: {record.get('processed', 'N/A')}\n")
                f.write(f"  Error Message: {record.get('error_message', 'None')}\n")
                f.write(f"  Created: {record.get('created_at', 'N/A')}\n")
                f.write(f"  Updated: {record.get('last_updated', 'N/A')}\n")
                f.write("\n")
        
        print(f"Saved {len(records)} records to {output_filename}")
        
    except Exception as e:
        print(f"Error saving records to file: {e}")

def save_records_to_csv(records, output_filename):
    """Save complete records to CSV file"""
    try:
        import csv
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(['id', 'site_name', 'url', 'processed', 'error_message', 'created_at', 'last_updated'])
            
            # Write records
            for record in records:
                writer.writerow([
                    record.get('id', ''),
                    record.get('site_name', ''),
                    record.get('url', ''),
                    record.get('processed', ''),
                    record.get('error_message', ''),
                    record.get('created_at', ''),
                    record.get('last_updated', '')
                ])
        
        print(f"Saved {len(records)} records to {output_filename}")
        
    except Exception as e:
        print(f"Error saving CSV file: {e}")

def save_urls_to_file(urls, output_filename):
    """Save URLs to a file"""
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(f"# Missing URLs from vehicle_details table\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# Total URLs: {len(urls)}\n\n")
            
            for i, url in enumerate(urls, 1):
                f.write(f"{url}\n")
        
        print(f"Saved {len(urls)} URLs to {output_filename}")
        
    except Exception as e:
        print(f"Error saving URLs to file: {e}")

def main():
    """Main function"""
    print("Extracting missing records from database...")
    
    # Get missing records from database
    missing_records = get_missing_records_from_database()
    
    if not missing_records:
        print("No missing records found")
        return
    
    print(f"Found {len(missing_records)} missing records")
    
    # Save to different formats
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save complete records as text
    txt_filename = f"missing_records_{timestamp}.txt"
    save_records_to_file(missing_records, txt_filename)
    
    # Save complete records as CSV
    csv_filename = f"missing_records_{timestamp}.csv"
    save_records_to_csv(missing_records, csv_filename)
    
    # Save just URLs as text
    urls_only = [record.get('url', '') for record in missing_records]
    urls_filename = f"missing_urls_{timestamp}.txt"
    save_urls_to_file(urls_only, urls_filename)
    
    # Print summary
    print(f"\nSummary:")
    print(f"  - Total missing records: {len(missing_records)}")
    print(f"  - Complete records (text): {txt_filename}")
    print(f"  - Complete records (CSV): {csv_filename}")
    print(f"  - URLs only (text): {urls_filename}")
    
    # Show first few records as preview
    print(f"\nFirst 3 records:")
    for i, record in enumerate(missing_records[:3], 1):
        print(f"  {i}. ID: {record.get('id')}, Site: {record.get('site_name')}")
        print(f"     URL: {record.get('url')[:80]}...")
        print(f"     Processed: {record.get('processed')}")
        print()
    
    # Show breakdown by site
    site_counts = {}
    for record in missing_records:
        site = record.get('site_name', 'unknown')
        site_counts[site] = site_counts.get(site, 0) + 1
    
    print("Breakdown by site:")
    for site, count in sorted(site_counts.items()):
        print(f"  {site}: {count} records")

if __name__ == "__main__":
    main() 