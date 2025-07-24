#!/usr/bin/env python3
"""
Analyze Missing URLs - Investigate why URLs are marked as processed but missing from vehicle_details
"""

import asyncio
import logging
from datetime import datetime
from db import DatabaseHandler
from config import auction_sites

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MissingUrlsAnalyzer:
    def __init__(self):
        self.db_handler = DatabaseHandler()
        
    async def connect_database(self):
        """Connect to Supabase database"""
        try:
            self.db_handler.connect()
            logger.info("✅ Connected to Supabase")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def get_missing_urls_details(self):
        """Get detailed information about missing URLs"""
        try:
            # Get all processed URLs
            processed_result = self.db_handler.supabase_client.table("processed_urls").select("*").execute()
            processed_urls = {row['url']: row for row in processed_result.data}
            
            # Get all vehicle_details URLs
            details_result = self.db_handler.supabase_client.table("vehicle_details").select("url").execute()
            details_urls = {row['url'] for row in details_result.data}
            
            # Find missing URLs
            missing_urls = []
            for url, record in processed_urls.items():
                if url not in details_urls:
                    missing_urls.append(record)
            
            return missing_urls
            
        except Exception as e:
            logger.error(f"❌ Error fetching missing URLs: {e}")
            return []

    def analyze_error_patterns(self, missing_urls):
        """Analyze patterns in missing URLs"""
        if not missing_urls:
            return {}
        
        analysis = {
            'by_site': {},
            'by_error_message': {},
            'by_processing_time': {},
            'total_missing': len(missing_urls)
        }
        
        for record in missing_urls:
            site_name = record.get('site_name', 'Unknown')
            error_message = record.get('error_message', 'No error message')
            processing_completed = record.get('processing_completed')
            
            # Count by site
            if site_name not in analysis['by_site']:
                analysis['by_site'][site_name] = 0
            analysis['by_site'][site_name] += 1
            
            # Count by error message
            if error_message not in analysis['by_error_message']:
                analysis['by_error_message'][error_message] = 0
            analysis['by_error_message'][error_message] += 1
            
            # Analyze processing time if available
            if processing_completed:
                try:
                    completed_time = datetime.fromisoformat(processing_completed.replace('Z', '+00:00'))
                    hour = completed_time.hour
                    if hour not in analysis['by_processing_time']:
                        analysis['by_processing_time'][hour] = 0
                    analysis['by_processing_time'][hour] += 1
                except:
                    pass
        
        return analysis

    def check_vehicle_id_relationship(self, missing_urls):
        """Check if missing URLs have valid vehicle_id relationships"""
        try:
            # Get all vehicle IDs from vehicles table
            vehicles_result = self.db_handler.supabase_client.table("vehicles").select("id").execute()
            valid_vehicle_ids = {row['id'] for row in vehicles_result.data}
            
            missing_with_valid_vehicle = 0
            missing_without_vehicle = 0
            
            for record in missing_urls:
                vehicle_id = record.get('vehicle_id')
                if vehicle_id and vehicle_id in valid_vehicle_ids:
                    missing_with_valid_vehicle += 1
                else:
                    missing_without_vehicle += 1
            
            return {
                'missing_with_valid_vehicle': missing_with_valid_vehicle,
                'missing_without_vehicle': missing_without_vehicle,
                'total_missing': len(missing_urls)
            }
            
        except Exception as e:
            logger.error(f"❌ Error checking vehicle relationships: {e}")
            return {}

    def generate_detailed_report(self, missing_urls, analysis, vehicle_analysis):
        """Generate a detailed analysis report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"missing_urls_analysis_{timestamp}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("MISSING URLS ANALYSIS REPORT\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary
            f.write("SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total missing URLs: {analysis.get('total_missing', 0)}\n")
            f.write(f"Missing with valid vehicle_id: {vehicle_analysis.get('missing_with_valid_vehicle', 0)}\n")
            f.write(f"Missing without vehicle_id: {vehicle_analysis.get('missing_without_vehicle', 0)}\n\n")
            
            # Analysis by site
            f.write("MISSING URLS BY SITE\n")
            f.write("-" * 40 + "\n")
            for site, count in analysis.get('by_site', {}).items():
                f.write(f"{site}: {count} URLs\n")
            f.write("\n")
            
            # Analysis by error message
            f.write("ERROR MESSAGE ANALYSIS\n")
            f.write("-" * 40 + "\n")
            for error, count in analysis.get('by_error_message', {}).items():
                f.write(f"{error}: {count} URLs\n")
            f.write("\n")
            
            # Analysis by processing time
            f.write("PROCESSING TIME ANALYSIS\n")
            f.write("-" * 40 + "\n")
            for hour, count in sorted(analysis.get('by_processing_time', {}).items()):
                f.write(f"Hour {hour:02d}:00 - {hour:02d}:59: {count} URLs\n")
            f.write("\n")
            
            # Detailed missing URLs list
            f.write("DETAILED MISSING URLS LIST\n")
            f.write("-" * 40 + "\n")
            for i, record in enumerate(missing_urls, 1):
                f.write(f"{i:3d}. ID: {record.get('id', 'N/A')}\n")
                f.write(f"     Site: {record.get('site_name', 'N/A')}\n")
                f.write(f"     URL: {record.get('url', 'N/A')}\n")
                f.write(f"     Vehicle ID: {record.get('vehicle_id', 'N/A')}\n")
                f.write(f"     Processed: {record.get('processed', 'N/A')}\n")
                f.write(f"     Error: {record.get('error_message', 'None')}\n")
                f.write(f"     Created: {record.get('created_at', 'N/A')}\n")
                f.write(f"     Completed: {record.get('processing_completed', 'N/A')}\n")
                f.write("\n")
        
        logger.info(f"📄 Detailed report saved to: {report_file}")
        return report_file

    async def run_analysis(self):
        """Run the complete analysis"""
        logger.info("🔍 Starting missing URLs analysis...")
        
        # Connect to database
        await self.connect_database()
        
        # Get missing URLs
        logger.info("📊 Fetching missing URLs...")
        missing_urls = self.get_missing_urls_details()
        
        if not missing_urls:
            logger.info("✅ No missing URLs found!")
            return
        
        logger.info(f"📊 Found {len(missing_urls)} missing URLs")
        
        # Analyze patterns
        logger.info("🔍 Analyzing error patterns...")
        analysis = self.analyze_error_patterns(missing_urls)
        
        # Check vehicle relationships
        logger.info("🔍 Checking vehicle relationships...")
        vehicle_analysis = self.check_vehicle_id_relationship(missing_urls)
        
        # Generate report
        logger.info("📄 Generating detailed report...")
        report_file = self.generate_detailed_report(missing_urls, analysis, vehicle_analysis)
        
        # Print summary
        print("\n" + "=" * 60)
        print("MISSING URLS ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Total missing URLs: {analysis.get('total_missing', 0)}")
        print(f"Missing with valid vehicle_id: {vehicle_analysis.get('missing_with_valid_vehicle', 0)}")
        print(f"Missing without vehicle_id: {vehicle_analysis.get('missing_without_vehicle', 0)}")
        
        print("\nBy Site:")
        for site, count in analysis.get('by_site', {}).items():
            print(f"  {site}: {count} URLs")
        
        print("\nBy Error Message:")
        for error, count in analysis.get('by_error_message', {}).items():
            print(f"  {error}: {count} URLs")
        
        print(f"\nDetailed report: {report_file}")
        print("=" * 60)

async def main():
    """Main function"""
    analyzer = MissingUrlsAnalyzer()
    await analyzer.run_analysis()

if __name__ == "__main__":
    asyncio.run(main()) 