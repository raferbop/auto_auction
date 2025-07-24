#!/usr/bin/env python3
"""
URL Comparison Script for Auction Data Collection System

This script compares the URL and processed columns in the processed_urls table 
to the URL column in the vehicle_details table to identify discrepancies in 
record counts and data integrity issues.

The script will:
1. Count records in both tables
2. Compare URLs between tables
3. Identify missing records
4. Analyze processing status
5. Generate a detailed report
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Set
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('url_comparison.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class URLComparisonAnalyzer:
    def __init__(self):
        """Initialize the URL comparison analyzer"""
        # Try to get from environment variables first, then use defaults from supabase_config
        self.supabase_url = os.getenv("SUPABASE_URL", "https://lszhrgyiwekndcsnibcj.supabase.co")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxzemhyZ3lpd2VrbmRjc25pYmNqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI0MzgwNzQsImV4cCI6MjA2ODAxNDA3NH0.zoXJswUFoa7wagv9Y8t19bDYyB3N166NH1Pg6cmeGXs")
        
        self.client = create_client(self.supabase_url, self.supabase_key)
        self.report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'detailed_analysis': {},
            'discrepancies': [],
            'recommendations': []
        }
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            # Simple query to test connection
            result = self.client.table("processed_urls").select("count").limit(1).execute()
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def get_table_counts(self) -> Dict[str, int]:
        """Get record counts for all relevant tables"""
        counts = {}
        
        try:
            # Check processed_urls table - get actual count
            result = self.client.table("processed_urls").select("*", count="exact").execute()
            counts['processed_urls'] = result.count if result.count else 0
            logger.info(f"processed_urls table: {counts['processed_urls']} records")
            
            # Check vehicle_details table (try both possible names)
            try:
                result = self.client.table("vehicle_details").select("*", count="exact").execute()
                counts['vehicle_details'] = result.count if result.count else 0
                logger.info(f"vehicle_details table: {counts['vehicle_details']} records")
            except:
                try:
                    result = self.client.table("detailed_auction_data").select("*", count="exact").execute()
                    counts['detailed_auction_data'] = result.count if result.count else 0
                    logger.info(f"detailed_auction_data table: {counts['detailed_auction_data']} records")
                except Exception as e:
                    logger.error(f"Could not access vehicle_details or detailed_auction_data table: {e}")
                    counts['vehicle_details'] = 0
            
            # Check vehicles table for reference
            result = self.client.table("vehicles").select("*", count="exact").execute()
            counts['vehicles'] = result.count if result.count else 0
            logger.info(f"vehicles table: {counts['vehicles']} records")
            
        except Exception as e:
            logger.error(f"Error getting table counts: {e}")
        
        return counts
    
    def get_processed_urls_data(self) -> List[Dict]:
        """Get all data from processed_urls table"""
        try:
            # Get all records with pagination
            all_records = []
            page_size = 1000
            offset = 0
            
            while True:
                result = self.client.table("processed_urls").select("*").range(offset, offset + page_size - 1).execute()
                if not result.data:
                    break
                all_records.extend(result.data)
                offset += page_size
                logger.info(f"Fetched {len(all_records)} processed_urls records so far...")
                
                if len(result.data) < page_size:
                    break
            
            logger.info(f"Total processed_urls records fetched: {len(all_records)}")
            return all_records
        except Exception as e:
            logger.error(f"Error getting processed_urls data: {e}")
            return []
    
    def get_vehicle_details_data(self) -> List[Dict]:
        """Get all data from vehicle_details table (or detailed_auction_data)"""
        try:
            # Try vehicle_details first
            try:
                # Get all records with pagination
                all_records = []
                page_size = 1000
                offset = 0
                
                while True:
                    result = self.client.table("vehicle_details").select("*").range(offset, offset + page_size - 1).execute()
                    if not result.data:
                        break
                    all_records.extend(result.data)
                    offset += page_size
                    logger.info(f"Fetched {len(all_records)} vehicle_details records so far...")
                    
                    if len(result.data) < page_size:
                        break
                
                if all_records:
                    logger.info(f"Using vehicle_details table - Total records: {len(all_records)}")
                    return all_records
            except Exception as e:
                logger.error(f"Error accessing vehicle_details table: {e}")
            
            # Try detailed_auction_data
            try:
                # Get all records with pagination
                all_records = []
                page_size = 1000
                offset = 0
                
                while True:
                    result = self.client.table("detailed_auction_data").select("*").range(offset, offset + page_size - 1).execute()
                    if not result.data:
                        break
                    all_records.extend(result.data)
                    offset += page_size
                    logger.info(f"Fetched {len(all_records)} detailed_auction_data records so far...")
                    
                    if len(result.data) < page_size:
                        break
                
                if all_records:
                    logger.info(f"Using detailed_auction_data table - Total records: {len(all_records)}")
                    return all_records
            except Exception as e:
                logger.error(f"Error accessing detailed_auction_data table: {e}")
            
            logger.warning("Could not access vehicle_details or detailed_auction_data table")
            return []
            
        except Exception as e:
            logger.error(f"Error getting vehicle details data: {e}")
            return []
    
    def analyze_url_matching(self, processed_urls: List[Dict], vehicle_details: List[Dict]) -> Dict:
        """Analyze URL matching between tables"""
        analysis = {
            'total_processed_urls': len(processed_urls),
            'total_vehicle_details': len(vehicle_details),
            'matching_urls': 0,
            'missing_in_vehicle_details': 0,
            'missing_in_processed_urls': 0,
            'processing_status': {
                'processed': 0,
                'unprocessed': 0,
                'failed': 0
            }
        }
        
        # Create sets of URLs for comparison
        processed_urls_set = set()
        vehicle_details_urls_set = set()
        
        # Extract URLs from processed_urls
        logger.info("Extracting URLs from processed_urls...")
        for record in processed_urls:
            url = record.get('url', '').strip()
            if url:
                processed_urls_set.add(url)
            
            # Count processing status
            if record.get('processed'):
                analysis['processing_status']['processed'] += 1
            elif record.get('error_message'):
                analysis['processing_status']['failed'] += 1
            else:
                analysis['processing_status']['unprocessed'] += 1
        
        logger.info(f"Extracted {len(processed_urls_set)} unique URLs from processed_urls")
        
        # Extract URLs from vehicle_details
        logger.info("Extracting URLs from vehicle_details...")
        for record in vehicle_details:
            url = record.get('url', '').strip()
            if url:
                vehicle_details_urls_set.add(url)
        
        logger.info(f"Extracted {len(vehicle_details_urls_set)} unique URLs from vehicle_details")
        
        # Find matching URLs
        matching_urls = processed_urls_set.intersection(vehicle_details_urls_set)
        analysis['matching_urls'] = len(matching_urls)
        
        # Find missing URLs
        missing_in_vehicle_details = processed_urls_set - vehicle_details_urls_set
        missing_in_processed_urls = vehicle_details_urls_set - processed_urls_set
        
        analysis['missing_in_vehicle_details'] = len(missing_in_vehicle_details)
        analysis['missing_in_processed_urls'] = len(missing_in_processed_urls)
        
        logger.info(f"URL matching analysis complete:")
        logger.info(f"  - Matching URLs: {len(matching_urls)}")
        logger.info(f"  - Missing in vehicle_details: {len(missing_in_vehicle_details)}")
        logger.info(f"  - Missing in processed_urls: {len(missing_in_processed_urls)}")
        
        return analysis, matching_urls, missing_in_vehicle_details, missing_in_processed_urls
    
    def analyze_site_distribution(self, processed_urls: List[Dict], vehicle_details: List[Dict]) -> Dict:
        """Analyze distribution by site"""
        site_analysis = {}
        
        # Analyze processed_urls by site
        for record in processed_urls:
            site = record.get('site_name', 'unknown')
            if site not in site_analysis:
                site_analysis[site] = {
                    'processed_urls': 0,
                    'vehicle_details': 0,
                    'processed': 0,
                    'unprocessed': 0,
                    'failed': 0
                }
            
            site_analysis[site]['processed_urls'] += 1
            
            if record.get('processed'):
                site_analysis[site]['processed'] += 1
            elif record.get('error_message'):
                site_analysis[site]['failed'] += 1
            else:
                site_analysis[site]['unprocessed'] += 1
        
        # Analyze vehicle_details by site
        for record in vehicle_details:
            site = record.get('site_name', 'unknown')
            if site not in site_analysis:
                site_analysis[site] = {
                    'processed_urls': 0,
                    'vehicle_details': 0,
                    'processed': 0,
                    'unprocessed': 0,
                    'failed': 0
                }
            
            site_analysis[site]['vehicle_details'] += 1
        
        return site_analysis
    
    def find_specific_discrepancies(self, processed_urls: List[Dict], vehicle_details: List[Dict]) -> List[Dict]:
        """Find specific discrepancies and issues"""
        discrepancies = []
        
        # Create lookup dictionaries
        processed_urls_by_url = {record.get('url', '').strip(): record for record in processed_urls}
        vehicle_details_by_url = {record.get('url', '').strip(): record for record in vehicle_details}
        
        # Check for processed URLs that don't have corresponding vehicle details
        for url, processed_record in processed_urls_by_url.items():
            if url and url not in vehicle_details_by_url:
                discrepancies.append({
                    'type': 'missing_vehicle_details',
                    'id': processed_record.get('id'),
                    'url': url,
                    'site_name': processed_record.get('site_name'),
                    'processed': processed_record.get('processed'),
                    'error_message': processed_record.get('error_message'),
                    'created_at': processed_record.get('created_at'),
                    'last_updated': processed_record.get('last_updated'),
                    'description': f"URL is in processed_urls but missing from vehicle_details"
                })
        
        # Check for vehicle details that don't have corresponding processed URLs
        for url, vehicle_record in vehicle_details_by_url.items():
            if url and url not in processed_urls_by_url:
                discrepancies.append({
                    'type': 'missing_processed_url',
                    'id': vehicle_record.get('id'),
                    'url': url,
                    'site_name': vehicle_record.get('site_name'),
                    'created_at': vehicle_record.get('created_at'),
                    'description': f"URL is in vehicle_details but missing from processed_urls"
                })
        
        # Check for processing failures
        for url, processed_record in processed_urls_by_url.items():
            if processed_record.get('error_message'):
                discrepancies.append({
                    'type': 'processing_error',
                    'id': processed_record.get('id'),
                    'url': url,
                    'site_name': processed_record.get('site_name'),
                    'processed': processed_record.get('processed'),
                    'error_message': processed_record.get('error_message'),
                    'created_at': processed_record.get('created_at'),
                    'description': f"URL processing failed: {processed_record.get('error_message')}"
                })
        
        return discrepancies
    
    def generate_recommendations(self, analysis: Dict, discrepancies: List[Dict]) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        # Check for missing vehicle details
        if analysis.get('missing_in_vehicle_details', 0) > 0:
            recommendations.append(
                f"{analysis['missing_in_vehicle_details']} URLs are processed but missing from vehicle_details. "
                "Consider re-running the extraction process for these URLs."
            )
        
        # Check for processing failures
        failed_count = analysis.get('processing_status', {}).get('failed', 0)
        if failed_count > 0:
            recommendations.append(
                f"{failed_count} URLs have processing errors. "
                "Review error messages and consider retry mechanisms."
            )
        
        # Check for unprocessed URLs
        unprocessed_count = analysis.get('processing_status', {}).get('unprocessed', 0)
        if unprocessed_count > 0:
            recommendations.append(
                f"{unprocessed_count} URLs are still unprocessed. "
                "Consider running the extraction process to complete processing."
            )
        
        # Check for orphaned vehicle details
        if analysis.get('missing_in_processed_urls', 0) > 0:
            recommendations.append(
                f"{analysis['missing_in_processed_urls']} vehicle details records don't have corresponding processed_urls. "
                "This might indicate data integrity issues."
            )
        
        # General recommendations
        if not recommendations:
            recommendations.append("No major issues detected. Data appears to be consistent.")
        
        return recommendations
    
    def run_complete_analysis(self) -> Dict:
        """Run the complete URL comparison analysis"""
        logger.info("Starting URL comparison analysis...")
        
        # Test connection
        if not self.test_connection():
            return self.report
        
        # Get table counts
        counts = self.get_table_counts()
        self.report['summary']['table_counts'] = counts
        
        # Get data from tables
        processed_urls = self.get_processed_urls_data()
        vehicle_details = self.get_vehicle_details_data()
        
        if not processed_urls:
            logger.error("No data found in processed_urls table")
            return self.report
        
        # Analyze URL matching
        url_analysis, matching_urls, missing_in_vehicle_details, missing_in_processed_urls = self.analyze_url_matching(
            processed_urls, vehicle_details
        )
        self.report['detailed_analysis']['url_matching'] = url_analysis
        
        # Analyze site distribution
        site_analysis = self.analyze_site_distribution(processed_urls, vehicle_details)
        self.report['detailed_analysis']['site_distribution'] = site_analysis
        
        # Find specific discrepancies
        discrepancies = self.find_specific_discrepancies(processed_urls, vehicle_details)
        self.report['discrepancies'] = discrepancies
        
        # Generate recommendations
        recommendations = self.generate_recommendations(url_analysis, discrepancies)
        self.report['recommendations'] = recommendations
        
        # Print summary
        self.print_summary()
        
        return self.report
    
    def print_summary(self):
        """Print a summary of the analysis"""
        logger.info("\n" + "="*60)
        logger.info("URL COMPARISON ANALYSIS SUMMARY")
        logger.info("="*60)
        
        counts = self.report['summary']['table_counts']
        url_analysis = self.report['detailed_analysis']['url_matching']
        
        logger.info(f"Table Counts:")
        for table, count in counts.items():
            logger.info(f"   {table}: {count:,} records")
        
        logger.info(f"\nURL Matching:")
        logger.info(f"   Total processed URLs: {url_analysis['total_processed_urls']:,}")
        logger.info(f"   Total vehicle details: {url_analysis['total_vehicle_details']:,}")
        logger.info(f"   Matching URLs: {url_analysis['matching_urls']:,}")
        logger.info(f"   Missing in vehicle details: {url_analysis['missing_in_vehicle_details']:,}")
        logger.info(f"   Missing in processed URLs: {url_analysis['missing_in_processed_urls']:,}")
        
        logger.info(f"\nProcessing Status:")
        status = url_analysis['processing_status']
        logger.info(f"   Processed: {status['processed']:,}")
        logger.info(f"   Unprocessed: {status['unprocessed']:,}")
        logger.info(f"   Failed: {status['failed']:,}")
        
        logger.info(f"\nDiscrepancies Found: {len(self.report['discrepancies'])}")
        
        logger.info(f"\nRecommendations:")
        for rec in self.report['recommendations']:
            logger.info(f"   {rec}")
        
        logger.info("="*60)
    
    def save_report(self, filename: str = None):
        """Save the analysis report to a file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"url_comparison_report_{timestamp}.txt"
        
        try:
            with open(filename, 'w') as f:
                f.write("URL COMPARISON ANALYSIS REPORT\n")
                f.write("=" * 50 + "\n")
                f.write(f"Generated: {self.report['timestamp']}\n\n")
                
                # Summary
                f.write("SUMMARY\n")
                f.write("-" * 20 + "\n")
                counts = self.report['summary']['table_counts']
                for table, count in counts.items():
                    f.write(f"{table}: {count:,} records\n")
                f.write("\n")
                
                # Detailed Analysis
                f.write("DETAILED ANALYSIS\n")
                f.write("-" * 20 + "\n")
                url_analysis = self.report['detailed_analysis']['url_matching']
                f.write(f"Total processed URLs: {url_analysis['total_processed_urls']:,}\n")
                f.write(f"Total vehicle details: {url_analysis['total_vehicle_details']:,}\n")
                f.write(f"Matching URLs: {url_analysis['matching_urls']:,}\n")
                f.write(f"Missing in vehicle details: {url_analysis['missing_in_vehicle_details']:,}\n")
                f.write(f"Missing in processed URLs: {url_analysis['missing_in_processed_urls']:,}\n\n")
                
                # Site Distribution
                f.write("SITE DISTRIBUTION\n")
                f.write("-" * 20 + "\n")
                site_analysis = self.report['detailed_analysis']['site_distribution']
                for site, data in site_analysis.items():
                    f.write(f"{site}:\n")
                    f.write(f"  Processed URLs: {data['processed_urls']:,}\n")
                    f.write(f"  Vehicle Details: {data['vehicle_details']:,}\n")
                    f.write(f"  Processed: {data['processed']:,}\n")
                    f.write(f"  Unprocessed: {data['unprocessed']:,}\n")
                    f.write(f"  Failed: {data['failed']:,}\n\n")
                
                # Discrepancies
                f.write("DISCREPANCIES\n")
                f.write("-" * 20 + "\n")
                for i, disc in enumerate(self.report['discrepancies'], 1):  # Show all discrepancies
                    f.write(f"{i}. {disc['type']}: {disc['description']}\n")
                    f.write(f"   ID: {disc.get('id', 'N/A')}\n")
                    f.write(f"   URL: {disc['url']}\n")  # Show full URL
                    f.write(f"   Site: {disc['site_name']}\n")
                    f.write(f"   Processed: {disc.get('processed', 'N/A')}\n")
                    if disc.get('error_message'):
                        f.write(f"   Error: {disc['error_message']}\n")
                    f.write(f"   Created: {disc.get('created_at', 'N/A')}\n")
                    f.write("\n")
                
                # Missing URLs List
                f.write("\nMISSING URLS FROM VEHICLE_DETAILS\n")
                f.write("-" * 40 + "\n")
                missing_urls = [disc for disc in self.report['discrepancies'] if disc['type'] == 'missing_vehicle_details']
                f.write(f"Total missing URLs: {len(missing_urls)}\n\n")
                for i, disc in enumerate(missing_urls, 1):
                    f.write(f"{i:3d}. ID: {disc.get('id', 'N/A')} | Processed: {disc.get('processed', 'N/A')}\n")
                    f.write(f"     URL: {disc['url']}\n")
                    f.write(f"     Site: {disc['site_name']}\n")
                    if disc.get('error_message'):
                        f.write(f"     Error: {disc['error_message']}\n")
                    f.write("\n")
                
                # Recommendations
                f.write("RECOMMENDATIONS\n")
                f.write("-" * 20 + "\n")
                for rec in self.report['recommendations']:
                    f.write(f"• {rec}\n")
            
            logger.info(f"Report saved to: {filename}")
            
        except Exception as e:
            logger.error(f"Error saving report: {e}")


def main():
    """Main function to run the URL comparison analysis"""
    try:
        # Initialize analyzer
        analyzer = URLComparisonAnalyzer()
        
        # Run analysis
        report = analyzer.run_complete_analysis()
        
        # Save report
        analyzer.save_report()
        
        logger.info("URL comparison analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error running URL comparison analysis: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 