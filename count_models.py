#!/usr/bin/env python3
"""
Script to count the number of models saved to the vehicle_sales table in Supabase database.
This script connects to the database and provides various model count statistics from sales data.
"""

import os
import sys
import csv
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.supabase_config import get_supabase_client, validate_config
from src.core.db import DatabaseHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ModelCounter:
    """Class to count and analyze models in the database"""
    
    def __init__(self):
        self.db_handler = None
        self.supabase_client = None
        
    def connect(self):
        """Connect to the database"""
        try:
            # Validate configuration first
            if not validate_config():
                logger.error("Invalid Supabase configuration. Please check your environment variables.")
                return False
            
            # Connect using the database handler
            self.db_handler = DatabaseHandler()
            self.db_handler.connect()
            self.supabase_client = self.db_handler.supabase_client
            
            logger.info("Successfully connected to Supabase database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from the database"""
        if self.db_handler:
            self.db_handler.close()
            logger.info("Disconnected from database")
    
    def count_total_models(self) -> int:
        """Count total number of unique models in the vehicle_sales table"""
        try:
            # Query to count distinct models
            result = self.supabase_client.table("vehicle_sales").select("model").not_.is_("model", "null").execute()
            
            if not result.data:
                return 0
            
            # Get unique models
            unique_models = set()
            for record in result.data:
                if record.get("model"):
                    unique_models.add(record["model"].strip())
            
            return len(unique_models)
            
        except Exception as e:
            logger.error(f"Error counting total models: {e}")
            return 0
    
    def count_models_by_make(self) -> Dict[str, int]:
        """Count models grouped by make"""
        try:
            result = self.supabase_client.table("vehicle_sales").select("make, model").not_.is_("make", "null").not_.is_("model", "null").execute()
            
            if not result.data:
                return {}
            
            make_model_counts = {}
            for record in result.data:
                make = record.get("make", "").strip()
                model = record.get("model", "").strip()
                
                if make and model:
                    if make not in make_model_counts:
                        make_model_counts[make] = set()
                    make_model_counts[make].add(model)
            
            # Convert sets to counts
            return {make: len(models) for make, models in make_model_counts.items()}
            
        except Exception as e:
            logger.error(f"Error counting models by make: {e}")
            return {}
    
    def count_models_by_site(self) -> Dict[str, int]:
        """Count models grouped by auction site"""
        try:
            result = self.supabase_client.table("vehicle_sales").select("site_name, model").not_.is_("site_name", "null").not_.is_("model", "null").execute()
            
            if not result.data:
                return {}
            
            site_model_counts = {}
            for record in result.data:
                site = record.get("site_name", "").strip()
                model = record.get("model", "").strip()
                
                if site and model:
                    if site not in site_model_counts:
                        site_model_counts[site] = set()
                    site_model_counts[site].add(model)
            
            # Convert sets to counts
            return {site: len(models) for site, models in site_model_counts.items()}
            
        except Exception as e:
            logger.error(f"Error counting models by site: {e}")
            return {}
    
    def get_model_list(self, limit: int = 50) -> List[str]:
        """Get a list of unique models (limited for display)"""
        try:
            result = self.supabase_client.table("vehicle_sales").select("model").not_.is_("model", "null").execute()
            
            if not result.data:
                return []
            
            unique_models = set()
            for record in result.data:
                if record.get("model"):
                    unique_models.add(record["model"].strip())
            
            # Return sorted list, limited to specified number
            return sorted(list(unique_models))[:limit]
            
        except Exception as e:
            logger.error(f"Error getting model list: {e}")
            return []
    
    def get_all_make_model_records(self) -> List[Dict]:
        """Get all individual records with make and model for CSV export"""
        try:
            # Get all records with make and model
            result = self.supabase_client.table("vehicle_sales").select("make, model").not_.is_("make", "null").not_.is_("model", "null").execute()
            
            if not result.data:
                return []
            
            # Process all records
            records = []
            for record in result.data:
                make = record.get("make", "").strip()
                model = record.get("model", "").strip()
                
                if make and model:
                    records.append({
                        "make": make,
                        "model": model
                    })
            
            return records
            
        except Exception as e:
            logger.error(f"Error getting all make-model records: {e}")
            return []

    def get_make_model_counts(self) -> List[Tuple[str, str, int]]:
        """Get make, model, and count for each unique make-model combination"""
        try:
            # Use a more efficient approach to get all records
            all_records = []
            offset = 0
            batch_size = 1000
            
            while True:
                result = self.supabase_client.table("vehicle_sales").select("make, model").not_.is_("make", "null").not_.is_("model", "null").range(offset, offset + batch_size - 1).execute()
                
                if not result.data:
                    break
                
                all_records.extend(result.data)
                offset += batch_size
                
                # If we got less than batch_size, we've reached the end
                if len(result.data) < batch_size:
                    break
            
            if not all_records:
                return []
            
            # Count occurrences of each make-model combination
            make_model_counts = {}
            for record in all_records:
                make = record.get("make", "").strip()
                model = record.get("model", "").strip()
                
                if make and model:
                    key = (make, model)
                    make_model_counts[key] = make_model_counts.get(key, 0) + 1
            
            # Convert to list of tuples and sort by count (descending)
            make_model_list = [(make, model, count) for (make, model), count in make_model_counts.items()]
            return sorted(make_model_list, key=lambda x: x[2], reverse=True)
            
        except Exception as e:
            logger.error(f"Error getting make-model counts: {e}")
            return []

    def generate_csv_report(self, filename: str = None) -> str:
        """Generate CSV report with make-model counts"""
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"make_model_counts_{timestamp}.csv"
            
            # Get make-model counts
            make_model_counts = self.get_make_model_counts()
            
            if not make_model_counts:
                logger.error("No make-model data found to export")
                return ""
            
            # Write to CSV
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['make', 'model', 'count']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for make, model, count in make_model_counts:
                    writer.writerow({
                        'make': make,
                        'model': model,
                        'count': count
                    })
            
            logger.info(f"CSV report generated: {filename}")
            logger.info(f"Total unique make-model combinations: {len(make_model_counts)}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Error generating CSV report: {e}")
            return ""

    def get_detailed_statistics(self) -> Dict:
        """Get comprehensive model statistics"""
        try:
            stats = {
                "timestamp": datetime.now().isoformat(),
                "total_models": self.count_total_models(),
                "models_by_make": self.count_models_by_make(),
                "models_by_site": self.count_models_by_site(),
                "sample_models": self.get_model_list(20),
                "make_model_counts": self.get_make_model_counts()
            }
            
            # Calculate additional statistics
            total_vehicles = 0
            try:
                vehicle_count_result = self.supabase_client.table("vehicle_sales").select("count").execute()
                total_vehicles = vehicle_count_result.data[0]['count'] if vehicle_count_result.data else 0
            except Exception as e:
                logger.error(f"Error getting total vehicle count: {e}")
            
            stats["total_vehicles"] = total_vehicles
            stats["models_per_vehicle_ratio"] = round(stats["total_models"] / total_vehicles, 3) if total_vehicles > 0 else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting detailed statistics: {e}")
            return {"error": str(e)}

def main():
    """Main function to run the model counting script"""
    print("=" * 60)
    print("VEHICLE SALES MODEL COUNTING SCRIPT")
    print("=" * 60)
    
    counter = ModelCounter()
    
    try:
        # Connect to database
        if not counter.connect():
            print("Failed to connect to database. Exiting.")
            return
        
        print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 60)
        
        # Generate CSV report
        print("Generating CSV report with make-model counts...")
        csv_filename = counter.generate_csv_report()
        
        if csv_filename:
            print(f"\nCSV report generated successfully: {csv_filename}")
            print(f"File saved in current directory")
            
            # Show summary counts
            print("\nSummary - Make-Model Counts:")
            make_model_counts = counter.get_make_model_counts()
            
            if make_model_counts:
                print("-" * 50)
                total_count = 0
                for make, model, count in make_model_counts:
                    print(f"{make} {model} - {count}")
                    total_count += count
                
                print("-" * 50)
                print(f"Total unique make-model combinations: {len(make_model_counts)}")
                print(f"Total records: {total_count:,}")
            else:
                print("No make-model data found.")
        else:
            print("Failed to generate CSV report.")
        
        print("\n" + "=" * 60)
        print("Report generation completed!")
        
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"An error occurred: {e}")
    finally:
        counter.disconnect()

if __name__ == "__main__":
    main() 