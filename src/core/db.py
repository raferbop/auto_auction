# db.py
"""
Database Handler for Auction Data Collection System using Supabase
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import json
import os
from supabase import create_client, Client
from config.supabase_config import (
    SUPABASE_CONFIG, 
    DATABASE_CONFIG, 
    LOGGING_CONFIG,
    get_supabase_client
)

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)

class DatabaseHandler:
    """Main database handler for auction data operations using Supabase"""
    
    def __init__(self, use_service_role: bool = False):
        self.logger = logging.getLogger(__name__)
        self.use_service_role = use_service_role
        self.supabase_client = None
        self.connection = None  # For backward compatibility
        self.connected = False
    
    def connect(self):
        """Connect to Supabase database"""
        try:
            key = SUPABASE_CONFIG["service_role_key"] if self.use_service_role else SUPABASE_CONFIG["key"]
            self.supabase_client = create_client(SUPABASE_CONFIG["url"], key)
            self.connection = self.supabase_client  # For backward compatibility
            self.connected = True
            self.logger.info("Successfully connected to Supabase database")
            
            # Test connection
            self.supabase_client.table("vehicles").select("count").limit(1).execute()
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Supabase: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connected:
            self.supabase_client = None
            self.connection = None
            self.connected = False
            self.logger.info("Database connection closed")
    
    # Vehicle operations
    async def bulk_insert_vehicles_direct(self, listings: List[Dict]) -> int:
        """Insert vehicles directly into the main table"""
        if not listings:
            return 0
        
        try:
            # Prepare data for insertion
            vehicle_data = []
            for listing in listings:
                vehicle_record = {
                    "site_name": listing.get("site_name"),
                    "lot_number": listing.get("lot_number"),
                    "make": listing.get("make"),
                    "model": listing.get("model"),
                    "year": listing.get("year"),
                    "mileage": listing.get("mileage"),
                    "start_price": listing.get("start_price"),
                    "end_price": listing.get("end_price"),
                    "grade": listing.get("grade"),
                    "color": listing.get("color"),
                    "result": listing.get("result"),
                    "scores": listing.get("scores"),
                    "url": listing.get("url"),
                    "lot_link": listing.get("lot_link"),
                    "auction": listing.get("auction"),
                    "search_date": listing.get("search_date")
                }
                vehicle_data.append(vehicle_record)
            
            # Insert in batches
            batch_size = DATABASE_CONFIG["batch_size"]
            total_inserted = 0
            
            for i in range(0, len(vehicle_data), batch_size):
                batch = vehicle_data[i:i + batch_size]
                
                try:
                    result = self.supabase_client.table("vehicles").upsert(
                        batch, 
                        on_conflict="site_name,lot_number"
                    ).execute()
                    
                    total_inserted += len(batch)
                    # Removed verbose batch insert logging
                    
                except Exception as e:
                    self.logger.error(f"Error inserting batch: {e}")
                    continue
            
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Error in bulk vehicle insert: {e}")
            return 0
    
    async def bulk_insert_staging_concurrent(self, site_name: str, listings: List[Dict]) -> Tuple[int, int, int]:
        """Insert into staging table for processing"""
        if not listings:
            return 0, 0, 0
        
        try:
            # Deduplicate listings by site_name and lot_number
            unique_listings = {}
            for listing in listings:
                key = (listing.get("site_name"), listing.get("lot_number"))
                if key not in unique_listings:
                    unique_listings[key] = listing
            
            deduplicated_listings = list(unique_listings.values())
            
            # Prepare staging data
            staging_data = []
            for listing in deduplicated_listings:
                staging_record = {
                    "site_name": site_name,
                    "lot_number": listing.get("lot_number"),
                    "make": listing.get("make"),
                    "model": listing.get("model"),
                    "year": listing.get("year"),
                    "mileage": listing.get("mileage"),
                    "start_price": listing.get("start_price"),
                    "end_price": listing.get("end_price"),
                    "grade": listing.get("grade"),
                    "color": listing.get("color"),
                    "result": listing.get("result"),
                    "scores": listing.get("scores"),
                    "lot_link": listing.get("lot_link"),
                    "auction": listing.get("auction"),
                    "search_date": listing.get("search_date"),
                    "processed": False
                }
                staging_data.append(staging_record)
            
            # Insert into staging table
            result = self.supabase_client.table("staging_vehicles").insert(staging_data).execute()
            
            staged_count = len(result.data) if result.data else 0
            return staged_count, 0, 0  # staged, processed, duplicates
            
        except Exception as e:
            self.logger.error(f"Error in staging insert: {e}")
            return 0, 0, 0
    
    def process_staging_to_main(self) -> Tuple[int, int]:
        """Process staging data to main vehicles table"""
        try:
            # Get unprocessed staging records
            staging_result = self.supabase_client.table("staging_vehicles").select("*").eq("processed", False).execute()
            
            if not staging_result.data:
                return 0, 0
            
            staging_records = staging_result.data
            processed_count = 0
            duplicate_count = 0
            
            # Process in batches
            batch_size = DATABASE_CONFIG["batch_size"]
            
            for i in range(0, len(staging_records), batch_size):
                batch = staging_records[i:i + batch_size]
                
                # Prepare main table data
                main_data = []
                staging_ids = []
                
                for record in batch:
                    main_record = {
                        "site_name": record["site_name"],
                        "lot_number": record["lot_number"],
                        "make": record["make"],
                        "model": record["model"],
                        "year": record["year"],
                        "mileage": record["mileage"],
                        "start_price": record["start_price"],
                        "end_price": record["end_price"],
                        "grade": record["grade"],
                        "color": record["color"],
                        "result": record["result"],
                        "scores": record["scores"],
                        "lot_link": record["lot_link"],
                        "auction": record.get("auction"),
                        "search_date": record["search_date"]
                    }
                    main_data.append(main_record)
                    staging_ids.append(record["id"])
                
                try:
                    # Insert into main table
                    main_result = self.supabase_client.table("vehicles").upsert(
                        main_data,
                        on_conflict="site_name,lot_number"
                    ).execute()
                    
                    # Mark staging records as processed
                    self.supabase_client.table("staging_vehicles").update(
                        {"processed": True}
                    ).in_("id", staging_ids).execute()
                    
                    processed_count += len(batch)
                    
                except Exception as e:
                    self.logger.error(f"Error processing batch: {e}")
                    continue
            
            return processed_count, duplicate_count
            
        except Exception as e:
            self.logger.error(f"Error processing staging to main: {e}")
            return 0, 0
    
    def cleanup_staging(self) -> int:
        """Clean up old processed staging records"""
        try:
            # Delete processed records older than 7 days
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=7)
            
            result = self.supabase_client.table("staging_vehicles").delete().eq("processed", True).lt("created_at", cutoff_date.isoformat()).execute()
            
            cleaned_count = len(result.data) if result.data else 0
            # Removed verbose cleanup logging
            
            return cleaned_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up staging: {e}")
            return 0
    
    # Processed URLs operations
    def populate_processed_urls(self) -> Tuple[int, int]:
        """Populate processed URLs table from vehicles table"""
        try:
            # Get vehicles with lot_links that aren't in processed_urls
            vehicles_result = self.supabase_client.table("vehicles").select("id, site_name, lot_number, lot_link").neq("lot_link", None).execute()
            
            if not vehicles_result.data:
                return 0, 0
            
            vehicles = vehicles_result.data
            
            # Get existing processed URLs
            existing_result = self.supabase_client.table("processed_urls").select("site_name, url").execute()
            existing_urls = {(row["site_name"], row["url"]) for row in existing_result.data} if existing_result.data else set()
            
            # Find new URLs to add
            new_urls = []
            for vehicle in vehicles:
                if vehicle["lot_link"] and (vehicle["site_name"], vehicle["lot_link"]) not in existing_urls:
                    new_urls.append({
                        "site_name": vehicle["site_name"],
                        "url": vehicle["lot_link"],
                        "vehicle_id": vehicle["id"],
                        "processed": False
                    })
            
            if new_urls:
                # Insert new URLs
                result = self.supabase_client.table("processed_urls").insert(new_urls).execute()
                inserted_count = len(result.data) if result.data else 0
            else:
                inserted_count = 0
            
            skipped_count = len(vehicles) - inserted_count
            
            return inserted_count, skipped_count
            
        except Exception as e:
            self.logger.error(f"Error populating processed URLs: {e}")
            return 0, 0
    
    async def get_unprocessed_urls_concurrent(self, batch_size: int = 1000) -> List[Dict]:
        """Get unprocessed URLs for detailed extraction"""
        try:
            result = self.supabase_client.table("processed_urls").select("*").eq("processed", False).limit(batch_size).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            self.logger.error(f"Error getting unprocessed URLs: {e}")
            return []
    
    async def save_vehicle_details_concurrent(self, details_data: Dict) -> bool:
        """Save detailed vehicle information from auction pages"""
        try:
            if not self.supabase_client:
                self.logger.warning(f"Cannot save vehicle details - database not connected")
                return False
                
            # Map extracted data to database schema fields (only detailed/extended info)
            db_record = {
                "url": details_data.get("url"),
                "start_price": details_data.get("start_price"),
                "final_price": details_data.get("final_price"),
                "auction_date": details_data.get("auction_date"),
                "engine_size": details_data.get("engine_size") or details_data.get("displacement"),
                "transmission": details_data.get("transmission"),

                "type_code": details_data.get("type_code"),
                "chassis_number": details_data.get("chassis_number"),
                "interior_score": details_data.get("interior_score"),
                "exterior_score": details_data.get("exterior_score"),
                "equipment": details_data.get("equipment"),
                "auction_time": details_data.get("auction_time"),
                "displacement": details_data.get("displacement"),
                "image_urls": details_data.get("image_urls", []),
                "total_images": details_data.get("total_images", 0),
                "auction_sheet_url": details_data.get("auction_sheet_url"),
                "additional_info": {}
            }
            
            # Insert or update into vehicle_details table (handle duplicates)
            result = self.supabase_client.table("vehicle_details").upsert(
                db_record, 
                on_conflict="vehicle_id"
            ).execute()
            
            if result.data:
                self.logger.info(f"Saved vehicle details for record {details_data.get('processed_url_id')}")
                return True
            else:
                self.logger.error(f"Failed to save vehicle details for record {details_data.get('processed_url_id')}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error saving vehicle details: {e}")
            return False
    
    async def mark_record_processed_concurrent(self, record_id: int) -> bool:
        """Mark a record as processed in the processed_urls table"""
        try:
            update_data = {
                "processed": True,
                "processing_completed": datetime.now().isoformat()
            }
            
            result = self.supabase_client.table("processed_urls").update(update_data).eq("id", record_id).execute()
            
            if result.data:
                return True
            else:
                self.logger.error(f"Failed to mark record {record_id} as processed")
                return False
                
        except Exception as e:
            self.logger.error(f"Error marking record as processed: {e}")
            return False
    
    async def mark_url_failed_concurrent(self, record_id: int, error_message: str = None) -> bool:
        """Mark a URL as failed in the processed_urls table"""
        try:
            if not self.supabase_client:
                self.logger.warning(f"Cannot mark record {record_id} as failed - database not connected")
                return False
                
            update_data = {
                "processed": False,
                "processing_completed": datetime.now().isoformat(),
                "error_message": error_message
            }
            
            result = self.supabase_client.table("processed_urls").update(update_data).eq("id", record_id).execute()
            
            if result.data:
                return True
            else:
                self.logger.error(f"Failed to mark record {record_id} as failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Error marking URL as failed: {e}")
            return False
    
    def mark_url_processed(self, url_id: int, success: bool = True, error_message: str = None):
        """Mark a URL as processed"""
        try:
            update_data = {
                "processed": success,
                "processing_completed": datetime.now().isoformat()
            }
            
            if error_message:
                update_data["error_message"] = error_message
            
            self.supabase_client.table("processed_urls").update(update_data).eq("id", url_id).execute()
            
        except Exception as e:
            self.logger.error(f"Error marking URL as processed: {e}")
    
    # Detailed auction data operations
    async def save_detailed_auction_data(self, data: Dict) -> bool:
        """Save detailed auction data"""
        try:
            # Prepare detailed data (only detailed/extended info)
            detailed_record = {
                "url": data.get("url"),
                "start_price": data.get("start_price"),
                "final_price": data.get("final_price"),
                "auction_date": data.get("auction_date"),
                "engine_size": data.get("engine_size"),
                "transmission": data.get("transmission"),

                "type_code": data.get("type_code"),
                "chassis_number": data.get("chassis_number"),
                "interior_score": data.get("interior_score"),
                "exterior_score": data.get("exterior_score"),
                "equipment": data.get("equipment"),
                "auction_time": data.get("auction_time"),
                "displacement": data.get("displacement"),
                "image_urls": data.get("image_urls", []),
                "total_images": data.get("total_images", 0),
                "auction_sheet_url": data.get("auction_sheet_url"),
                "additional_info": data.get("additional_info")
            }
            
            # Insert or update
            result = self.supabase_client.table("vehicle_details").upsert(
                detailed_record,
                on_conflict="vehicle_id"
            ).execute()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving detailed auction data: {e}")
            return False
    
    # Verification and statistics
    def verify_data_movement(self) -> Dict:
        """Verify data movement between tables"""
        try:
            # Get counts from different tables
            staging_result = self.supabase_client.table("staging_vehicles").select("count").execute()
            main_result = self.supabase_client.table("vehicles").select("count").execute()
            urls_result = self.supabase_client.table("processed_urls").select("count").execute()
            
            return {
                "processed_count": len(staging_result.data) if staging_result.data else 0,
                "duplicate_count": 0,  # Would need more complex query
                "main_count": len(main_result.data) if main_result.data else 0,
                "urls_count": len(urls_result.data) if urls_result.data else 0
            }
            
        except Exception as e:
            self.logger.error(f"Error verifying data movement: {e}")
            return {}
    
    def verify_url_processing(self) -> Dict:
        """Verify URL processing statistics"""
        try:
            # Get URL processing statistics
            total_result = self.supabase_client.table("processed_urls").select("count").execute()
            processed_result = self.supabase_client.table("processed_urls").select("count").eq("processed", True).execute()
            
            total_count = len(total_result.data) if total_result.data else 0
            processed_count = len(processed_result.data) if processed_result.data else 0
            
            return {
                "total_vehicles": total_count,
                "processed_urls": processed_count,
                "unprocessed_urls": total_count - processed_count
            }
            
        except Exception as e:
            self.logger.error(f"Error verifying URL processing: {e}")
            return {}
    
    def verify_data_consistency(self) -> Dict:
        """Verify data consistency between all tables"""
        try:
            consistency_report = {
                'timestamp': datetime.now().isoformat(),
                'table_counts': {},
                'discrepancies': [],
                'recommendations': []
            }
            
            # Get counts for all tables
            tables = ['vehicles', 'staging_vehicles', 'processed_urls', 'vehicle_details']
            
            for table in tables:
                try:
                    result = self.supabase_client.table(table).select("count").execute()
                    count = result.data[0]['count'] if result.data else 0
                    consistency_report['table_counts'][table] = count
                except Exception as e:
                    self.logger.error(f"Error getting count for {table}: {e}")
                    consistency_report['table_counts'][table] = 0
            
            # Check for discrepancies
            vehicles_count = consistency_report['table_counts'].get('vehicles', 0)
            staging_count = consistency_report['table_counts'].get('staging_vehicles', 0)
            urls_count = consistency_report['table_counts'].get('processed_urls', 0)
            detailed_count = consistency_report['table_counts'].get('vehicle_details', 0)
            
            # Check staging vs vehicles discrepancy
            if staging_count > 0 and vehicles_count != staging_count:
                discrepancy = {
                    'type': 'staging_vehicles_mismatch',
                    'description': f'Staging vehicles ({staging_count}) != Vehicles ({vehicles_count})',
                    'difference': abs(vehicles_count - staging_count)
                }
                consistency_report['discrepancies'].append(discrepancy)
                consistency_report['recommendations'].append(
                    'Run process_staging_to_main() to move staging data to vehicles table'
                )
            
            # Check vehicles vs processed_urls discrepancy
            if vehicles_count > 0 and urls_count < vehicles_count * 0.8:  # Allow 20% tolerance
                discrepancy = {
                    'type': 'urls_mismatch',
                    'description': f'Processed URLs ({urls_count}) significantly less than Vehicles ({vehicles_count})',
                    'difference': vehicles_count - urls_count
                }
                consistency_report['discrepancies'].append(discrepancy)
                consistency_report['recommendations'].append(
                    'Run populate_processed_urls() to populate missing URL records'
                )
            
            # Check for vehicles without lot_links
            try:
                vehicles_without_lot_links = self.supabase_client.table("vehicles").select("count").is_("lot_link", "null").execute()
                vehicles_without_lot_links_count = vehicles_without_lot_links.data[0]['count'] if vehicles_without_lot_links.data else 0
                
                if vehicles_without_lot_links_count > 0:
                    discrepancy = {
                        'type': 'vehicles_without_lot_links',
                        'description': f'{vehicles_without_lot_links_count} vehicles have no lot_link',
                        'count': vehicles_without_lot_links_count
                    }
                    consistency_report['discrepancies'].append(discrepancy)
                    consistency_report['recommendations'].append(
                        'Review vehicle extraction to ensure lot_links are captured'
                    )
            except Exception as e:
                self.logger.error(f"Error checking vehicles without lot_links: {e}")
            
            # Check for unprocessed URLs
            try:
                unprocessed_urls = self.supabase_client.table("processed_urls").select("count").eq("processed", False).execute()
                unprocessed_count = unprocessed_urls.data[0]['count'] if unprocessed_urls.data else 0
                
                if unprocessed_count > 0:
                    consistency_report['table_counts']['unprocessed_urls'] = unprocessed_count
                    consistency_report['recommendations'].append(
                        f'Run extract_auction_data.py to process {unprocessed_count} unprocessed URLs'
                    )
            except Exception as e:
                self.logger.error(f"Error checking unprocessed URLs: {e}")
            
            return consistency_report
            
        except Exception as e:
            self.logger.error(f"Error in data consistency verification: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'table_counts': {},
                'discrepancies': [],
                'recommendations': ['Check database connection and permissions']
            }
    
    # Logging operations
    def log_extraction_process(self, site_name: str, operation: str, status: str, 
                             records_processed: int = 0, errors: int = 0, 
                             start_time: datetime = None, end_time: datetime = None,
                             error_message: str = None):
        """Log extraction process details"""
        try:
            duration = None
            if start_time and end_time:
                duration = int((end_time - start_time).total_seconds())
            
            log_record = {
                "site_name": site_name,
                "operation": operation,
                "status": status,
                "records_processed": records_processed,
                "errors": errors,
                "start_time": start_time.isoformat() if start_time else None,
                "end_time": end_time.isoformat() if end_time else None,
                "duration": duration,
                "error_message": error_message
            }
            
            self.supabase_client.table("extraction_logs").insert(log_record).execute()
            
        except Exception as e:
            self.logger.error(f"Error logging extraction process: {e}")
    
    # Configuration data operations
    def get_auction_sites(self) -> List[Dict]:
        """Get auction sites configuration"""
        try:
            result = self.supabase_client.table("auction_sites").select("*").eq("active", True).execute()
            return result.data if result.data else []
        except Exception as e:
            self.logger.error(f"Error getting auction sites: {e}")
            return []
    
    def get_manufacturers(self) -> List[Dict]:
        """Get manufacturers configuration"""
        try:
            result = self.supabase_client.table("manufacturers").select("*").eq("active", True).execute()
            return result.data if result.data else []
        except Exception as e:
            self.logger.error(f"Error getting manufacturers: {e}")
            return []
    
    async def save_sales_data(self, sales_record: Dict) -> bool:
        """
        Save a single sales data record to the vehicle_sales table and track the URL in processed_sales_urls.
        
        Args:
            sales_record: Dictionary containing sales data
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            # Truncate string fields to avoid "value too long" errors
            def truncate_string(value, max_length=50):
                if isinstance(value, str) and len(value) > max_length:
                    return value[:max_length]
                return value
            
            # Prepare the data for the vehicle_sales table with correct field mapping
            vehicle_sales_data = {
                "site_name": truncate_string(sales_record.get("site_name")),
                "lot_number": truncate_string(sales_record.get("lot_number")),
                "make": truncate_string(sales_record.get("make")),
                "model": truncate_string(sales_record.get("model")),
                "year": self._parse_year(sales_record.get("year")),
                "grade": truncate_string(sales_record.get("grade")),
                "model_type": truncate_string(sales_record.get("model_type")),
                "mileage": self._parse_mileage(sales_record.get("mileage")),
                "displacement": truncate_string(sales_record.get("displacement")),
                "transmission": truncate_string(sales_record.get("transmission")),
                "color": truncate_string(sales_record.get("color")),
                "auction": truncate_string(sales_record.get("auction")),
                "sale_date": self._parse_date(sales_record.get("sale_date")),
                "end_price": self._parse_price(sales_record.get("end_price")),
                "result": truncate_string(sales_record.get("result")),
                "scores": truncate_string(sales_record.get("scores")),
                "url": truncate_string(sales_record.get("url")),
                "lot_link": truncate_string(sales_record.get("lot_link")),
                "search_date": datetime.now().isoformat(),
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat()
            }
            
            # Remove None values to avoid database errors
            vehicle_sales_data = {k: v for k, v in vehicle_sales_data.items() if v is not None}
            
            # Check if record already exists to avoid duplicate key errors
            existing_record = self.supabase_client.table("vehicle_sales").select("id").eq(
                "site_name", vehicle_sales_data.get("site_name")
            ).eq("lot_number", vehicle_sales_data.get("lot_number")).execute()

            if existing_record.data:
                # Record already exists, use its id
                vehicle_sales_id = existing_record.data[0]["id"]
            else:
                # Insert into vehicle_sales table and get the new id
                result = self.supabase_client.table("vehicle_sales").insert(vehicle_sales_data).execute()
                if not result.data or not result.data[0].get("id"):
                    self.logger.error("Failed to insert vehicle_sales or retrieve new id.")
                    return False
                vehicle_sales_id = result.data[0]["id"]
            
            # Save the URL to processed_sales_urls table for tracking (1-to-1 relationship)
            if sales_record.get("url"):
                processed_url_data = {
                    "url": truncate_string(sales_record.get("url")),
                    "site_name": truncate_string(sales_record.get("site_name")),
                    "lot_number": truncate_string(sales_record.get("lot_number")),
                    "status": "completed",
                    "processed_at": datetime.now().isoformat(),
                    "created_at": datetime.now().isoformat(),
                    "error_message": None,
                    "vehicle_sales_id": vehicle_sales_id
                }
                processed_url_data = {k: v for k, v in processed_url_data.items() if v is not None}
                # Check if processed_sales_urls already has this url
                existing_url = self.supabase_client.table("processed_sales_urls").select("id").eq(
                    "url", processed_url_data.get("url")
                ).execute()
                if not existing_url.data:
                    self.supabase_client.table("processed_sales_urls").insert(processed_url_data).execute()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving sales data: {e}")
            return False
    
    def _parse_year(self, year_str: str) -> Optional[int]:
        """Parse year string to integer"""
        if not year_str:
            return None
        try:
            return int(year_str.strip())
        except (ValueError, AttributeError):
            return None
    
    def _parse_mileage(self, mileage_str: str) -> Optional[int]:
        """Parse mileage string to integer (remove spaces and 'km')"""
        if not mileage_str:
            return None
        try:
            # Remove spaces and 'km' if present
            cleaned = mileage_str.replace(" ", "").replace("km", "").strip()
            return int(cleaned)
        except (ValueError, AttributeError):
            return None
    
    def _parse_price(self, price_str: str) -> Optional[int]:
        """Parse price string to integer"""
        if not price_str:
            return None
        try:
            return int(price_str.strip())
        except (ValueError, AttributeError):
            return None
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None
        try:
            # Assuming date format is YYYY-MM-DD
            return date_str.strip()
        except AttributeError:
            return None

# Backward compatibility functions
def get_database_handler(use_service_role: bool = False) -> DatabaseHandler:
    """Get a configured database handler"""
    handler = DatabaseHandler(use_service_role)
    handler.connect()
    return handler

# Export main classes and functions
__all__ = [
    "DatabaseHandler",
    "get_database_handler"
] 