import asyncio
import aiohttp
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import time

# Add the project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.db import DatabaseHandler
from config.run_config import logging_config

# Setup logging
logging.basicConfig(
    level=logging_config['level'],
    format=logging_config['format'],
    handlers=[
        logging.FileHandler(f"logs/{logging_config['files']['main']}"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ImageExtractor:
    def __init__(self):
        self.db_handler = DatabaseHandler()
        self.session = None
        self.download_dir = "downloads/images"
        self.max_concurrent = 5
        self.timeout = 30
        self.retry_attempts = 3
        self.retry_delay = 2
        
        # Create download directory
        os.makedirs(self.download_dir, exist_ok=True)
        
    async def connect_database(self):
        """Connect to the database with retry logic"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.db_handler.connect()
                logger.info("✅ Database connected successfully")
                return
            except Exception as e:
                logger.warning(f"⚠️ Database connection attempt {attempt + 1} failed: {e}")
                
                if attempt < max_retries - 1:
                    logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"❌ Database connection failed after {max_retries} attempts")
                    logger.error("💡 Please check your internet connection and try again")
                    raise

    async def initialize_session(self):
        """Initialize aiohttp session for downloading images"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=2)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
            logger.info("✅ HTTP session initialized")

    async def get_vehicle_images(self, limit: int = None) -> List[Dict]:
        """Get vehicle records with image URLs that haven't been downloaded yet"""
        try:
            logger.info("📊 Fetching vehicle records with image URLs...")
            
            # Get vehicles with image URLs
            query = self.db_handler.supabase_client.table("vehicle_details").select(
                "id, vehicle_id, site_name, lot_number, image_urls, total_images, auction_sheet_url"
            ).not_.is_("image_urls", "null").not_.eq("image_urls", "[]")
            
            if limit:
                query = query.limit(limit)
            
            result = query.execute()
            
            if not result.data:
                logger.warning("⚠️ No vehicles with image URLs found")
                return []
            
            logger.info(f"📊 Found {len(result.data)} vehicles with image URLs")
            return result.data
            
        except Exception as e:
            logger.error(f"❌ Error fetching vehicle images: {e}")
            return []

    async def download_image(self, url: str, filepath: str) -> bool:
        """Download a single image with retry logic"""
        for attempt in range(self.retry_attempts):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Save the image
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        logger.info(f"  ✅ Downloaded: {os.path.basename(filepath)}")
                        return True
                    else:
                        logger.warning(f"  ⚠️ HTTP {response.status} for {url}")
                        
            except Exception as e:
                logger.warning(f"  ⚠️ Download attempt {attempt + 1} failed for {url}: {e}")
                
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"  ❌ Failed to download {url} after {self.retry_attempts} attempts")
        
        return False

    async def process_vehicle_images(self, vehicle: Dict) -> Dict:
        """Process all images for a single vehicle"""
        vehicle_id = vehicle['vehicle_id']
        site_name = vehicle['site_name']
        lot_number = vehicle['lot_number']
        image_urls = vehicle['image_urls']
        
        if not image_urls:
            return {'vehicle_id': vehicle_id, 'downloaded': 0, 'failed': 0, 'total': 0}
        
        logger.info(f"🖼️ Processing images for vehicle {vehicle_id} ({site_name} - Lot {lot_number})")
        
        # Create directory structure: downloads/images/site_name/lot_number/
        vehicle_dir = os.path.join(self.download_dir, site_name, str(lot_number))
        os.makedirs(vehicle_dir, exist_ok=True)
        
        downloaded = 0
        failed = 0
        
        # Download each image
        for i, url in enumerate(image_urls):
            if not url:
                continue
                
            # Generate filename
            filename = f"image_{i+1}.jpg"
            filepath = os.path.join(vehicle_dir, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                logger.info(f"  ⏭️ Skipped (exists): {filename}")
                downloaded += 1
                continue
            
            # Download the image
            if await self.download_image(url, filepath):
                downloaded += 1
            else:
                failed += 1
            
            # Small delay between downloads
            await asyncio.sleep(0.1)
        
        logger.info(f"  📊 Vehicle {vehicle_id}: {downloaded}/{len(image_urls)} images downloaded")
        return {
            'vehicle_id': vehicle_id,
            'downloaded': downloaded,
            'failed': failed,
            'total': len(image_urls)
        }

    async def run_extraction(self, limit: int = None):
        """Main extraction process"""
        logger.info("🚀 Starting image extraction process...")
        
        try:
            # Connect to database
            await self.connect_database()
            
            # Initialize HTTP session
            await self.initialize_session()
            
            # Get vehicles with images
            vehicles = await self.get_vehicle_images(limit)
            
            if not vehicles:
                logger.warning("⚠️ No vehicles with images to process")
                return
            
            logger.info(f"📦 Processing {len(vehicles)} vehicles...")
            
            # Process vehicles with concurrency control
            semaphore = asyncio.Semaphore(self.max_concurrent)
            
            async def process_with_semaphore(vehicle):
                async with semaphore:
                    return await self.process_vehicle_images(vehicle)
            
            # Process all vehicles concurrently
            tasks = [process_with_semaphore(vehicle) for vehicle in vehicles]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Calculate statistics
            total_downloaded = 0
            total_failed = 0
            total_images = 0
            successful_vehicles = 0
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"❌ Vehicle processing error: {result}")
                    continue
                
                if isinstance(result, dict):
                    total_downloaded += result['downloaded']
                    total_failed += result['failed']
                    total_images += result['total']
                    if result['downloaded'] > 0:
                        successful_vehicles += 1
            
            # Display final statistics
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 IMAGE EXTRACTION COMPLETE")
            logger.info(f"{'='*60}")
            logger.info(f"📦 Vehicles processed: {len(vehicles)}")
            logger.info(f"✅ Successful vehicles: {successful_vehicles}")
            logger.info(f"🖼️ Total images: {total_images}")
            logger.info(f"✅ Images downloaded: {total_downloaded}")
            logger.info(f"❌ Images failed: {total_failed}")
            logger.info(f"📁 Download directory: {os.path.abspath(self.download_dir)}")
            logger.info(f"{'='*60}")
            
        except Exception as e:
            logger.error(f"❌ Error during image extraction: {e}")
            raise
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up...")
        
        if self.session:
            await self.session.close()
            logger.info("✅ HTTP session closed")
        
        try:
            self.db_handler.close()
            logger.info("✅ Database connection closed")
        except:
            pass
        
        logger.info("✅ Cleanup complete")

async def main():
    """Main function"""
    extractor = ImageExtractor()
    
    try:
        # You can specify a limit to process only a subset of vehicles
        # await extractor.run_extraction(limit=10)  # Process only 10 vehicles
        await extractor.run_extraction()  # Process all vehicles
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        await extractor.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 