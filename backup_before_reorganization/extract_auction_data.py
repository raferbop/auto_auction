# extract_auction_data.py
"""
Simple, clean auction data extraction script
Workflow:
1. Fetch unprocessed URLs from Supabase
2. Show metrics per auction site
3. Login to each site
4. Open 10 tabs per browser
5. Process 10 URLs at a time
6. Extract data and repeat until all URLs processed
"""

import asyncio
import logging
import time
import re
from datetime import datetime
from typing import Dict, List, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from db import DatabaseHandler
from config import auction_sites
from standardizer import DataStandardizer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AuctionDataExtractor:
    def __init__(self):
        self.db_handler = DatabaseHandler()
        self.standardizer = DataStandardizer()
        self.browsers = {}  # site_name -> browser
        self.contexts = {}  # site_name -> context
        self.pages = {}     # site_name -> list of pages
        self.playwright = None
        
    async def connect_database(self):
        """Connect to Supabase database"""
        try:
            self.db_handler.connect()
            logger.info("✅ Connected to Supabase")
            
            # Check database schema
            await self.check_database_schema()
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    async def check_database_schema(self):
        """Check database schema matches expected structure"""
        try:
            logger.info("🔍 Checking database schema...")
            
            # Get a sample record to see available columns
            sample = self.db_handler.supabase_client.table("processed_urls").select("*").limit(1).execute()
            
            if sample.data:
                available_columns = list(sample.data[0].keys())
                logger.info(f"📋 Available columns in processed_urls: {available_columns}")
                
                # Check for required columns based on your schema
                required_columns = ['id', 'site_name', 'url', 'processed']
                missing_required = [col for col in required_columns if col not in available_columns]
                
                if missing_required:
                    logger.error(f"❌ Missing required columns: {missing_required}")
                    raise Exception(f"Database schema invalid - missing: {missing_required}")
                else:
                    logger.info("✅ All required columns found")
                    
                # Check for optional tracking columns
                tracking_columns = ['processing_started', 'processing_completed', 'error_message', 'retry_count']
                available_tracking = [col for col in tracking_columns if col in available_columns]
                logger.info(f"📊 Available tracking columns: {available_tracking}")
                
            else:
                logger.warning("⚠️ No data found in processed_urls table")
                
        except Exception as e:
            logger.error(f"❌ Schema check failed: {e}")
            raise

    def get_unprocessed_metrics(self):
        """Get metrics of unprocessed URLs per auction site"""
        try:
            # Query using your actual schema
            result = self.db_handler.supabase_client.table("processed_urls").select(
                "site_name"
            ).eq("processed", False).execute()
            
            # Count by site
            site_counts = {}
            for row in result.data:
                site_name = row['site_name']
                site_counts[site_name] = site_counts.get(site_name, 0) + 1
            
            return site_counts, len(result.data)
            
        except Exception as e:
            logger.error(f"❌ Error fetching metrics: {e}")
            return {}, 0

    def display_metrics(self):
        """Display URL metrics per auction site"""
        logger.info("📊 Fetching unprocessed URL metrics...")
        
        site_counts, total = self.get_unprocessed_metrics()
        
        print(f"\n{'='*60}")
        print(f"📊 UNPROCESSED URL METRICS")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        if site_counts:
            for site_name, count in site_counts.items():
                print(f"🔗 {site_name:20} : {count:,} URLs")
            print(f"{'='*60}")
            print(f"📈 TOTAL UNPROCESSED    : {total:,} URLs")
        else:
            print("✅ No unprocessed URLs found!")
        
        print(f"{'='*60}\n")
        return site_counts, total

    async def initialize_browsers(self):
        """Initialize browsers for each auction site"""
        self.playwright = await async_playwright().start()
        
        logger.info("🚀 Initializing browsers for auction sites...")
        
        for site_name in auction_sites.keys():
            try:
                logger.info(f"  📱 Setting up browser for {site_name}...")
                
                # Create browser
                browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=['--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage']
                )
                
                # Create context
                context = await browser.new_context(
                    viewport={'width': 1280, 'height': 800}
                )
                
                # Store browser and context
                self.browsers[site_name] = browser
                self.contexts[site_name] = context
                self.pages[site_name] = []
                
                logger.info(f"  ✅ Browser ready for {site_name}")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to initialize browser for {site_name}: {e}")
                raise
        
        logger.info("✅ All browsers initialized successfully")

    async def login_to_sites(self):
        """Login to each auction site"""
        logger.info("🔐 Logging into auction sites...")
        
        login_tasks = []
        for site_name in auction_sites.keys():
            task = asyncio.create_task(self._login_single_site(site_name))
            login_tasks.append(task)
        
        # Execute all logins concurrently
        results = await asyncio.gather(*login_tasks, return_exceptions=True)
        
        successful_logins = 0
        for i, result in enumerate(results):
            site_name = list(auction_sites.keys())[i]
            if isinstance(result, Exception):
                logger.error(f"  ❌ Login failed for {site_name}: {result}")
            else:
                successful_logins += 1
                logger.info(f"  ✅ Login successful for {site_name}")
        
        logger.info(f"🔐 Login complete: {successful_logins}/{len(auction_sites)} sites")
        return successful_logins

    async def _login_single_site(self, site_name: str):
        """Login to a single auction site"""
        try:
            # Get site config
            site_config = auction_sites[site_name]
            
            # Create a page for login
            page = await self.contexts[site_name].new_page()
            
            # Navigate to login page
            await page.goto(site_config['scraping']['auction_url'])
            
            # Check if already logged in
            try:
                await page.wait_for_selector('input[type="password"]', timeout=3000)
                # Login form found, perform login
                await page.fill('input[name="username"]#usr_name', site_config["username"])
                await page.fill('input[name="password"]#usr_pwd', site_config["password"])
                await page.click('input[name="Submit"][value="Sign in"]')
                await page.wait_for_load_state('networkidle', timeout=30000)
            except:
                # No login form, already logged in
                pass
            
            # Keep this page for future use
            self.pages[site_name].append(page)
            
            return True
            
        except Exception as e:
            logger.error(f"Login error for {site_name}: {e}")
            raise

    async def open_tabs_per_site(self):
        """Open 10 tabs per browser (per site)"""
        logger.info("📄 Opening 10 tabs per site...")
        
        for site_name in auction_sites.keys():
            try:
                logger.info(f"  📑 Opening tabs for {site_name}...")
                
                # We already have 1 page from login, create 9 more
                for i in range(9):
                    page = await self.contexts[site_name].new_page()
                    self.pages[site_name].append(page)
                
                logger.info(f"  ✅ {len(self.pages[site_name])} tabs ready for {site_name}")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to open tabs for {site_name}: {e}")
        
        total_tabs = sum(len(pages) for pages in self.pages.values())
        logger.info(f"📄 Total tabs opened: {total_tabs}")

    def get_unprocessed_urls(self, limit: int = 10):
        """Fetch unprocessed URLs from database based on actual schema"""
        try:
            # Query only the columns that exist in your schema
            result = self.db_handler.supabase_client.table("processed_urls").select(
                "id, site_name, url, vehicle_id"
            ).eq("processed", False).limit(limit).execute()
            
            return result.data
            
        except Exception as e:
            logger.error(f"❌ Error fetching URLs: {e}")
            return []

    async def extract_data_from_url(self, page: Page, site_name: str, url_record: Dict) -> Optional[Dict]:
        """Extract data from a single URL"""
        try:
            # Mark processing as started
            await self.mark_processing_started(url_record['id'])
            
            # Construct full URL
            base_urls = {
                'AutoPacific': 'https://auction.pacificcoastjdm.com',
                'Zervtek': 'https://auctions.zervtek.com',
                'Manga Auto Import': 'https://auc.mangaautoimport.ca',
                'Japan Car Auc': 'https://auc.japancarauc.com',
                'Zen Autoworks': 'https://auction.zenautoworks.ca'
            }
            
            base_url = base_urls.get(site_name, '')
            url = url_record.get('url', '')
            
            if not url:
                await self.mark_processing_failed(url_record['id'], "No URL provided")
                return None
                
            full_url = url if url.startswith('http') else f"{base_url}{url}"
            
            # Extract lot number from URL
            lot_number = self.extract_lot_number_from_url(url)
            
            # Navigate to URL
            await page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
            
            # Wait for content
            try:
                await page.wait_for_selector('table, td.ColorCell_1', timeout=10000)
            except:
                pass  # Continue even if no specific content found
            
            # Extract data using JavaScript - targeting specific HTML structure
            data = await page.evaluate("""() => {
                // Extract model name from Verdana16px div
                const getModelName = () => {
                    const modelDiv = document.querySelector('div.Verdana16px');
                    if (modelDiv) {
                        return modelDiv.textContent.trim().replace(/&nbsp;/g, ' ');
                    }
                    return '';
                };
                
                // Extract data from the main data table (bgcolor="#D8D8D8")
                const getTableData = () => {
                    const data = {};
                    const table = document.querySelector('table[bgcolor="#D8D8D8"]');
                    
                    if (table) {
                        const rows = table.querySelectorAll('tr');
                        rows.forEach(row => {
                            const cells = row.querySelectorAll('td');
                            for (let i = 0; i < cells.length; i += 2) {
                                const labelCell = cells[i];
                                const valueCell = cells[i + 1];
                                
                                if (labelCell && valueCell && labelCell.classList.contains('ColorCell_1')) {
                                    const label = labelCell.textContent.trim().toLowerCase();
                                    const value = valueCell.textContent.trim();
                                    
                                    // Map labels to data fields
                                    if (label.includes('type')) data.type_code = value;
                                    else if (label.includes('year')) data.year = value;
                                    else if (label.includes('scores')) data.scores = value;
                                    else if (label.includes('start price')) data.start_price = value;
                                    else if (label.includes('mileage')) data.mileage = value;
                                    else if (label.includes('interior score')) data.interior_score = value;
                                    else if (label.includes('final price')) data.final_price = value;
                                    else if (label.includes('transmission')) data.transmission = value;
                                    else if (label.includes('displacement')) data.displacement = value;
                                    else if (label.includes('exterior score')) data.exterior_score = value;
                                    else if (label.includes('result')) data.result = value;
                                    else if (label.includes('color')) data.color = value;
                                    else if (label.includes('equipment')) data.equipment = value;
                                    else if (label.includes('time')) data.auction_time = value;
                                }
                            }
                        });
                    }
                    
                    return data;
                };
                
                // Extract image URLs
                const getImageUrls = () => {
                    const imageUrls = [];
                    const imageLinks = document.querySelectorAll('a[href*="system=auto"]');
                    
                    imageLinks.forEach(link => {
                        const href = link.href;
                        if (href && href.includes('pic/?system=auto') && !href.includes('&h=')) {
                            imageUrls.push(href);
                        }
                    });
                    
                    return imageUrls;
                };
                
                // Extract auction sheet URL (first image is usually the auction sheet)
                const getAuctionSheetUrl = () => {
                    const firstImageLink = document.querySelector('a[href*="system=auto"][href*="number=0"]');
                    return firstImageLink ? firstImageLink.href : '';
                };
                
                // Combine all extracted data
                const modelName = getModelName();
                const tableData = getTableData();
                const imageUrls = getImageUrls();
                const auctionSheetUrl = getAuctionSheetUrl();
                
                // Debug logging
                console.log('Extracted model name:', modelName);
                console.log('Extracted table data:', tableData);
                console.log('Found images:', imageUrls.length);
                
                return {
                    model_name: modelName,
                    type_code: tableData.type_code || '',
                    year: tableData.year || '',
                    scores: tableData.scores || '',
                    start_price: tableData.start_price || '',
                    mileage: tableData.mileage || '',
                    interior_score: tableData.interior_score || '',
                    final_price: tableData.final_price || '',
                    transmission: tableData.transmission || '',
                    displacement: tableData.displacement || '',
                    exterior_score: tableData.exterior_score || '',
                    result: tableData.result || '',
                    color: tableData.color || '',
                    equipment: tableData.equipment || '',
                    auction_time: tableData.auction_time || '',
                    auction_sheet_url: auctionSheetUrl,
                    image_urls: imageUrls,
                    total_images: imageUrls.length
                };
            }""")
            
            # Log extraction debug info
            logger.debug(f"    🔍 Extracted: {data.get('model', 'No model')} | "
                        f"Price: {data.get('start_price', 'N/A')} | "
                        f"Images: {data.get('total_images', 0)}")
            
            # Parse make and model from model_name
            make, model = self.parse_make_model(data.get('model', ''))
            
            # Add URL record info to extracted data
            extracted_data = {
                'url_record_id': url_record['id'],
                'vehicle_id': url_record.get('vehicle_id'),  # Include vehicle_id from URL record
                'site_name': site_name,
                'lot_number': lot_number,
                'url': url,
                'make': make,
                'model': model,
                'year': self._parse_numeric(data.get('year', '')),
                'model_name': data.get('model_name', ''),  # Full model name from page
                'type_code': data.get('type_code', ''),
                'scores': data.get('scores', ''),
                'start_price': data.get('start_price', ''),
                'final_price': data.get('final_price', ''),
                'mileage': data.get('mileage', ''),
                'interior_score': data.get('interior_score', ''),
                'exterior_score': data.get('exterior_score', ''),
                'transmission': data.get('transmission', ''),
                'displacement': data.get('displacement', ''),
                'result': data.get('result', ''),
                'color': data.get('color', ''),
                'equipment': data.get('equipment', ''),
                'auction_time': data.get('auction_time', ''),
                'auction_sheet_url': data.get('auction_sheet_url', ''),
                'image_urls': data.get('image_urls', []),
                'total_images': data.get('total_images', 0)
            }
            
            # Log what we extracted for debugging
            logger.debug(f"    📊 URL Record: {url_record}")
            logger.debug(f"    📊 Extracted Data: {data}")
            
            return extracted_data
            
        except Exception as e:
            error_msg = f"Extraction failed for {url}: {e}"
            logger.error(f"❌ {error_msg}")
            await self.mark_processing_failed(url_record['id'], error_msg)
            return None

    def extract_lot_number_from_url(self, url: str) -> str:
        """Extract lot number from URL"""
        try:
            # Common patterns for lot numbers in URLs
            if 'id=' in url:
                return url.split('id=')[1].split('&')[0]
            elif 'lot=' in url:
                return url.split('lot=')[1].split('&')[0]
            elif 'bid=' in url:
                return url.split('bid=')[1].split('&')[0]
            else:
                # Try to extract any number from the URL
                import re
                numbers = re.findall(r'\d+', url)
                return numbers[-1] if numbers else 'unknown'
        except:
            return 'unknown'

    def parse_make_model(self, model_name: str) -> tuple:
        """Parse make and model from full model name"""
        try:
            if not model_name:
                return 'Unknown', 'Unknown'
            
            # Common makes
            makes = ['TOYOTA', 'HONDA', 'NISSAN', 'MAZDA', 'SUBARU', 'MITSUBISHI', 'SUZUKI', 'DAIHATSU', 'LEXUS', 'INFINITI', 'ACURA']
            
            model_name_upper = model_name.upper()
            
            for make in makes:
                if model_name_upper.startswith(make):
                    model = model_name[len(make):].strip()
                    return make, model
            
            # If no known make found, split on first space
            parts = model_name.split(' ', 1)
            if len(parts) >= 2:
                return parts[0], parts[1]
            else:
                return parts[0], ''
                
        except:
            return 'Unknown', 'Unknown'

    async def mark_processing_started(self, url_id: int):
        """Mark URL processing as started"""
        try:
            self.db_handler.supabase_client.table("processed_urls").update({
                "processing_started": datetime.now().isoformat()
            }).eq("id", url_id).execute()
        except Exception as e:
            logger.error(f"❌ Failed to mark processing started for URL {url_id}: {e}")

    async def mark_processing_failed(self, url_id: int, error_message: str):
        """Mark URL processing as failed"""
        try:
            self.db_handler.supabase_client.table("processed_urls").update({
                "processed": False,
                "processing_completed": datetime.now().isoformat(),
                "error_message": error_message,
                "retry_count": 1  # Increment retry count
            }).eq("id", url_id).execute()
        except Exception as e:
            logger.error(f"❌ Failed to mark processing failed for URL {url_id}: {e}")

    async def save_extracted_data(self, data: Dict):
        """Save extracted data to database and mark URL as processed"""
        try:
            # Parse auction time if available
            auction_date = None
            if data.get('auction_time'):
                try:
                    auction_date = data['auction_time'][:10]  # Extract date part (YYYY-MM-DD)
                except:
                    auction_date = None
            
            # Prepare data for vehicle_details table (only detailed/extended info)
            db_record = {
                "vehicle_id": data.get("vehicle_id"),  # Link to vehicles table
                "url": data.get("url"),
                "start_price": self._parse_numeric(data.get("start_price")),
                "final_price": self._parse_numeric(data.get("final_price")),
                "auction_date": auction_date,
                "engine_size": self._parse_numeric(data.get("displacement")),
                "transmission": data.get("transmission"),

                "type_code": data.get("type_code"),
                "chassis_number": data.get("chassis_number"),
                "interior_score": data.get("interior_score"),
                "exterior_score": data.get("exterior_score"),
                "equipment": data.get("equipment"),
                "auction_time": data.get("auction_time"),
                "displacement": data.get("displacement"),
                "additional_info": {
                    "scores": data.get("scores"),
                    "auction_sheet_url": data.get("auction_sheet_url"),
                    "total_images": data.get("total_images", 0)
                },
                "image_urls": data.get("image_urls", []),  # Store all image URLs
                "total_images": data.get("total_images", 0),
                "auction_sheet_url": data.get("auction_sheet_url", "")
            }
            
            # Insert into vehicle_details
            result = self.db_handler.supabase_client.table("vehicle_details").upsert(
                db_record, on_conflict="vehicle_id"
            ).execute()
            
            # Mark URL as processed with completion timestamp
            self.db_handler.supabase_client.table("processed_urls").update({
                "processed": True,
                "processing_completed": datetime.now().isoformat(),
                "error_message": None  # Clear any previous error
            }).eq("id", data["url_record_id"]).execute()
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to save data: {e}"
            logger.error(f"❌ {error_msg}")
            
            # Mark as failed in processed_urls
            try:
                self.db_handler.supabase_client.table("processed_urls").update({
                    "processed": False,
                    "processing_completed": datetime.now().isoformat(),
                    "error_message": error_msg
                }).eq("id", data["url_record_id"]).execute()
            except:
                pass
                
            return False

    def _parse_numeric(self, value) -> int:
        """Parse numeric values safely"""
        if not value:
            return 0
        try:
            cleaned = ''.join(c for c in str(value) if c.isdigit())
            return int(cleaned) if cleaned else 0
        except:
            return 0

    async def process_url_batch(self, urls: List[Dict]):
        """Process a batch of URLs across all sites"""
        logger.info(f"🔄 Processing batch of {len(urls)} URLs...")
        
        # Group URLs by site
        urls_by_site = {}
        for url_record in urls:
            site_name = url_record['site_name']
            if site_name not in urls_by_site:
                urls_by_site[site_name] = []
            urls_by_site[site_name].append(url_record)
        
        # Process each site's URLs
        total_processed = 0
        for site_name, site_urls in urls_by_site.items():
            if site_name not in self.pages:
                logger.warning(f"⚠️ No pages available for {site_name}")
                continue
                
            logger.info(f"  🔗 Processing {len(site_urls)} URLs for {site_name}")
            
            # Use available pages for this site
            available_pages = self.pages[site_name]
            
            # Process URLs concurrently using available pages
            tasks = []
            for i, url_record in enumerate(site_urls):
                page_index = i % len(available_pages)
                page = available_pages[page_index]
                
                task = asyncio.create_task(
                    self.extract_data_from_url(page, site_name, url_record)
                )
                tasks.append((task, url_record))
            
            # Wait for all extractions for this site
            for task, url_record in tasks:
                try:
                    extracted_data = await task
                    if extracted_data:
                        success = await self.save_extracted_data(extracted_data)
                        if success:
                            total_processed += 1
                            # Enhanced logging with extracted details
                            model_display = extracted_data.get('model') or f"{url_record.get('make', 'Unknown')} {url_record.get('model', 'Model')}"
                            price_display = extracted_data.get('start_price', 'N/A')
                            images_count = extracted_data.get('total_images', 0)
                            result_status = extracted_data.get('result', 'unknown')
                            lot_display = url_record.get('lot_number', url_record.get('id', 'Unknown'))
                            
                            logger.info(f"    ✅ {model_display} - Lot {lot_display}")
                            logger.info(f"       Price: {price_display} | Status: {result_status} | Images: {images_count}")
                        else:
                            lot_display = url_record.get('lot_number', url_record.get('id', 'Unknown'))
                            logger.error(f"    ❌ Failed to save Lot {lot_display}")
                    else:
                        lot_display = url_record.get('lot_number', url_record.get('id', 'Unknown'))
                        logger.error(f"    ❌ No data extracted for Lot {lot_display}")
                except Exception as e:
                    logger.error(f"    ❌ Error processing {url_record['lot_number']}: {e}")
        
        logger.info(f"✅ Batch complete: {total_processed}/{len(urls)} URLs processed successfully")
        return total_processed

    async def run_extraction(self):
        """Main extraction loop"""
        logger.info("🚀 Starting auction data extraction...")
        
        # Connect to database
        await self.connect_database()
        
        # Show metrics
        site_counts, total_unprocessed = self.display_metrics()
        
        if total_unprocessed == 0:
            logger.info("✅ No URLs to process!")
            return
        
        # Initialize browsers
        await self.initialize_browsers()
        
        # Login to sites
        successful_logins = await self.login_to_sites()
        
        if successful_logins == 0:
            logger.error("❌ No successful logins - cannot proceed")
            return
        
        # Open tabs
        await self.open_tabs_per_site()
        
        # Main processing loop
        total_processed = 0
        batch_number = 1
        
        while True:
            logger.info(f"\n📦 BATCH {batch_number}")
            
            # Fetch next batch of URLs
            urls = self.get_unprocessed_urls(limit=10)
            
            if not urls:
                logger.info("✅ All URLs processed!")
                break
            
            # Process the batch
            processed_count = await self.process_url_batch(urls)
            total_processed += processed_count
            
            batch_number += 1
            
            # Short delay between batches
            await asyncio.sleep(2)
        
        logger.info(f"\n🎉 EXTRACTION COMPLETE!")
        logger.info(f"📊 Total processed: {total_processed} URLs")

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up...")
        
        # Close all pages
        for site_name, pages in self.pages.items():
            for page in pages:
                try:
                    await page.close()
                except:
                    pass
        
        # Close all contexts
        for context in self.contexts.values():
            try:
                await context.close()
            except:
                pass
        
        # Close all browsers
        for browser in self.browsers.values():
            try:
                await browser.close()
            except:
                pass
        
        # Stop playwright
        if self.playwright:
            await self.playwright.stop()
        
        # Close database
        try:
            self.db_handler.close()
        except:
            pass
        
        logger.info("✅ Cleanup complete")

async def main():
    """Main function"""
    extractor = AuctionDataExtractor()
    
    try:
        await extractor.run_extraction()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        await extractor.cleanup()

if __name__ == "__main__":
    asyncio.run(main())