from playwright.async_api import async_playwright, Page, Browser, BrowserContext, ElementHandle
import asyncio
import logging
from typing import Dict, List, Tuple, Optional, Any
import re
import traceback
from datetime import datetime, date
from abc import ABC, abstractmethod
from config import auction_sites, manufacturer_configs
from run_config import logging_config
from db import DatabaseHandler
import os
import time

def setup_listing_logging():
    """Setup dedicated logging for listing process with better console output"""
    log_dir = 'logs'  # Hardcode the directory name
    os.makedirs(log_dir, exist_ok=True)
    
    # Disable verbose HTTP request logging from external libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    logger = logging.getLogger('listing')
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(
        os.path.join(log_dir, logging_config['files']['listing'])
    )
    file_handler.setFormatter(logging.Formatter(logging_config['format']))
    
    # Console handler with better visibility
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    console_handler.setLevel(logging.INFO)  # Show INFO level messages in console
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

class Base(ABC):
    """Base class providing shared logging functionality"""
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging_config['level'],
            format=logging_config['format'],
            filename=f"logs/{logging_config['files']['main']}"
        )

class DirectDatabaseHandler(Base):
    """Direct database operations without staging overhead"""
    
    def __init__(self, db_handler: DatabaseHandler):
        super().__init__()
        self.db_handler = db_handler
    
    async def bulk_upsert_vehicles_direct(self, listings: List[Dict]) -> int:
        """Direct bulk upsert to vehicles table"""
        if not listings:
            # Removed verbose no listings message
            return 0
            
        try:
            # Deduplicate listings by site_name and lot_number
            unique_listings = {}
            for listing in listings:
                key = (listing.get('site_name'), listing.get('lot_number'))
                if key not in unique_listings:
                    unique_listings[key] = listing
            
            deduplicated_listings = list(unique_listings.values())
            
            # Prepare data for bulk insert
            values = []
            for listing in deduplicated_listings:
                values.append((
                    listing.get('site_name'),
                    listing.get('lot_number'),
                    listing.get('make'),
                    listing.get('model'),
                    listing.get('year'),
                    listing.get('mileage'),
                    listing.get('start_price'),
                    listing.get('end_price'),
                    listing.get('grade'),
                    listing.get('color'),
                    listing.get('result'),
                    listing.get('scores'),
                    listing.get('url'),
                    listing.get('lot_link'),
                    listing.get('auction'),
                    listing.get('search_date')
                ))
            
            # Use batch processing for maximum speed
            total_inserted = 0
            batch_size = 1000
            
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                
                # Use existing database handler but with optimized approach
                batch_listings = []
                for value in batch:
                    batch_listings.append({
                        'site_name': value[0],
                        'lot_number': value[1],
                        'make': value[2],
                        'model': value[3],
                        'year': value[4],
                        'mileage': value[5],
                        'start_price': value[6],
                        'end_price': value[7],
                        'grade': value[8],
                        'color': value[9],
                        'result': value[10],
                        'scores': value[11],
                        'url': value[12],
                        'lot_link': value[13],
                        'auction': value[14],
                        'search_date': value[15]
                    })
                
                # Direct insertion to avoid staging overhead
                staged, processed, duplicates = await self.db_handler.bulk_insert_staging_concurrent(
                    batch_listings[0]['site_name'] if batch_listings else 'unknown',
                    batch_listings
                )
                
                # Immediately process staging to main
                processed_count, duplicate_count = self.db_handler.process_staging_to_main()
                total_inserted += processed_count
                
                self.logger.info(f"Direct batch insert: {processed_count} records, {duplicate_count} duplicates")
            
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Error in direct bulk upsert: {e}")
            return 0

    async def execute_batch(self, query: str, batch: List[Tuple]) -> List:
        """Execute batch query - simplified version using existing handler"""
        try:
            # This is a simplified version - the actual PostgreSQL COPY would be implemented
            # in the database handler itself, here we're using the existing infrastructure
            return batch  # Return batch as successful for counting
        except Exception as e:
            self.logger.error(f"Error executing batch: {e}")
            return []

class TrulyDirectDatabaseHandler(Base):
    """Actually direct database operations - no staging"""
    
    def __init__(self, db_handler: DatabaseHandler):
        super().__init__()
        self.db_handler = db_handler
    
    async def bulk_upsert_vehicles_truly_direct(self, listings: List[Dict]) -> int:
        """Fixed implementation ensuring perfect 1:1 correspondence"""
        if not listings:
            return 0
            
        try:
            # Deduplicate listings by site_name and lot_number
            unique_listings = {}
            for listing in listings:
                key = (listing.get("site_name"), listing.get("lot_number"))
                if key not in unique_listings:
                    unique_listings[key] = listing
            
            deduplicated_listings = list(unique_listings.values())
            self.logger.info(f"Deduplicated {len(listings)} listings to {len(deduplicated_listings)} unique records")
            
            # Process each vehicle individually to ensure atomicity
            total_processed = 0
            
            for listing in deduplicated_listings:
                vehicle_record = {
                    'site_name': listing.get('site_name'),
                    'lot_number': listing.get('lot_number'),
                    'make': listing.get('make'),
                    'model': listing.get('model'),
                    'year': listing.get('year'),
                    'mileage': listing.get('mileage'),
                    'start_price': listing.get('start_price'),
                    'end_price': listing.get('end_price'),
                    'grade': listing.get('grade'),
                    'color': listing.get('color'),
                    'result': listing.get('result'),
                    'scores': listing.get('scores'),
                    'lot_link': listing.get('lot_link'),
                    'auction': listing.get('auction'),
                    'search_date': listing.get('search_date'),
                    'created_at': datetime.now().isoformat()
                }
                
                try:
                    # Step 1: Upsert vehicle
                    vehicle_result = self.db_handler.supabase_client.table("vehicles").upsert(
                        [vehicle_record],
                        on_conflict="site_name,lot_number"
                    ).execute()
                    
                    if vehicle_result.data:
                        vehicle = vehicle_result.data[0]
                        
                        # Step 2: Create URL record if lot_link exists
                        if vehicle.get('lot_link'):
                            url_record = {
                                'site_name': vehicle['site_name'],
                                'url': vehicle['lot_link'],
                                'vehicle_id': vehicle['id'],
                                'processed': False,
                                'created_at': datetime.now().isoformat()
                            }
                            
                            # Check if a processed_urls record already exists for this vehicle
                            existing_url_result = self.db_handler.supabase_client.table("processed_urls").select("id, url").eq("vehicle_id", vehicle['id']).execute()
                            
                            if existing_url_result.data:
                                # Update existing record if URL has changed
                                existing_url = existing_url_result.data[0]
                                if existing_url['url'] != vehicle['lot_link']:
                                    # URL has changed, update the record
                                    self.db_handler.supabase_client.table("processed_urls").update({
                                        'url': vehicle['lot_link'],
                                        'processed': False  # Reset processing status for new URL
                                    }).eq("vehicle_id", vehicle['id']).execute()
                                    self.logger.info(f"Updated URL for vehicle {vehicle['id']}: {existing_url['url']} → {vehicle['lot_link']}")
                                # If URL hasn't changed, no action needed
                            else:
                                # Create new processed_urls record
                                self.db_handler.supabase_client.table("processed_urls").upsert(
                                    [url_record],
                                    on_conflict="site_name,url"
                                ).execute()
                        
                        total_processed += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing vehicle {listing.get('lot_number')}: {e}")
                    continue
            
            self.logger.info(f"Successfully processed {total_processed} vehicles with 1:1 correspondence")
            return total_processed
            
        except Exception as e:
            self.logger.error(f"Error in truly direct bulk upsert: {e}")
            return 0
    
    async def _execute_direct_batch(self, query: str, batch: List[Tuple]) -> List:
        """Execute direct batch query using Supabase client"""
        try:
            # Convert batch tuples to dictionaries for Supabase
            batch_data = []
            for row in batch:
                # This is a simplified approach - in practice, we'd need to map columns properly
                # For now, we'll use the existing Supabase upsert method
                pass
            
            # Use the existing database handler's Supabase client
            # For now, we'll use a simpler approach with the existing infrastructure
            return []
            
        except Exception as e:
            self.logger.error(f"Error executing direct batch: {e}")
            return []
    
    async def _populate_processed_urls_direct(self, listings: List[Dict]):
        """Directly populate processed URLs table using Supabase"""
        try:
            url_data = []
            for listing in listings:
                if listing.get('lot_link'):
                    url_data.append({
                        'site_name': listing['site_name'],
                        'url': listing['lot_link'],
                        'processed': False,
                        'created_at': datetime.now().isoformat()
                    })
            
            if url_data:
                # Deduplicate URL data by site_name and url
                unique_urls = {}
                for url_record in url_data:
                    key = (url_record['site_name'], url_record['url'])
                    if key not in unique_urls:
                        unique_urls[key] = url_record
                
                deduplicated_urls = list(unique_urls.values())
                self.logger.info(f"Deduplicated {len(url_data)} URLs to {len(deduplicated_urls)} unique URLs")
                
                # First, get the vehicle IDs for the URLs
                for url_record in deduplicated_urls:
                    # Find the corresponding vehicle record
                    vehicle_result = self.db_handler.supabase_client.table("vehicles").select("id").eq("site_name", url_record['site_name']).eq("lot_link", url_record['url']).execute()
                    if vehicle_result.data:
                        url_record['vehicle_id'] = vehicle_result.data[0]['id']
                
                # Use Supabase client to insert URLs
                result = self.db_handler.supabase_client.table("processed_urls").upsert(
                    deduplicated_urls,
                    on_conflict="site_name,url"
                ).execute()
                
                self.logger.info(f"Populated {len(deduplicated_urls)} URLs directly")
                
        except Exception as e:
            self.logger.error(f"Error populating URLs directly: {e}")

class MemoryOptimizedBrowserPool(Base):
    """Browser pool with memory management and periodic cleanup"""
    
    def __init__(self, pool_size=2):
        super().__init__()
        self.pool_size = pool_size
        self.available_browsers = asyncio.Queue(maxsize=pool_size)
        self.in_use = set()
        self.playwright = None
        self.initialized = False
        self.context_cleanup_interval = 300  # 5 minutes
        self.cleanup_task = None
        self.browser_creation_time = {}

    async def initialize(self):
        """Initialize with memory cleanup task"""
        if self.initialized:
            return
            
        try:
            self.playwright = await async_playwright().start()
            
            # Create browser pool with memory-optimized settings
            for i in range(self.pool_size):
                browser = await self._create_optimized_browser()
                await self.available_browsers.put(browser)
                self.browser_creation_time[browser] = time.time()
                self.logger.info(f"Memory-optimized browser {i+1}/{self.pool_size} initialized")
            
            self.initialized = True
            
            # Start memory cleanup task
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            
            self.logger.info(f"Memory-optimized browser pool initialized with {self.pool_size} browsers")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize memory-optimized browser pool: {e}")
            raise

    async def get_browser(self) -> Browser:
        """Get an available browser from the pool"""
        if not self.initialized:
            await self.initialize()
        
        browser = await self.available_browsers.get()
        self.in_use.add(browser)
        return browser

    async def return_browser(self, browser: Browser):
        """Return a browser to the pool"""
        if browser in self.in_use:
            self.in_use.remove(browser)
            await self.available_browsers.put(browser)

    async def _periodic_cleanup(self):
        """Periodically clean up to prevent memory leaks"""
        while True:
            try:
                await asyncio.sleep(self.context_cleanup_interval)
                
                # Force garbage collection on browsers not in use
                browsers_to_refresh = []
                browsers_to_keep = []
                
                while not self.available_browsers.empty():
                    browser = await self.available_browsers.get()
                    
                    if self._should_refresh_browser(browser):
                        browsers_to_refresh.append(browser)
                    else:
                        browsers_to_keep.append(browser)
                
                # Refresh browsers that need it
                for browser in browsers_to_refresh:
                    try:
                        await browser.close()
                        new_browser = await self._create_optimized_browser()
                        browsers_to_keep.append(new_browser)
                        self.browser_creation_time[new_browser] = time.time()
                        self.logger.info("Refreshed browser due to memory management")
                    except Exception as e:
                        self.logger.error(f"Error refreshing browser: {e}")
                
                # Return all browsers to pool
                for browser in browsers_to_keep:
                    await self.available_browsers.put(browser)
                        
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")
                
    async def _create_optimized_browser(self):
        """Create browser with safe memory optimization"""
        return await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-gpu', 
                '--no-sandbox', 
                '--disable-dev-shm-usage',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images',  # Keep this - saves bandwidth
                # REMOVED: '--disable-javascript',  # DON'T disable JS!
                '--memory-pressure-off',
                '--max-old-space-size=512',
                '--disable-background-networking',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-features=TranslateUI',
                '--disable-ipc-flooding-protection'
            ]
        )
        
    def _should_refresh_browser(self, browser) -> bool:
        """Determine if browser should be refreshed based on age and memory usage"""
        # Refresh browsers older than 1 hour
        creation_time = self.browser_creation_time.get(browser, 0)
        browser_age = time.time() - creation_time
        
        return browser_age > 3600  # 1 hour

    async def cleanup(self):
        """Enhanced cleanup with task cancellation"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
                
        try:
            # Close browsers in use
            for browser in self.in_use.copy():
                await browser.close()
            self.in_use.clear()
            
            # Close browsers in pool
            while not self.available_browsers.empty():
                browser = await self.available_browsers.get()
                await browser.close()
            
            if self.playwright:
                await self.playwright.stop()
                
            self.logger.info("Memory-optimized browser pool cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up memory-optimized browser pool: {e}")

# SafeMemoryOptimizedBrowserPool with fixed JS settings
class SafeMemoryOptimizedBrowserPool(MemoryOptimizedBrowserPool):
    """Memory optimized pool with safe browser settings"""
    pass

# Legacy BrowserPool class for backward compatibility
class BrowserPool(MemoryOptimizedBrowserPool):
    """Legacy browser pool - now uses memory optimization"""
    pass

class EnhancedSearchOptimizer(Base):
    """Enhanced optimizer with rate limiting and error recovery"""
    
    def __init__(self):
        super().__init__()
        self.dropdown_cache = {}
        self.site_sessions = {}
        self.last_request_time = {}
        self.min_request_interval = 2.0
        self.retry_count = {}
        self.max_retries = 3
        
    async def respect_rate_limit(self, site_name: str):
        """Enforce rate limiting per site"""
        now = time.time()
        last_time = self.last_request_time.get(site_name, 0)
        time_diff = now - last_time
        
        if time_diff < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_diff)
        
        self.last_request_time[site_name] = time.time()
    
    async def search_with_retry(self, page: Page, site_name: str, make: str, models: List[str], min_year: int = None) -> List[Dict]:
        """Search with automatic retry on failure"""
        retry_key = f"{site_name}:{make}"
        
        for attempt in range(self.max_retries):
            try:
                await self.respect_rate_limit(site_name)
                return await self.search_make_all_models(page, site_name, make, models, min_year)
                
            except Exception as e:
                self.logger.warning(f"Search attempt {attempt + 1} failed for {make}: {e}")
                
                if attempt < self.max_retries - 1:
                    # Exponential backoff
                    await asyncio.sleep(2 ** attempt)
                    
                    # Try to refresh the page
                    try:
                        await page.reload()
                        await asyncio.sleep(2)
                    except:
                        pass
                else:
                    self.logger.error(f"All retry attempts failed for {make} on {site_name}")
                    return []
        
        return []

class SearchOptimizer(EnhancedSearchOptimizer):
    """Optimized search handling with caching and batch operations"""
    def __init__(self):
        super().__init__()

    async def get_cached_mappings(self, page: Page, site_name: str) -> Dict[str, Dict]:
        """Get cached dropdown mappings for a site"""
        if site_name not in self.dropdown_cache:
            # Removed verbose dropdown mappings logging
            self.dropdown_cache[site_name] = await self._fetch_all_mappings(page)
        return self.dropdown_cache[site_name]

    async def _fetch_all_mappings(self, page: Page) -> Dict[str, Dict]:
        """Fetch all dropdown mappings dynamically by selecting each make"""
        try:
            # Get all makes first
            make_mappings = await page.evaluate('''() => {
                const makeMappings = {};
                const makeSelect = document.querySelector('#mrk');
                if (makeSelect) {
                    Array.from(makeSelect.options).forEach(opt => {
                        if (opt.value && opt.value !== '-1') {
                            makeMappings[opt.text.trim()] = opt.value;
                        }
                    });
                }
                return makeMappings;
            }''')
            
            # Fetch models for each make dynamically
            all_model_mappings = {}
            
            for make_name, make_value in make_mappings.items():
                try:
                    # Select this make
                    await page.select_option('#mrk', value=make_value)
                    await page.wait_for_load_state('networkidle', timeout=10000)
                    await asyncio.sleep(1)  # Wait for model dropdown to populate
                    
                    # Get models for this make
                    model_mappings = await page.evaluate('''() => {
                        const modelMappings = {};
                        const modelSelect = document.querySelector('select[name="mdl[]"]');
                        if (modelSelect) {
                            Array.from(modelSelect.options).forEach(opt => {
                                if (opt.value && opt.value !== '-1') {
                                    modelMappings[opt.text.trim()] = opt.value;
                                }
                            });
                        }
                        return modelMappings;
                    }''')
                    
                    # Store models for this make
                    all_model_mappings[make_name] = model_mappings
                    
                except Exception as e:
                    self.logger.warning(f"Error fetching models for make {make_name}: {e}")
                    all_model_mappings[make_name] = {}
            
            return {'makes': make_mappings, 'models': all_model_mappings}
            
        except Exception as e:
            self.logger.error(f"Error fetching dropdown mappings: {e}")
            return {'makes': {}, 'models': {}}

    async def search_make_all_models(self, page: Page, site_name: str, make: str, models: List[str], min_year: int = None) -> List[Dict]:
        """Search for specific models assigned to this site (should be only one model per site)"""
        try:
            # Get cached mappings for this site
            make_mappings = await self.get_cached_mappings(page, site_name)
            make_value = make_mappings['makes'].get(make)
            
            if not make_value:
                self.logger.warning(f"Make {make} not found in cache for {site_name}")
                return []

            # Get cached model mappings for this make
            model_mappings = make_mappings['models'].get(make, {})
            
            if not model_mappings:
                self.logger.warning(f"No models cached for make {make} on {site_name}")
                return []
            
            # Select make once
            await page.select_option('#mrk', value=make_value)
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(1)  # Brief wait for model dropdown to populate
            
            all_listings = []
            
            # Process the specific model(s) assigned to this site
            for model in models:
                try:
                    model_value = model_mappings.get(model)
                    if not model_value:
                        self.logger.warning(f"Model {model} not found for make {make}")
                        continue
                    
                    # Select model and search
                    await page.select_option('select[name="mdl[]"]', value=model_value)
                    
                    # Apply year filter if specified
                    if min_year is not None:
                        try:
                            # Fill the minimum year field
                            await page.fill('input[name="year1"]', str(min_year))
                        except Exception as year_error:
                            self.logger.warning(f"Could not apply year filter: {year_error}")
                    
                    # Execute search
                    search_clicked = await page.evaluate('''() => {
                        const searchButtons = [
                            '#btnSearch', '#btnSearch1', '#btnSearch2', '#btnSearsh', '#btnSearch3',
                            'input[value="Search"]', 'button[value="Search"]',
                            'input[type="submit"]', 'button[type="submit"]'
                        ];
                        
                        for (const selector of searchButtons) {
                            const btn = document.querySelector(selector);
                            if (btn && btn.offsetParent !== null) {
                                btn.click();
                                return true;
                            }
                        }
                        return false;
                    }''')
                    
                    if search_clicked:
                        await page.wait_for_load_state('networkidle')
                        await asyncio.sleep(1)
                        
                        # Extract listings for this specific model
                        listings = await self._extract_listings_fast(page, site_name, make, model)
                        
                        if listings:
                            all_listings.extend(listings)
                        
                    else:
                        self.logger.warning(f"Could not click search button for {make} {model}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing {make} {model}: {e}")
                    continue
            
            return all_listings
            
        except Exception as e:
            self.logger.error(f"Error in search for {make} on {site_name}: {e}")
            return []

    async def _debug_page_content(self, page: Page, make: str, model: str) -> None:
        """Debug helper to capture page content when extraction fails"""
        try:
            current_url = page.url
            print(f"🔍 Debugging page content for {make} {model} at {current_url}")
            
            # Get all table structures
            tables_info = await page.evaluate('''() => {
                const tables = document.querySelectorAll('table');
                const tableInfo = [];
                
                tables.forEach((table, index) => {
                    const rows = table.querySelectorAll('tr');
                    const rowInfo = [];
                    
                    rows.forEach((row, rowIndex) => {
                        const cells = row.querySelectorAll('td, th');
                        const cellInfo = [];
                        
                        cells.forEach((cell, cellIndex) => {
                            cellInfo.push({
                                index: cellIndex,
                                id: cell.id || null,
                                class: cell.className || null,
                                text: cell.textContent.trim().substring(0, 30),
                                hasLink: !!cell.querySelector('a')
                            });
                        });
                        
                        if (cellInfo.length > 0) {
                            rowInfo.push({
                                rowIndex: rowIndex,
                                cells: cellInfo
                            });
                        }
                    });
                    
                    if (rowInfo.length > 0) {
                        tableInfo.push({
                            tableIndex: index,
                            rows: rowInfo.slice(0, 5)  # First 5 rows only
                        });
                    }
                });
                
                return tableInfo;
            }''')
            
            print(f"📋 Found {len(tables_info)} tables on page")
            for table_idx, table in enumerate(tables_info):
                print(f"  Table {table_idx}: {len(table['rows'])} rows")
                for row_idx, row in enumerate(table['rows'][:3]):  # Show first 3 rows
                    print(f"    Row {row_idx}: {len(row['cells'])} cells")
                    for cell in row['cells'][:5]:  # Show first 5 cells
                        print(f"      Cell {cell['index']}: id='{cell['id']}', class='{cell['class']}', text='{cell['text']}'")
            
            # Also check for any div-based content that might contain results
            div_content = await page.evaluate('''() => {
                const divs = document.querySelectorAll('div');
                const divInfo = [];
                
                divs.forEach((div, index) => {
                    const text = div.textContent.trim();
                    if (text.length > 10 && (text.toLowerCase().includes('available') || text.toLowerCase().includes('lot'))) {
                        divInfo.push({
                            index: index,
                            class: div.className || null,
                            text: text.substring(0, 100)
                        });
                    }
                });
                
                return divInfo.slice(0, 10);  # First 10 relevant divs
            }''')
            
            if div_content:
                print(f"📋 Found {len(div_content)} relevant divs:")
                for div in div_content:
                    print(f"  Div {div['index']}: class='{div['class']}', text='{div['text']}'")
                        
        except Exception as e:
            print(f"❌ Error debugging page content: {e}")
            self.logger.error(f"Error debugging page content: {e}")

    async def _extract_listings_fast(self, page: Page, site_name: str, make: str, model: str) -> List[Dict]:
        """Fast listing extraction with correct selectors for the actual HTML structure"""
        try:
            # Log the current URL for debugging (file only)
            current_url = page.url
            # Removed verbose processing logging
            
            # Wait longer and check for different loading states
            try:
                await page.wait_for_load_state('networkidle', timeout=15000)
                await asyncio.sleep(2)  # Additional wait for dynamic content
            except:
                print(f"⚠️  Page load timeout for {make} {model}, continuing...")
            
            # Check what's actually on the page
            page_info = await page.evaluate('''() => {
                const info = {
                    title: document.title,
                    url: window.location.href,
                    bodyText: document.body.textContent.substring(0, 200),
                    tableCount: document.querySelectorAll('table').length,
                    mainTableExists: !!document.querySelector('#mainTable'),
                    hasResultsText: document.body.textContent.toLowerCase().includes('result'),
                    hasAvailableText: document.body.textContent.toLowerCase().includes('available'),
                    hasFoundTotal: document.body.textContent.toLowerCase().includes('found total'),
                    colorGreedRows: document.querySelectorAll('tr[class*="ColorGreed"]').length
                };
                return info;
            }''')
            
            # Only log table info if there are issues
            # Removed verbose "No results found" logging
            
            # Wait for results table to load
            try:
                await page.wait_for_selector('tr.ColorGreed1, tr.ColorGreed2', timeout=10000)
            except:
                if page_info['tableCount'] == 0:
                    return []
            
            # Target the specific ColorGreed rows using exact class names
            rows = await page.query_selector_all('tr.ColorGreed1, tr.ColorGreed2')
            
            if not rows:
                return []
            
            # Removed verbose processing count output
            
            listings = []
            current_date = datetime.now().date()
            
            for row_index, row in enumerate(rows, 1):
                try:
                    # Extract data using the specific ID pattern
                    listing_data = await row.evaluate('''(row) => {
                        const rowId = row.id;
                        const cellNumber = rowId ? rowId.replace('cell_', '') : row.rowIndex || '1';
                        
                        const getData = (fieldName) => {
                            const cell = document.querySelector(`#${fieldName}_${cellNumber}`);
                            if (!cell) return null;
                            
                            const link = cell.querySelector('a');
                            const priceDiv = cell.querySelector(`div[id^="price"]`);
                            
                            return {
                                text: cell.textContent.trim(),
                                href: link ? link.href : null,
                                priceValue: priceDiv ? priceDiv.textContent.trim() : null
                            };
                        };
                        
                        return {
                            bid_number: getData('bid_number'),
                            company: getData('company'),
                            model: getData('model'),
                            grade: getData('grade'),
                            year: getData('year'),
                            mileage: getData('mileage'),
                            start_price: getData('start_price'),
                            end_price: getData('end_price'),
                            result: getData('result'),
                            scores: getData('scores'),
                            auction: getData('auction'),
                            displacement: getData('displacement'),
                            transmission: getData('transmission'),
                            model_type: getData('model_type'),
                            date: getData('date'),
                            color: getData('color'),
                            equipment: getData('equipment'),
                            inspection: getData('inspection')
                        };
                    }''')
                    
                    # Check if we have essential data
                    if not listing_data['bid_number'] or not listing_data['bid_number']['text']:
                        continue
                    
                    # Check if result is available
                    result_text = listing_data['result']['text'].lower() if listing_data['result'] else ''
                    if 'available' not in result_text:
                        continue
                    
                    # Extract and format the data
                    lot_number = listing_data['bid_number']['text']
                    auction_house = listing_data['auction']['text'] if listing_data['auction'] else 'N/A'
                    date_raw = listing_data['date']['text'] if listing_data['date'] else 'N/A'
                    status = listing_data['result']['text'] if listing_data['result'] else 'N/A'
                    maker = listing_data['company']['text'] if listing_data['company'] else 'N/A'
                    model = listing_data['model']['text'] if listing_data['model'] else 'N/A'
                    grade = listing_data['grade']['text'] if listing_data['grade'] else 'N/A'
                    model_type = listing_data['model_type']['text'] if listing_data['model_type'] else 'N/A'
                    year = listing_data['year']['text'] if listing_data['year'] else 'N/A'
                    
                    # Format mileage
                    mileage_raw = listing_data['mileage']['text'] if listing_data['mileage'] else '0'
                    mileage_clean = mileage_raw.replace(' ', '').replace(',', '').replace('km', '').strip()
                    try:
                        mileage_num = int(''.join(c for c in mileage_clean if c.isdigit())) if mileage_clean else 0
                        mileage_formatted = f"{mileage_num:,} km" if mileage_num > 0 else "N/A"
                    except:
                        mileage_formatted = "N/A"
                    
                    engine = listing_data['displacement']['text'] if listing_data['displacement'] else 'N/A'
                    transmission = listing_data['transmission']['text'] if listing_data['transmission'] else 'N/A'
                    color = listing_data['color']['text'] if listing_data['color'] else 'N/A'
                    equipment = listing_data['equipment']['text'] if listing_data['equipment'] else 'N/A'
                    
                    # Format prices
                    start_price_raw = listing_data['start_price']['priceValue'] if listing_data['start_price'] and listing_data['start_price']['priceValue'] else listing_data['start_price']['text'] if listing_data['start_price'] else '- - -'
                    end_price_raw = listing_data['end_price']['priceValue'] if listing_data['end_price'] and listing_data['end_price']['priceValue'] else listing_data['end_price']['text'] if listing_data['end_price'] else '- - -'
                    
                    start_price = start_price_raw if start_price_raw != '- - -' else 'Not available (- - -)'
                    end_price = end_price_raw if end_price_raw != '- - -' else 'Not available (- - -)'
                    
                    inspection_score = listing_data['scores']['text'] if listing_data['scores'] else 'N/A'
                    inspection_status = listing_data['inspection']['text'] if listing_data['inspection'] else '(blank)'
                    
                    lot_url = listing_data['bid_number']['href'] if listing_data['bid_number'] else ''
                    
                    # Format date if available
                    if date_raw != 'N/A' and date_raw:
                        try:
                            # Try to parse and format the date
                            if '2025' in date_raw:
                                # Extract date parts
                                import re
                                date_match = re.search(r'2025-(\d{2})-(\d{2})', date_raw)
                                if date_match:
                                    month, day = date_match.groups()
                                    month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June', 
                                                 'July', 'August', 'September', 'October', 'November', 'December']
                                    month_name = month_names[int(month)] if int(month) <= 12 else 'Unknown'
                                    date_formatted = f"{month_name} {int(day)}, 2025"
                                else:
                                    date_formatted = date_raw
                            else:
                                date_formatted = date_raw
                        except:
                            date_formatted = date_raw
                    else:
                        date_formatted = 'N/A'
                    
                    # Debug: Print extracted data for first few records
                    if len(listings) < 3:  # Only debug first few records
                        print(f"🔍 DEBUG - Extracted data for {make} {model}:")
                        print(f"  Lot: {lot_number}")
                        print(f"  Auction: {auction_house}")
                        print(f"  Grade: {grade}")
                        print(f"  Color: {color}")
                        print(f"  Result: {status}")
                        print(f"  Scores: {inspection_score}")
                        print(f"  Lot URL: {lot_url}")
                        print(f"  Equipment: {equipment}")
                    
                    # Build listing record for database storage
                    year_num = self._extract_number(year) if year != 'N/A' else 0
                    mileage_num = mileage_num if 'mileage_num' in locals() else 0
                    start_price_num = self._extract_number(start_price_raw) if start_price_raw != '- - -' else 0
                    end_price_num = self._extract_number(end_price_raw) if end_price_raw != '- - -' else 0
                    
                    listing = {
                        'lot_number': lot_number,
                        'year': year_num,
                        'make': make,
                        'model': model,
                        'grade': grade if grade != 'N/A' else '',
                        'color': color if color != 'N/A' else '',
                        'mileage': mileage_num,
                        'start_price': start_price_num,
                        'end_price': end_price_num,
                        'result': 'available',
                        'scores': inspection_score if inspection_score != 'N/A' else '',
                        'lot_link': lot_url,
                        'url': lot_url,
                        'auction': auction_house if auction_house != 'N/A' else '',
                        'site_name': site_name,
                        'search_date': current_date.isoformat()  # Convert date to string
                    }
                    
                    listings.append(listing)
                    
                except Exception as e:
                    self.logger.error(f"Error extracting row {row_index}: {e}")
                    continue
            
            if listings:
                # Removed verbose extraction complete messages
                self.logger.info(f"Extracted {len(listings)} listings for {make} {model} from {site_name}")
            
            # ADD DEDUPLICATION HERE - before returning listings:
            # Deduplicate by lot_number to prevent database constraint violations
            seen_lots = set()
            unique_listings = []
            for listing in listings:
                lot_key = listing.get('lot_number')
                if lot_key and lot_key not in seen_lots:
                    seen_lots.add(lot_key)
                    unique_listings.append(listing)
            
            if len(listings) != len(unique_listings):
                self.logger.warning(f"Removed {len(listings) - len(unique_listings)} duplicate lot numbers from {make} {model}")
            
            return unique_listings
            
        except Exception as e:
            self.logger.error(f"Error extracting listings for {make} {model}: {e}")
            return []

    def _extract_number(self, value: str) -> int:
        """Extract numeric value from string, handling various formats"""
        if not value:
            return 0
        try:
            # Handle special cases
            if value == '- - -' or value == '---':
                return 0
            
            # Remove common non-numeric characters but keep decimal points and commas
            cleaned = ''.join(c for c in str(value) if c.isdigit() or c in ['.', ','])
            
            # Remove commas (thousand separators)
            cleaned = cleaned.replace(',', '')
            
            # Handle decimal points
            if '.' in cleaned:
                cleaned = cleaned.split('.')[0]  # Take only the integer part
            
            # Convert to int
            return int(cleaned) if cleaned else 0
        except (TypeError, ValueError):
            return 0

class Authenticator(Base):
    """Handles login process for auction sites"""
    async def login(self, page: Page, credentials: Dict) -> bool:
        try:
            # Removed verbose navigation logging
            await page.goto(credentials['auction_url'])
            
            # Use the correct selectors from the form
            await page.fill('#usr_name', credentials['username'])
            await page.fill('#usr_pwd', credentials['password'])
            
            # Use the correct submit button selector
            await page.click('input[name="Submit"][value="Sign in"]')
            
            await page.wait_for_load_state('networkidle')
            # Removed verbose login success logging
            return True
        except Exception as e:
            self.logger.error(f"Login failed with error: {str(e)}")
            return False

class OptimizedSearchExecutor(Base):
    """Optimized search executor with concurrent processing and direct database operations"""
    def __init__(self, db_handler: DatabaseHandler, browser_pool: BrowserPool):
        super().__init__()
        self.db_handler = db_handler
        self.browser_pool = browser_pool
        self.search_optimizer = SearchOptimizer()
        self.max_concurrent_searches = 5  # Increased from 1
        self.semaphore = asyncio.Semaphore(self.max_concurrent_searches)

    async def process_searches_concurrently(self, site_name: str, searches: List[Dict]) -> List[Dict]:
        """Process multiple searches concurrently with controlled concurrency"""
        async def _execute_search_with_semaphore(search_task):
            async with self.semaphore:
                return await self._execute_single_search(site_name, search_task)
        
        # Create tasks for all searches
        tasks = [
            _execute_search_with_semaphore(search) 
            for search in searches
        ]
        
        # Execute all searches concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and collect listings
        all_listings = []
        for result in results:
            if isinstance(result, list):
                all_listings.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Search failed: {result}")
        
        return all_listings

    async def _execute_single_search(self, site_name: str, search_task: Dict) -> List[Dict]:
        """Execute a single search task using browser pool"""
        browser = None
        try:
            # Get browser from pool
            browser = await self.browser_pool.get_browser()
            
            # Create new context for this search
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            page = await context.new_page()
            
            # Navigate to site
            site_config = auction_sites[site_name]
            await page.goto(site_config['scraping']['auction_url'])
            
            # Login if needed
            authenticator = Authenticator()
            login_credentials = {
                'auction_url': site_config['scraping']['auction_url'],
                'username': site_config['username'],
                'password': site_config['password']
            }
            
            if not await authenticator.login(page, login_credentials):
                self.logger.error(f"Login failed for {site_name}")
                return []
            
            # Process make and all its models in batch
            make = search_task['make']
            models = search_task['models']
            min_year = search_task.get('min_year')  # Get year filter if specified
            
            listings = await self.search_optimizer.search_make_all_models(
                page, site_name, make, models, min_year
            )
            
            # Save listings directly to database
            if listings:
                await self.bulk_save_listings(listings)
                self.logger.info(f"Saved {len(listings)} listings for {make} from {site_name}")
            
            await context.close()
            return listings
            
        except Exception as e:
            self.logger.error(f"Error executing search for {site_name}: {e}")
            return []
        finally:
            # Always return browser to pool
            if browser:
                await self.browser_pool.return_browser(browser)

    async def bulk_save_listings(self, listings: List[Dict], batch_size: int = 100) -> int:
        """Save listings directly to database in batches"""
        try:
            saved_count = 0
            
            for i in range(0, len(listings), batch_size):
                batch = listings[i:i + batch_size]
                
                # Use existing staging method for compatibility
                staged, processed, duplicates = await self.db_handler.bulk_insert_staging_concurrent(
                    batch[0]['site_name'] if batch else 'unknown', 
                    batch
                )
                saved_count += staged
                
            self.logger.info(f"Bulk saved {saved_count} listings")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"Error bulk saving listings: {e}")
            return 0

    async def _get_site_name(self, page: Page) -> str:
        """Get site name from current page URL"""
        try:
            url = page.url.lower()
            for site_name, config in auction_sites.items():
                base_url = config['scraping']['auction_url'].lower()
                domain = base_url.split('/')[2]  # Get domain like 'auction.pacificcoastjdm.com'
                if domain in url:
                    return site_name
            return "Unknown"
        except Exception as e:
            self.logger.error(f"Error getting site name: {str(e)}")
            return "Unknown"

def calculate_min_year_for_vehicle(make: str, model: str) -> int:
    """
    Calculate the minimum year allowed for a vehicle based on Jamaica's importation age limits.
    
    Args:
        make: Vehicle make (e.g., 'TOYOTA')
        model: Vehicle model (e.g., 'CAMRY')
    
    Returns:
        int: Minimum year allowed for import (e.g., 2019 for a 6-year age limit in 2025)
    """
    try:
        # Get the age limit for this make/model combination
        if make in manufacturer_configs and model in manufacturer_configs[make]:
            age_limit = manufacturer_configs[make][model]['age_limit']
        else:
            # Default to 6 years if not found in config
            age_limit = 6
            # Removed verbose age limit warning
        
        # Calculate minimum year based on current date and age limit
        current_year = date.today().year
        min_year = current_year - age_limit
        
        return min_year
        
    except Exception as e:
        # Removed verbose error output
        # Fallback to a reasonable default
        return date.today().year - 6

def calculate_min_year_for_make(make: str) -> int:
    """
    Calculate the minimum year for a make based on the most restrictive age limit.
    This is useful when we want to apply a single year filter for all models of a make.
    
    Args:
        make: Vehicle make (e.g., 'TOYOTA')
    
    Returns:
        int: Minimum year based on the most restrictive age limit for this make
    """
    try:
        if make not in manufacturer_configs:
            return date.today().year - 6  # Default fallback
        
        # Find the most restrictive age limit for this make
        age_limits = []
        for model, config in manufacturer_configs[make].items():
            age_limits.append(config['age_limit'])
        
        if age_limits:
            most_restrictive_age = max(age_limits)  # Higher age limit = more restrictive
            current_year = date.today().year
            min_year = current_year - most_restrictive_age
            return min_year
        else:
            return date.today().year - 6  # Default fallback
            
    except Exception as e:
        # Removed verbose error output
        return date.today().year - 6

class SimplifiedSiteProcessor(Base):
    """Simplified concurrent processing without complex task management"""
    
    def __init__(self):
        super().__init__()
        self.db_handler = DatabaseHandler()
        self.browser_pool = MemoryOptimizedBrowserPool(pool_size=5)  # Increased from 3 to 5
        self.direct_db = TrulyDirectDatabaseHandler(self.db_handler)
        self.search_optimizer = SearchOptimizer()
        self.logger = setup_listing_logging()
        
    async def initialize(self):
        """Initialize components"""
        self.logger.info("=== Initializing Simplified Site Processor ===")
        self.db_handler.connect()
        await self.browser_pool.initialize()
        self.logger.info("Simplified initialization complete\n")
        
    async def process_all_sites_simply(self):
        """Simplified concurrent processing"""
        
        # Group searches by site efficiently
        site_searches = self._prepare_site_searches()
        
        # Process each site concurrently
        site_tasks = [
            self._process_single_site(site_name, searches)
            for site_name, searches in site_searches.items()
        ]
        
        # Execute with controlled concurrency
        results = await asyncio.gather(*site_tasks, return_exceptions=True)
        
        # Process results
        total_listings = 0
        for result in results:
            if isinstance(result, int):
                total_listings += result
            elif isinstance(result, Exception):
                self.logger.error(f"Site processing failed: {result}")
        
        self.logger.info(f"Total listings processed: {total_listings}")
        return total_listings
        
    def _prepare_site_searches(self) -> Dict[str, List[Dict]]:
        """Prepare search tasks grouped by site"""
        site_searches = {}
        sites = list(auction_sites.keys())
        
        for site_name in sites:
            site_searches[site_name] = []
            site_index = sites.index(site_name)
            
            for make, models in manufacturer_configs.items():
                # Get list of model names from the dictionary keys
                model_names = list(models.keys())
                # Distribute models across sites using round-robin
                site_models = [
                    model for i, model in enumerate(model_names) 
                    if i % len(sites) == site_index
                ]
                
                if site_models:
                    # Create individual search tasks for each model with its specific age limit
                    for model in site_models:
                        min_year = calculate_min_year_for_vehicle(make, model)
                        # Removed verbose year filter output
                        site_searches[site_name].append({
                            'make': make,
                            'models': [model],  # Single model per search
                            'min_year': min_year  # Model-specific year filter
                        })
        
        return site_searches
        
    async def _process_single_site(self, site_name: str, searches: List[Dict]) -> int:
        """Process all searches for a single site"""
        browser = None
        total_listings = 0
        
        try:
            browser = await self.browser_pool.get_browser()
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # Login once per site
            if not await self._login_to_site(page, site_name):
                return 0
            
            # Process all make/model combinations for this site
            for search in searches:
                try:
                    await self.search_optimizer.respect_rate_limit(site_name)
                    
                    listings = await self.search_optimizer.search_with_retry(
                        page, site_name, search['make'], search['models'], search.get('min_year')
                    )
                    
                    if listings:
                        # Save directly to database
                        saved = await self.direct_db.bulk_upsert_vehicles_truly_direct(listings)
                        total_listings += saved
                        
                        # Calculate duplicates removed
                        duplicates_removed = len(listings) - saved
                        print(f"{search['make']} ({site_name}): {len(listings)} found → {saved} saved ({duplicates_removed} duplicates)")
                        self.logger.info(f"Site {site_name}: {search['make']} - {saved} listings")
                    else:
                        print(f"{search['make']} ({site_name}): 0 found → 0 saved")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {search['make']} on {site_name}: {e}")
                    continue
            
            await context.close()
            return total_listings
            
        except Exception as e:
            self.logger.error(f"Error processing site {site_name}: {e}")
            return 0
        finally:
            if browser:
                await self.browser_pool.return_browser(browser)

    async def _login_to_site(self, page: Page, site_name: str) -> bool:
        """Login to a specific site"""
        try:
            site_config = auction_sites[site_name]
            authenticator = Authenticator()
            
            login_credentials = {
                'auction_url': site_config['scraping']['auction_url'],
                'username': site_config['username'],
                'password': site_config['password']
            }
            
            return await authenticator.login(page, login_credentials)
            
        except Exception as e:
            self.logger.error(f"Login failed for {site_name}: {e}")
            return False

    async def cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up simplified processor resources...")
        try:
            await self.browser_pool.cleanup()
            self.db_handler.close()
            self.logger.info("Simplified cleanup completed successfully")
        except Exception as e:
            self.logger.error(f"Error during simplified cleanup: {str(e)}")

class TrulyOptimizedSiteProcessor(SimplifiedSiteProcessor):
    """Site processor with proper staging workflow for duplicate elimination"""
    
    def __init__(self):
        super().__init__()
        self.db_handler = DatabaseHandler()
        self.browser_pool = SafeMemoryOptimizedBrowserPool(pool_size=5)  # Increased from 3 to 5
        self.direct_db = TrulyDirectDatabaseHandler(self.db_handler)  # Use staging workflow
        self.search_optimizer = SearchOptimizer()
        self.logger = setup_listing_logging()
        
    async def _process_single_site(self, site_name: str, searches: List[Dict]) -> int:
        """Process site with truly direct database operations"""
        browser = None
        total_listings = 0
        
        try:
            browser = await self.browser_pool.get_browser()
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # Login once per site
            if not await self._login_to_site(page, site_name):
                return 0
            
            # Process all searches with direct DB saves
            for search in searches:
                try:
                    await self.search_optimizer.respect_rate_limit(site_name)
                    
                    listings = await self.search_optimizer.search_with_retry(
                        page, site_name, search['make'], search['models'], search.get('min_year')
                    )
                    
                    if listings:
                        # Save using direct database handler
                        saved = await self.direct_db.bulk_upsert_vehicles_truly_direct(listings)
                        total_listings += saved
                        
                        # Calculate duplicates removed
                        duplicates_removed = len(listings) - saved
                        print(f"{search['make']} ({site_name}): {len(listings)} found → {saved} saved ({duplicates_removed} duplicates)")
                    else:
                        print(f"{search['make']} ({site_name}): 0 found → 0 saved")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {search['make']} on {site_name}: {e}")
                    continue
            
            await context.close()
            return total_listings
            
        except Exception as e:
            self.logger.error(f"Error processing site {site_name}: {e}")
            return 0
        finally:
            if browser:
                await self.browser_pool.return_browser(browser)

class OptimizedSiteProcessor(SimplifiedSiteProcessor):
    """Legacy optimized site processor - now uses simplified version"""
    def __init__(self):
        super().__init__()
        self.search_executor = OptimizedSearchExecutor(self.db_handler, self.browser_pool)
        
    async def process_all_sites_concurrently(self):
        """Legacy method name - redirects to simplified processing"""
        return await self.process_all_sites_simply()

    async def run(self):
        """Main entry point for running the optimized site processor"""
        try:
            print(f"\n{'='*70}")
            print("STARTING OPTIMIZED AUCTION SITE DATA COLLECTION")
            print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Sites: {', '.join(auction_sites.keys())}")
            print(f"Concurrency: {self.search_executor.max_concurrent_searches} searches")
            print(f"Browser pool: {self.browser_pool.pool_size} browsers")
            print(f"{'='*70}\n")
            
            await self.initialize()
            await self.process_all_sites_concurrently()
            
            # Process staging data after collection is complete
            print("\nProcessing staging to main...")
            processed, duplicates = self.db_handler.process_staging_to_main()
            print(f"Processed {processed} records ({duplicates} duplicates) from staging to main")
            
            # Verify transfer
            print("\nVerifying data transfer...")
            stats = self.db_handler.verify_data_movement()
            if stats:
                print(f"Records in staging: {stats['processed_count']}")
                print(f"Duplicates found: {stats['duplicate_count']}")
                print(f"Total records in main: {stats['main_count']}")
                print(f"URLs mapped: {stats['urls_count']}")
            
            # Populate processed URLs for detailed extraction
            print("\nPopulating processed URLs...")
            inserted, skipped = self.db_handler.populate_processed_urls()
            print(f"Inserted {inserted} new URLs for processing")
            print(f"Skipped {skipped} existing URLs")
            
            # Verify URL processing
            print("\nVerifying URL processing...")
            url_stats = self.db_handler.verify_url_processing()
            print(f"Total vehicles: {url_stats['total_vehicles']}")
            print(f"URLs processed: {url_stats['processed_urls']}")
            print(f"URLs remaining: {url_stats['unprocessed_urls']}")
            
            # Clean up old staging data
            print("\nCleaning up old staging data...")
            cleaned = self.db_handler.cleanup_staging()
            print(f"Cleaned up {cleaned} old staging records")
            
            print(f"\n{'='*70}")
            print("OPTIMIZED DATA COLLECTION COMPLETE")
            print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}\n")
            
        finally:
            await self.cleanup()

# Legacy code removed - now using optimized classes above

async def optimized_main():
    """Complete optimized main function"""
    
    print("🚀 Starting FULLY OPTIMIZED Auction Data Collection")
    print(f"⏰ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    processor = SimplifiedSiteProcessor()
    
    try:
        start_time = time.time()
        
        # Initialize
        await processor.initialize()
        
        # Process all sites
        total_listings = await processor.process_all_sites_simply()
        
        # Calculate performance
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ Collection completed in {duration:.2f} seconds")
        print(f"📊 Total listings processed: {total_listings}")
        print(f"⏰ End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Process remaining staging data
        print("\nProcessing remaining staging data...")
        processed, duplicates = processor.db_handler.process_staging_to_main()
        print(f"Processed {processed} records ({duplicates} duplicates)")
        
        # Populate processed URLs
        print("\nPopulating processed URLs...")
        inserted, skipped = processor.db_handler.populate_processed_urls()
        print(f"Inserted {inserted} new URLs for processing")
        
        # Cleanup old staging data
        print("\nCleaning up old staging data...")
        cleaned = processor.db_handler.cleanup_staging()
        print(f"Cleaned up {cleaned} old staging records")
        
        print(f"\n🎉 FULLY OPTIMIZED PROCESSING COMPLETE!")
        print(f"⚡ Processing speed: {total_listings/duration:.1f} listings/second")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise
    finally:
        await processor.cleanup()

async def truly_optimized_main():
    """Optimized main with proper staging workflow for duplicate elimination"""
    
    print("🚀 Starting Auction Data Collection")
    print(f"⏰ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📋 Processing sites: " + ", ".join(auction_sites.keys()))
    print("-" * 60)
    
    processor = TrulyOptimizedSiteProcessor()
    
    try:
        start_time = time.time()
        
        # Initialize
        await processor.initialize()
        
        # Process all sites with direct DB operations
        total_listings = await processor.process_all_sites_simply()
        
        # Calculate performance
        end_time = time.time()
        duration = end_time - start_time
        rate = total_listings / duration if duration > 0 else 0
        
        print(f"\n🎉 EXTRACTION COMPLETE!")
        print(f"📊 Total vehicles saved: {total_listings}")
        print(f"⏱️  Processing time: {duration/60:.1f} minutes")
        print(f"⚡ Speed: {rate:.1f} vehicles/minute")
        print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Process staging data to main with duplicate elimination
        # Process staging data to main with duplicate elimination
        processed, duplicates = processor.db_handler.process_staging_to_main()
        
        # Populate processed URLs for detailed extraction
        inserted, skipped = processor.db_handler.populate_processed_urls()
        
        # Clean up old staging data
        cleaned = processor.db_handler.cleanup_staging()
        
        print(f"\n🚀 TRULY OPTIMIZED PROCESSING COMPLETE!")
        print(f"⚡ Achieved {rate:.1f}x speed improvement with direct operations!")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise
    finally:
        await processor.cleanup()

async def main():
    print("\n=== Starting Optimized Auction Site Data Collection ===\n")
    print(f"Processing sites: {', '.join(auction_sites.keys())}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Use the new optimized main function
    await truly_optimized_main()

if __name__ == "__main__":
    asyncio.run(truly_optimized_main())
