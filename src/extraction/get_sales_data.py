#!/usr/bin/env python3
"""
get_sales_data.py - Clean Sales Data Extraction Script
Step 5: Advanced filters - year, scores, and result (Sold).
"""

import asyncio
import logging
import sys
import os
import re
from typing import List, Dict, Optional, Tuple
from itertools import cycle
from datetime import date, datetime
import traceback

# Add the project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.config import auction_sites, manufacturer_configs
from playwright.async_api import async_playwright, Page
from src.core.db import get_database_handler

# Setup comprehensive logging for task scheduler
def setup_logging():
    """Setup logging configuration for task scheduler monitoring"""
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create timestamped log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f"get_sales_data_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # Also log to console
        ]
    )
    
    # Suppress verbose HTTP and database logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("src.core.db").setLevel(logging.WARNING)
    logging.getLogger("supabase").setLevel(logging.WARNING)
    
    return log_filename

# Initialize logging
log_filename = setup_logging()
logger = logging.getLogger(__name__)

# === AGE CALCULATION FUNCTION ===
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
        
        # Calculate minimum year based on current date and age limit
        current_year = date.today().year
        min_year = current_year - age_limit
        
        return min_year
        
    except Exception as e:
        # Fallback to a reasonable default
        return date.today().year - 6

# === POOLING SYSTEM ===
class RoundRobinPool:
    """Manages round-robin distribution of make/model combinations across sites."""
    
    def __init__(self, auction_sites: Dict, manufacturer_configs: Dict):
        self.sites = list(auction_sites.keys())
        self.site_cycle = cycle(self.sites)
        self.workloads = {site: [] for site in self.sites}
        self.manufacturer_configs = manufacturer_configs
    
    def distribute_workload_round_robin(self, workload: List[Tuple[str, str, str]], num_sessions: int) -> List[List[Tuple[str, str, str]]]:
        """
        Distribute workload in true round-robin fashion across sessions.
        Each session gets items from throughout the entire workload list.
        """
        sessions = [[] for _ in range(num_sessions)]
        for i, item in enumerate(workload):
            session_index = i % num_sessions
            sessions[session_index].append(item)
        
        # Remove empty sessions
        sessions = [session for session in sessions if session]
        
        return sessions
        
    def generate_make_model_combinations(self) -> List[Tuple[str, str, str]]:
        """Generate all make/model combinations from manufacturer configs."""
        combinations = []
        for make, models in self.manufacturer_configs.items():
            for model in models.keys():
                combinations.append((make, model, f"{make} {model}"))
        return combinations
    
    def distribute_workload(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """Distribute make/model combinations in round-robin fashion."""
        combinations = self.generate_make_model_combinations()
        
        print(f"📊 Total make/model combinations: {len(combinations)}")
        print(f"🌐 Distributing across {len(self.sites)} sites...")
        
        # Reset workloads
        self.workloads = {site: [] for site in self.sites}
        
        # Distribute in round-robin
        for i, (make, model, description) in enumerate(combinations):
            site = next(self.site_cycle)
            self.workloads[site].append((make, model, description))
            
            if i < 10:  # Show first 10 assignments for debugging
                print(f"  {description} → {site}")
        
        # Show distribution summary
        print(f"\n📋 Workload Distribution:")
        for site, workload in self.workloads.items():
            print(f"  {site}: {len(workload)} combinations")
        
        return self.workloads

# === SMART MATCHING UTILITIES ===
def extract_base_name(option_text: str) -> str:
    """Extract the base name from option text, removing counts and extra info."""
    cleaned = re.sub(r'\s*\([^)]*\)', '', option_text).strip()
    return cleaned

def calculate_match_score(search_term: str, option_text: str) -> Tuple[int, str]:
    """Calculate a match score for an option. Higher score = better match."""
    search_upper = search_term.upper().strip()
    base_name = extract_base_name(option_text).upper().strip()
    if search_upper == base_name:
        return (100, "exact_match")
    if search_upper in base_name and base_name.startswith(search_upper):
        base_words = base_name.split()
        search_words = search_upper.split()
        if len(base_words) == len(search_words):
            return (95, "base_name_exact")
    search_pattern = r'\b' + re.escape(search_upper) + r'\b'
    if re.search(search_pattern, base_name):
        words_in_base = len(base_name.split())
        words_in_search = len(search_upper.split())
        if words_in_base == words_in_search:
            return (90, "word_boundary_exact")
        elif words_in_base == words_in_search + 1:
            return (85, "word_boundary_close")
        else:
            return (80, "word_boundary_variant")
    if base_name.startswith(search_upper):
        return (75, "prefix_match")
    if search_upper in base_name:
        return (70, "contains_match")
    return (0, "no_match")

def find_best_match(search_term: str, options: List[Dict]) -> Tuple[Optional[str], Optional[str], str]:
    """Find the best matching option using intelligent scoring."""
    if not options:
        return (None, None, "no_options")
    scored_matches = []
    for option in options:
        score, reason = calculate_match_score(search_term, option['text'])
        if score > 0:
            scored_matches.append({
                'value': option['value'],
                'text': option['text'],
                'score': score,
                'reason': reason,
                'base_name': extract_base_name(option['text'])
            })
    if not scored_matches:
        return (None, None, "no_matches_found")
    scored_matches.sort(key=lambda x: (-x['score'], len(x['text'])))
    best_match = scored_matches[0]
    match_info = f"{best_match['reason']} (score: {best_match['score']})"
    if len(scored_matches) > 1:
        runner_up = scored_matches[1]
        match_info += f" [beat: {runner_up['text']} ({runner_up['score']})]"
    return (best_match['value'], best_match['text'], match_info)

# === SMART FORM FILLING ===
async def get_dropdown_options(page: Page, selector: str) -> List[Dict]:
    """Extract all options from a dropdown as a list of dicts with text and value."""
    return await page.evaluate(f'''
        () => {{
            const options = [];
            const select = document.querySelector('{selector}');
            if (select) {{
                Array.from(select.options).forEach(opt => {{
                    if (opt.value && opt.value !== '-1' && opt.value !== '') {{
                        options.push({{text: opt.text.trim(), value: opt.value}});
                    }}
                }});
            }}
            return options;
        }}
    ''')

# Helper functions with error handling
async def get_text_safe(cells, idx, debug_msgs, row_num, field_name):
    try:
        if idx >= len(cells):
            debug_msgs.append(f"Row {row_num}: Cell {idx} ({field_name}) not found - only {len(cells)} cells")
            return ''
        el = cells[idx]
        return (await el.inner_text()).strip()
    except Exception as e:
        debug_msgs.append(f"Row {row_num}: Failed to extract {field_name} from cell {idx}: {e}")
        return ''

async def get_img_url_safe(cells, idx, debug_msgs, row_num):
    try:
        if idx >= len(cells):
            return ''
        el = cells[idx]
        a_tag = await el.query_selector('a')
        if a_tag:
            href = await a_tag.get_attribute('href')
            return href if href else ''
        return ''
    except Exception as e:
        debug_msgs.append(f"Row {row_num}: Failed to extract image HREF from cell {idx}: {e}")
        return ''

async def get_lot_link_safe(cells, idx, debug_msgs, row_num):
    try:
        if idx >= len(cells):
            return ''
        el = cells[idx]
        a_tag = await el.query_selector('a.red')  # Look for the red link class
        if a_tag:
            href = await a_tag.get_attribute('href')
            return href if href else ''
        return ''
    except Exception as e:
        debug_msgs.append(f"Row {row_num}: Failed to extract lot link HREF from cell {idx}: {e}")
        return ''

async def get_price_safe(cells, idx, prefix, debug_msgs, row_num):
    try:
        if idx >= len(cells):
            debug_msgs.append(f"Row {row_num}: Price cell {idx} not found - only {len(cells)} cells")
            return ''
        el = cells[idx]
        price_div = await el.query_selector(f'div[id^="{prefix}"]')
        if price_div:
            return (await price_div.inner_text()).strip()
        return (await el.inner_text()).strip()
    except Exception as e:
        debug_msgs.append(f"Row {row_num}: Failed to extract price from cell {idx}: {e}")
        return ''

async def extract_sales_data_from_results(page: Page, debug_msgs=None, session_name: str = None) -> list:
    # Print debug messages in real time
    def log(msg):
        print(msg, flush=True)
        # Only log important messages, not every debug step
        if "Starting pagination" in msg or "Processing page" in msg or "Total data rows" in msg or "No data rows" in msg or "Pagination complete" in msg:
            if session_name:
                logger.info(f"{session_name} - {msg}")
            else:
                logger.info(msg)

    all_results = []
    page_num = 1
    
    log("🚀 Starting pagination extraction...")
    
    try:
        while True:
            log(f"📄 Processing page {page_num}...")
            
            # Alternative approach - show columns directly
            await page.evaluate("""
                // Show all hidden columns
                const hiddenElements = document.querySelectorAll('[style*=\"display: none\"]');
                hiddenElements.forEach(el => el.style.display = '');
                // Ensure main table is visible
                const mainTable = document.getElementById('mainTable');
                if (mainTable) mainTable.style.display = '';
            """)
            
            # Wait a bit for DOM to update
            await asyncio.sleep(0.5)  # Reduced from 1 second
            
            # Check total rows on current page
            total_rows = await page.evaluate("""
                document.querySelectorAll('#mainTable tr[id^=\"cell_\"]').length
            """)
            log(f"Page {page_num}: Total data rows found: {total_rows}")
            
            # Debug: Log when we find data
            if total_rows > 0 and session_name:
                logger.info(f"{session_name} - Found {total_rows} data rows on page {page_num}")
            elif session_name:
                logger.debug(f"{session_name} - No data rows found on page {page_num}")
            
            if total_rows == 0:
                log(f"Page {page_num}: No data rows found, stopping pagination")
                break
            
            # Extract data from current page
            rows = await page.query_selector_all('#mainTable tr[id^="cell_"]')
            page_results = []
            for i, row in enumerate(rows, start=1):
                try:
                    cells = await row.query_selector_all('td')
                    # Suppress per-row logs
                    if not cells or len(cells) < 15:
                        continue
                    result = {}
                    try:
                        result['date'] = await get_text_safe(cells, 0, None, i, 'date')
                        result['lot_number'] = await get_text_safe(cells, 1, None, i, 'lot_number')
                        result['lot_link'] = await get_lot_link_safe(cells, 1, None, i)  # Extract lot link from lot number cell
                        result['auction'] = await get_text_safe(cells, 2, None, i, 'auction')
                        result['photo_url'] = await get_img_url_safe(cells, 3, None, i)
                        result['maker'] = await get_text_safe(cells, 4, None, i, 'maker')
                        result['model'] = await get_text_safe(cells, 5, None, i, 'model')
                        
                        # Debug: Log the first record to see what's being extracted
                        if i == 1 and session_name:
                            logger.info(f"{session_name} - Raw extraction - Maker: '{result.get('maker')}', Model: '{result.get('model')}', Lot: '{result.get('lot_number')}'")
                            logger.info(f"{session_name} - Raw result data: {result}")
                        result['grade'] = await get_text_safe(cells, 6, None, i, 'grade')
                        result['year'] = await get_text_safe(cells, 7, None, i, 'year')
                        result['mileage'] = await get_text_safe(cells, 8, None, i, 'mileage')
                        result['displacement'] = await get_text_safe(cells, 9, None, i, 'displacement')
                        result['transmission'] = await get_text_safe(cells, 10, None, i, 'transmission')
                        result['color'] = await get_text_safe(cells, 12, None, i, 'color')
                        result['model_type'] = await get_text_safe(cells, 13, None, i, 'model_type')
                        result['end_price'] = await get_price_safe(cells, 16, 'priceE', None, i)
                        result['result'] = await get_text_safe(cells, 17, None, i, 'result')
                        result['scores'] = await get_text_safe(cells, 18, None, i, 'scores')
                        # Map fields to match save_sales_data expectations
                        sales_record = {
                            'site_name': result.get('auction'),
                            'lot_number': result.get('lot_number'),
                            'make': result.get('maker'),
                            'model': result.get('model'),
                            'year': result.get('year'),
                            'grade': result.get('grade'),
                            'model_type': result.get('model_type'),
                            'mileage': result.get('mileage'),
                            'displacement': result.get('displacement'),
                            'transmission': result.get('transmission'),
                            'color': result.get('color'),
                            'auction': result.get('auction'),
                            'sale_date': result.get('date'),
                            'end_price': result.get('end_price'),
                            'result': result.get('result'),
                            'scores': result.get('scores'),
                            'url': result.get('photo_url'),      # Image URL
                            'lot_link': result.get('lot_link'),  # Lot number link (the href you need)
                        }
                        page_results.append(sales_record)
                    except Exception:
                        pass
                except Exception:
                    pass
            
            # Add page results to all results
            all_results.extend(page_results)
            log(f"Page {page_num}: Extracted {len(page_results)} records")
            log(f"Running total extracted: {len(all_results)} vehicles")
            
            # Debug: Check what pagination elements exist
            # pagination_debug = await page.evaluate(
            #     """() => {
            #         const links = document.querySelectorAll('a');
            #         const paginationInfo = [];
            #         for (const link of links) {
            #             const text = link.textContent || '';
            #             const href = link.getAttribute('href') || '';
            #             if (text.includes('»') || text.includes('»»') || href.includes('javascript:page(')) {
            #                 paginationInfo.push({
            #                     text: text.trim(),
            #                     href: href
            #                 });
            #             }
            #         }
            #         return paginationInfo;
            #     }"""
            # )
            # log(f"Page {page_num}: Found pagination elements: {pagination_debug}")
            
            # Also check for any text containing pagination info
            # page_text = await page.evaluate(
            #     """() => {
            #         const bodyText = document.body.textContent;
            #         const paginationMatch = bodyText.match(/Pages:.*?\\d+/s);
            #         return paginationMatch ? paginationMatch[0] : 'No pagination text found';
            #     }"""
            # )
            # log(f"Page {page_num}: Pagination text: {page_text}")
            
            # Check for next page using the specific pagination structure
            next_page_exists = await page.evaluate(
                """(currentPage) => {
                    const nextButtons = document.querySelectorAll('a');
                    for (const btn of nextButtons) {
                        const text = btn.textContent || '';
                        const pageNum = parseInt(text, 10);
                        if (!isNaN(pageNum) && pageNum > currentPage) {
                            return true;
                        }
                    }
                    return false;
                }""",
                page_num
            )
            log(f"Page {page_num}: Next page exists: {next_page_exists}")
            
            if not next_page_exists:
                log(f"No next page found, stopping pagination at page {page_num}")
                break
            
            # Try to click next page using the specific pagination structure
            next_clicked = await page.evaluate(
                """(currentPage) => {
                    // Only click the button whose text is exactly currentPage + 1
                    const nextButtons = document.querySelectorAll('a');
                    const nextPageNum = (currentPage + 1).toString();
                    for (const btn of nextButtons) {
                        const text = btn.textContent && btn.textContent.trim();
                        if (text === nextPageNum) {
                            btn.click();
                            return true;
                        }
                    }
                    // No next page found
                    return false;
                }""",
                page_num
            )
            log(f"Page {page_num}: Next page clicked: {next_clicked}")
            
            if not next_clicked:
                log(f"Could not click next page button, stopping pagination at page {page_num}")
                break
            
            # Wait for page to load
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
                await asyncio.sleep(1)  # Reduced from 3 seconds
                # Wait for the results table to be visible again
                await page.wait_for_selector('#mainTable', timeout=10000)
            except Exception as e:
                log(f"Page {page_num + 1} load timeout: {e}, stopping pagination")
                break
            
            page_num += 1
            
            # Safety limit to prevent infinite loops
            if page_num > 50:
                log(f"Reached maximum page limit (50), stopping pagination")
                break
        
        log(f"✅ Pagination complete: Processed {page_num} pages, extracted {len(all_results)} total records")
        return all_results
        
    except Exception as e:
        log(f"Failed to extract sales data: {e}")
        return all_results

async def process_site_workload_parallel(site_name: str, site_config: dict, workload: List[Tuple[str, str, str]], num_sessions: int = 3):
    """
    Process site workload using multiple parallel browser sessions for maximum performance.
    """
    logger.info(f"Starting parallel processing for site: {site_name} with {len(workload)} combinations using {num_sessions} sessions")
    print(f"🌐 {site_name}: {len(workload)} combinations ({num_sessions} parallel sessions)")
    
    # Create a pool instance to use the round-robin distribution method
    from src.core.config import auction_sites, manufacturer_configs
    pool = RoundRobinPool(auction_sites, manufacturer_configs)
    
    # Distribute workload in true round-robin fashion across sessions
    workload_chunks = pool.distribute_workload_round_robin(workload, num_sessions)
    
    logger.info(f"✅ True round-robin distribution: {len(workload)} combinations across {len(workload_chunks)} sessions")
    print(f"  🔄 Round-robin distribution applied")
    
    # Log the distribution for debugging
    for i, chunk in enumerate(workload_chunks):
        logger.info(f"Session-{i+1}: {len(chunk)} combinations")
        if chunk:
            first_item = chunk[0]
            last_item = chunk[-1]
            logger.info(f"Session-{i+1}: First={first_item[2]}, Last={last_item[2]}")
            print(f"    Session-{i+1}: {len(chunk)} items (First: {first_item[2]}, Last: {last_item[2]})")
    
    logger.info(f"Split {len(workload)} combinations into {len(workload_chunks)} chunks")
    
    # Create tasks for each session
    tasks = []
    for i, chunk in enumerate(workload_chunks):
        task = asyncio.create_task(
            process_site_session(site_name, site_config, chunk, f"Session-{i+1}")
        )
        tasks.append(task)
    
    logger.info(f"Created {len(tasks)} parallel sessions for {site_name}")
    
    # Wait for all sessions to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Aggregate results
    total_saved = 0
    total_failed = 0
    for result in results:
        if isinstance(result, tuple):
            saved, failed = result
            total_saved += saved
            total_failed += failed
        else:
            logger.error(f"Session failed with exception: {result}")
    
    logger.info(f"{site_name} - Final Summary: {len(workload)} combinations processed. Total saved: {total_saved}, Total failed: {total_failed}")
    print(f"  📊 {site_name}: Final - Saved: {total_saved}, Failed: {total_failed}")
    print(f"✅ {site_name}: Done")

async def process_site_session(site_name: str, site_config: dict, workload_chunk: List[Tuple[str, str, str]], session_name: str):
    """
    Process a chunk of workload using a single browser session.
    """
    logger.info(f"Starting {session_name} for {site_name} with {len(workload_chunk)} combinations")
    
    playwright = None
    browser = None
    db_handler = None
    
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=True)
        db_handler = get_database_handler()
        
        context = await browser.new_context()
        page = await context.new_page()
        
        login_success = await login_to_site(page, site_name, site_config)
        
        if login_success:
            logger.info(f"{session_name} - Login successful for {site_name}")
            await set_session_filters(page)
            
            total_saved = 0
            total_failed = 0
            total_extracted = 0
            
            session_restart_needed = False
            for i, (make, model, description) in enumerate(workload_chunk):
                # Check if session restart is needed
                if session_restart_needed:
                    logger.warning(f"{session_name} - Restarting session due to previous critical error")
                    break
                
                # Only log every 20th item or when there are results
                should_log = (i + 1) % 20 == 0 or i == 0
                if should_log:
                    logger.info(f"{session_name} - Processing [{i+1}/{len(workload_chunk)}] {description}")
                
                try:
                    logger.debug(f"{session_name} - Attempting search for {make} {model}")
                    form_success, sales_count, debug_msgs = await fill_search_form_with_filters(page, make, model, session_name)
                    logger.debug(f"{session_name} - Search result: success={form_success}, count={sales_count}")
                    
                    if form_success:
                        sales_data = await extract_sales_data_from_results(page, session_name=session_name)
                        
                        # Only log if we found data or it's a milestone
                        if len(sales_data) > 0 or should_log:
                            logger.info(f"{session_name} - {description}: Found {len(sales_data)} records")
                        
                        # Batch database saves for better performance
                        batch_records = []
                        saved_count = 0
                        failed_count = 0
                        
                        # Initialize unique_records to avoid UnboundLocalError
                        unique_records = []
                        
                        # Log extraction progress immediately
                        if len(sales_data) > 0:
                            logger.info(f"{session_name} - {description}: Extracted {len(sales_data)} records, processing batches...")
                        elif should_log:
                            logger.info(f"{session_name} - {description}: No records found")
                        
                        # Deduplicate records based on site_name and lot_number to avoid constraint violations
                        seen_records = set()
                        
                        for vehicle in sales_data:
                            try:
                                # Add required fields for db schema
                                vehicle['site_name'] = site_name
                                # The make field is already correctly mapped in extract_sales_data_from_results
                                # vehicle['make'] is already set correctly
                                # The url, lot_link, and sale_date are already correctly mapped in extract_sales_data_from_results
                                # vehicle['url'], vehicle['lot_link'], vehicle['sale_date'] are already set correctly
                                vehicle['image_url'] = ''
                                
                                # Create unique key for deduplication
                                unique_key = (vehicle.get('site_name'), vehicle.get('lot_number'))
                                
                                # Skip if we've already seen this record
                                if unique_key in seen_records:
                                    logger.debug(f"{session_name} - {description}: Skipping duplicate record - Site: {vehicle.get('site_name')}, Lot: {vehicle.get('lot_number')}")
                                    continue
                                
                                seen_records.add(unique_key)
                                unique_records.append(vehicle)
                                
                                # Debug: Log the first vehicle to check field mapping
                                if len(unique_records) == 1:
                                    logger.info(f"{session_name} - {description}: Sample vehicle - Make: '{vehicle.get('make')}', Model: '{vehicle.get('model')}', Lot: '{vehicle.get('lot_number')}'")
                                    logger.info(f"{session_name} - {description}: Image URL: '{vehicle.get('url')}', Lot Link: '{vehicle.get('lot_link')}', Sale Date: '{vehicle.get('sale_date')}'")
                                    logger.info(f"{session_name} - {description}: Raw vehicle data keys: {list(vehicle.keys())}")
                                    logger.info(f"{session_name} - {description}: Raw vehicle data: {vehicle}")
                                
                                # Add to batch instead of individual save
                                batch_records.append(vehicle)
                                
                                # Process batch when it reaches 100 records
                                if len(batch_records) >= 100:
                                    logger.info(f"{session_name} - {description}: Saving batch of {len(batch_records)} records...")
                                    batch_success = await db_handler.save_sales_data_batch(batch_records)
                                    if batch_success:
                                        saved_count += len(batch_records)
                                        logger.info(f"{session_name} - {description}: ✅ Batch saved successfully ({len(batch_records)} records)")
                                    else:
                                        failed_count += len(batch_records)
                                        logger.warning(f"{session_name} - {description}: ❌ Batch save failed ({len(batch_records)} records)")
                                    batch_records = []
                                    
                            except Exception as e:
                                failed_count += 1
                                logger.error(f"{session_name} - Error processing vehicle {vehicle.get('lot_number')}: {e}")
                        
                        # Save any remaining records in the batch
                        if batch_records:
                            try:
                                logger.info(f"{session_name} - {description}: Saving final batch of {len(batch_records)} records...")
                                batch_success = await db_handler.save_sales_data_batch(batch_records)
                                if batch_success:
                                    saved_count += len(batch_records)
                                    logger.info(f"{session_name} - {description}: ✅ Final batch saved successfully ({len(batch_records)} records)")
                                else:
                                    failed_count += len(batch_records)
                                    logger.warning(f"{session_name} - {description}: ❌ Final batch save failed ({len(batch_records)} records)")
                            except Exception as e:
                                failed_count += len(batch_records)
                                logger.error(f"{session_name} - Error saving final batch: {e}")
                        
                        total_saved += saved_count
                        total_failed += failed_count
                        total_extracted += len(unique_records)  # Use deduplicated count
                        
                        # Log final results for this combination
                        if len(sales_data) > 0:
                            duplicates_removed = len(sales_data) - len(unique_records)
                            if duplicates_removed > 0:
                                logger.info(f"{session_name} - {description}: 📊 Final - Extracted: {len(sales_data)}, Unique: {len(unique_records)}, Duplicates: {duplicates_removed}, Saved: {saved_count}, Failed: {failed_count}")
                            else:
                                logger.info(f"{session_name} - {description}: 📊 Final - Extracted: {len(sales_data)}, Saved: {saved_count}, Failed: {failed_count}")
                        
                    else:
                        # Only log failures for milestone items
                        if should_log:
                            logger.warning(f"{session_name} - {description}: Search failed")
                        
                except Exception as e:
                    logger.error(f"{session_name} - {description}: Processing error: {e}")
                    logger.error(traceback.format_exc())
                    
                    # Check if this is a critical error that requires session restart
                    if "Execution context was destroyed" in str(e) or "Timeout" in str(e):
                        logger.warning(f"{session_name} - Critical error detected, will restart session after this item")
                        session_restart_needed = True
                        break
                
                await asyncio.sleep(0.5)
                
                # Log progress every 25 items
                if (i + 1) % 25 == 0:
                    logger.info(f"{session_name} - Progress: {i+1}/{len(workload_chunk)} completed. Extracted: {total_extracted}, Saved: {total_saved}, Failed: {total_failed}")
            
            logger.info(f"{session_name} - Completed: {len(workload_chunk)} combinations. Extracted: {total_extracted}, Saved: {total_saved}, Failed: {total_failed}")
            return total_saved, total_failed
            
        else:
            logger.error(f"{session_name} - Login failed for {site_name}")
            return 0, 0
            
    except Exception as e:
        logger.error(f"{session_name} - Critical error: {e}")
        logger.error(traceback.format_exc())
        return 0, 0
    finally:
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()

async def process_site_workload(site_name: str, site_config: dict, workload: List[Tuple[str, str, str]]):
    """
    Legacy single-session processing (kept for compatibility).
    Use process_site_workload_parallel for better performance.
    """
    return await process_site_workload_parallel(site_name, site_config, workload, num_sessions=3)

async def set_session_filters(page: Page) -> bool:
    """
    Set score and result filters once for the entire session.
    """
    try:
        # Set score filters (4, 4.5, 5, 6)
        score_filters = ['4', '4.5', '5', '6']
        for score in score_filters:
            score_checkbox = await page.query_selector(f'input[name="score[]"][value="{score}"]')
            if score_checkbox:
                await score_checkbox.check()
            else:
                logger.warning(f"Score {score} checkbox not found")
        
        # Set result filter to "Sold"
        result_select = await page.query_selector('select[name="result"]')
        if result_select:
            await result_select.select_option('1')  # Value "1" corresponds to "Sold"
        else:
            logger.warning("Result filter field not found")
        
        return True
        
    except Exception as e:
        logger.error(f"Could not set session filters: {e}")
        return False

async def fill_search_form_with_filters(page: Page, make: str, model: str, session_name: str = None) -> tuple:
    max_retries = 3
    debug_msgs = []
    for attempt in range(max_retries):
        try:
            await page.wait_for_selector('select[name="mrk"]', timeout=15000)
            make_options = await get_dropdown_options(page, 'select[name="mrk"]')
            make_value, make_text, make_info = find_best_match(make, make_options)
            if session_name:
                logger.debug(f"{session_name} - Make matching: '{make}' -> '{make_text}' (value: {make_value}, info: {make_info})")
            if not make_value:
                debug_msgs.append(f"Make '{make}' not found (smart match info: {make_info})")
                return False, 0, debug_msgs
            await page.select_option('select[name="mrk"]', make_value)
            await asyncio.sleep(0.5)  # Reduced from 2 seconds
            await page.wait_for_selector('select[name="mdl"]:not([disabled])', timeout=20000)
            await asyncio.sleep(0.5)  # Reduced from 1 second
            model_options = await get_dropdown_options(page, 'select[name="mdl"]')
            model_value, model_text, model_info = find_best_match(model, model_options)
            if session_name:
                logger.debug(f"{session_name} - Model matching: '{model}' -> '{model_text}' (value: {model_value}, info: {model_info})")
            if not model_value:
                debug_msgs.append(f"Model '{model}' not found (smart match info: {model_info})")
                return False, 0, debug_msgs
            await page.select_option('select[name="mdl"]', model_value)
            await asyncio.sleep(0.5)  # Reduced from 1 second
            min_year = calculate_min_year_for_vehicle(make, model)
            try:
                year1_field = await page.query_selector('input[name="year1"]')
                if year1_field:
                    await year1_field.fill(str(min_year))
                else:
                    debug_msgs.append("Year filter field not found")
            except Exception as e:
                debug_msgs.append(f"Could not set year filter: {e}")
            search_success = await click_search_button_with_retry(page)
            if session_name:
                logger.debug(f"{session_name} - Search button click: {search_success}")
            if not search_success:
                if attempt < max_retries - 1:
                    debug_msgs.append(f"Search button click failed, retrying... (attempt {attempt + 1})")
                    await asyncio.sleep(2)
                    continue
                else:
                    debug_msgs.append(f"Search button click failed after {max_retries} attempts")
                    return False, 0, debug_msgs
            # Wait for page to load after search, but with better error handling
            try:
                await page.wait_for_load_state('domcontentloaded', timeout=10000)
            except Exception as nav_error:
                if session_name:
                    logger.warning(f"{session_name} - Navigation timeout after search: {nav_error}")
                debug_msgs.append(f"Navigation timeout after search: {nav_error}")
                # Continue anyway - the page might be partially loaded
            
            # Set pagination to 100 records per page after search results load
            # Only attempt this if we're on the search results page (where setvs function exists)
            try:
                # Check if we're on the results page by looking for the main table
                results_table = await page.query_selector('#mainTable')
                if results_table:
                    # We're on the results page, try to set pagination
                    await page.evaluate("setvs(100);")
                    try:
                        await page.wait_for_load_state('domcontentloaded', timeout=3000)
                    except Exception as pagination_error:
                        if session_name:
                            logger.debug(f"{session_name} - Pagination setting timeout: {pagination_error}")
                        # Continue anyway - pagination might still be set
                    if session_name:
                        logger.debug(f"{session_name} - Set pagination to 100 records per page")
                        print(f"    📊 {session_name} - Set pagination to 100 records per page")
                    debug_msgs.append("Set pagination to 100 records per page")
                else:
                    if session_name:
                        logger.debug(f"{session_name} - Not on results page, skipping pagination setting")
                    debug_msgs.append("Not on results page, skipping pagination setting")
            except Exception as e:
                if session_name:
                    logger.debug(f"{session_name} - Failed to set pagination to 100: {e}")
                    print(f"    ⚠️ {session_name} - Failed to set pagination to 100: {e}")
                debug_msgs.append(f"Failed to set pagination to 100: {e}")
            
            sales_data = await extract_sales_data_from_results(page, debug_msgs, session_name)
            return True, len(sales_data), debug_msgs
        except Exception as e:
            if attempt < max_retries - 1:
                debug_msgs.append(f"Attempt {attempt + 1} failed: {e}, retrying...")
                await asyncio.sleep(2)
                try:
                    await page.reload()
                    await page.wait_for_load_state('domcontentloaded', timeout=8000)
                except Exception as reload_error:
                    if session_name:
                        logger.debug(f"{session_name} - Page reload failed: {reload_error}")
                    pass
            else:
                debug_msgs.append(f"Failed after {max_retries} attempts: {e}")
                return False, 0, debug_msgs
    return False, 0, debug_msgs

async def click_search_button_with_retry(page: Page, max_retries=3) -> bool:
    """Click search button with retry logic to handle element detachment."""
    for attempt in range(max_retries):
        try:
            # Check if page is still responsive
            try:
                await page.wait_for_timeout(100)  # Quick check if page is responsive
            except Exception:
                print(f"    ⚠️ Page not responsive, attempt {attempt + 1}")
                return False
            
            # Try different search button selectors with shorter timeout
            search_button = await page.query_selector('#btnSearch1, #btnSearch2, #btnSearsh, input[value="Search"]')
            if search_button:
                # Check if element is still attached and enabled
                is_attached = await search_button.is_visible()
                if is_attached:
                    # Click without waiting for navigation - we'll handle that separately
                    await search_button.click(timeout=5000)  # 5 seconds for click only
                    
                    # Wait for navigation with shorter timeout and better error handling
                    try:
                        await page.wait_for_load_state('domcontentloaded', timeout=8000)
                        return True
                    except Exception as nav_error:
                        print(f"    ⚠️ Navigation timeout after click (attempt {attempt + 1}): {nav_error}")
                        # Try to continue anyway - the page might be partially loaded
                        return True
                else:
                    print(f"    ⚠️ Search button not visible, retrying... (attempt {attempt + 1})")
            else:
                # Fallback: try clicking by value with shorter timeout
                await page.click('input[type="button"][value="Search"]', timeout=5000)
                
                # Wait for navigation with shorter timeout
                try:
                    await page.wait_for_load_state('domcontentloaded', timeout=8000)
                    return True
                except Exception as nav_error:
                    print(f"    ⚠️ Navigation timeout after click (attempt {attempt + 1}): {nav_error}")
                    # Try to continue anyway
                    return True
                
        except Exception as e:
            print(f"    ⚠️ Search button click failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)  # Slightly longer wait between attempts
                # Try to refresh the page state with shorter timeout
                try:
                    await page.wait_for_load_state('domcontentloaded', timeout=5000)
                except:
                    pass  # Continue even if page load check fails
    
    print(f"    ❌ Failed to click search button after {max_retries} attempts")
    return False

async def login_to_site(page: Page, site_name: str, site_config: dict) -> bool:
    try:
        sales_data_url = site_config['scraping']['sales_data_url']
        
        await page.goto(sales_data_url, wait_until='networkidle', timeout=30000)
        login_form = await page.query_selector('form')
        
        if login_form:
            username = site_config['username']
            password = site_config['password']
            
            await page.fill('#usr_name', username)
            await page.fill('#usr_pwd', password)
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state('networkidle')
            
            logger.info(f"Login successful for {site_name}")
            return True
        else:
            logger.warning(f"No login form found for {site_name}")
            return True  # Some sites might not require login
    except Exception as e:
        logger.error(f"Login failed for {site_name}: {e}")
        logger.error(traceback.format_exc())
        print(f"❌ Login failed for {site_name}: {e}")
        return False

async def main():
    start_time = datetime.now()
    logger.info("="*60)
    logger.info("SALES DATA EXTRACTION - STEP 5: ADVANCED FILTERS")
    logger.info("="*60)
    logger.info(f"Log file: {log_filename}")
    
    print(f"\n{'='*60}")
    print("SALES DATA EXTRACTION - STEP 5: ADVANCED FILTERS")
    print(f"{'='*60}")
    
    try:
        # Initialize the pooling system
        logger.info("Initializing pooling system")
        pool = RoundRobinPool(auction_sites, manufacturer_configs)
        
        # Distribute workload across sites
        logger.info("Distributing workload across sites")
        workloads = pool.distribute_workload()
        
        # Log the site distribution for debugging
        logger.info("Site workload distribution:")
        for site_name, site_workload in workloads.items():
            logger.info(f"  {site_name}: {len(site_workload)} combinations")
            if site_workload:
                first_make = site_workload[0][0] if site_workload else "N/A"
                last_make = site_workload[-1][0] if site_workload else "N/A"
                logger.info(f"    {site_name}: First make={first_make}, Last make={last_make}")
        
        logger.info("Launching concurrent processing with advanced filters")
        print(f"\n🚀 Launching concurrent processing with advanced filters...")
        print(f"📅 Filters: Year (Jamaica age limits), Scores (4, 4.5, 5, 6), Result (Sold)")
        print(f"🔄 True round-robin distribution enabled")
        
        # Create tasks for each site with its assigned workload
        tasks = []
        for site_name, site_config in auction_sites.items():
            site_workload = workloads[site_name]
            logger.info(f"Creating task for {site_name} with {len(site_workload)} combinations")
            task = asyncio.create_task(
                process_site_workload(site_name, site_config, site_workload)
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        logger.info(f"Starting {len(tasks)} concurrent tasks")
        await asyncio.gather(*tasks)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"All sites processed successfully in {duration}")
        print(f"\n{'='*60}")
        print("✅ All sites processed with advanced filters!")
        print(f"{'='*60}")
        
    except Exception as e:
        logger.error(f"Critical error in main function: {e}")
        logger.error(traceback.format_exc())
        print(f"❌ Critical error: {e}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting get_sales_data.py script")
        asyncio.run(main())
        logger.info("get_sales_data.py script completed successfully")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user")
        print("\nScript interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        logger.error(traceback.format_exc())
        print(f"Script failed: {e}")
        sys.exit(1) 