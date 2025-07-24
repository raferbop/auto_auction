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
from datetime import date

# Add the project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.core.config import auction_sites, manufacturer_configs
from playwright.async_api import async_playwright, Page
from src.core.db import get_database_handler

# Configure logging to suppress verbose HTTP and database logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("src.core.db").setLevel(logging.WARNING)
logging.getLogger("supabase").setLevel(logging.WARNING)

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

async def extract_sales_data_from_results(page: Page, debug_msgs=None) -> list:
    # Print debug messages in real time
    def log(msg):
        print(msg, flush=True)

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
            await asyncio.sleep(1)
            
            # Check total rows on current page
            total_rows = await page.evaluate("""
                document.querySelectorAll('#mainTable tr[id^=\"cell_\"]').length
            """)
            log(f"Page {page_num}: Total data rows found: {total_rows}")
            
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
                        result['auction'] = await get_text_safe(cells, 2, None, i, 'auction')
                        result['photo_url'] = await get_img_url_safe(cells, 3, None, i)
                        result['maker'] = await get_text_safe(cells, 4, None, i, 'maker')
                        result['model'] = await get_text_safe(cells, 5, None, i, 'model')
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
                        # Save each record to the database in real time
                        from src.core.db import get_database_handler
                        db_handler = get_database_handler()
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
                            'url': result.get('photo_url'),
                            'lot_link': result.get('photo_url'),
                        }
                        await db_handler.save_sales_data(sales_record)
                        page_results.append(result)
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
                await asyncio.sleep(3)  # Additional wait for dynamic content
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

async def process_site_workload(site_name: str, site_config: dict, workload: List[Tuple[str, str, str]]):
    print(f"🌐 {site_name}: {len(workload)} combinations")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    # Initialize db handler once per site
    db_handler = get_database_handler()
    try:
        context = await browser.new_context()
        page = await context.new_page()
        login_success = await login_to_site(page, site_name, site_config)
        if login_success:
            print(f"  🔐 {site_name}: Logged in, setting session filters...")
            await set_session_filters(page)
            for i, (make, model, description) in enumerate(workload):
                summary = f"[{i+1}/{len(workload)}] {description}: "
                form_success, sales_count, debug_msgs = await fill_search_form_with_filters(page, make, model)
                # Extract sales data for saving
                if form_success:
                    print(f"  {summary}Search complete, {sales_count} records")
                    # Extract the actual sales data from debug_msgs (last extraction)
                    # Instead, re-extract to get the data list
                    sales_data = await extract_sales_data_from_results(page)
                    for vehicle in sales_data:
                        # Add required fields for db schema
                        vehicle['site_name'] = site_name
                        vehicle['make'] = vehicle.get('maker')
                        vehicle['sale_date'] = vehicle.get('date')
                        vehicle['url'] = vehicle.get('photo_url')
                        vehicle['lot_link'] = vehicle.get('photo_url')
                        vehicle['image_url'] = ''  # Optionally extract image src if needed
                        success = await db_handler.save_sales_data(vehicle)
                        if success:
                            print(f"    ✅ Saved vehicle sale: {vehicle.get('lot_number')} ({vehicle.get('model')})")
                        else:
                            print(f"    ❌ Failed to save vehicle sale: {vehicle.get('lot_number')} ({vehicle.get('model')})")
                else:
                    print(f"  {summary}FAILED")
                for msg in debug_msgs:
                    print(f"    ⚠️ {msg}")
                await asyncio.sleep(1)
        await page.close()
        await context.close()
    except Exception as e:
        print(f"  ❌ {site_name}: Error: {e}")
    finally:
        await browser.close()
        await playwright.stop()
    print(f"✅ {site_name}: Done")

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
                print(f"    ✅ Score {score} selected")
            else:
                print(f"    ⚠️ Score {score} checkbox not found")
        
        # Set result filter to "Sold"
        result_select = await page.query_selector('select[name="result"]')
        if result_select:
            await result_select.select_option('1')  # Value "1" corresponds to "Sold"
            print(f"    ✅ Result filter set to Sold")
        else:
            print(f"    ⚠️ Result filter field not found")
        
        return True
        
    except Exception as e:
        print(f"    ❌ Could not set session filters: {e}")
        return False

async def fill_search_form_with_filters(page: Page, make: str, model: str) -> tuple:
    max_retries = 3
    debug_msgs = []
    for attempt in range(max_retries):
        try:
            await page.wait_for_selector('select[name="mrk"]', timeout=15000)
            make_options = await get_dropdown_options(page, 'select[name="mrk"]')
            make_value, make_text, make_info = find_best_match(make, make_options)
            if not make_value:
                debug_msgs.append(f"Make '{make}' not found (smart match info: {make_info})")
                return False, 0, debug_msgs
            await page.select_option('select[name="mrk"]', make_value)
            await asyncio.sleep(2)
            await page.wait_for_selector('select[name="mdl"]:not([disabled])', timeout=20000)
            await asyncio.sleep(1)
            model_options = await get_dropdown_options(page, 'select[name="mdl"]')
            model_value, model_text, model_info = find_best_match(model, model_options)
            if not model_value:
                debug_msgs.append(f"Model '{model}' not found (smart match info: {model_info})")
                return False, 0, debug_msgs
            await page.select_option('select[name="mdl"]', model_value)
            await asyncio.sleep(1)
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
            if not search_success:
                if attempt < max_retries - 1:
                    debug_msgs.append(f"Search button click failed, retrying... (attempt {attempt + 1})")
                    await asyncio.sleep(2)
                    continue
                else:
                    debug_msgs.append(f"Search button click failed after {max_retries} attempts")
                    return False, 0, debug_msgs
            await page.wait_for_load_state('networkidle', timeout=15000)
            sales_data = await extract_sales_data_from_results(page, debug_msgs)
            return True, len(sales_data), debug_msgs
        except Exception as e:
            if attempt < max_retries - 1:
                debug_msgs.append(f"Attempt {attempt + 1} failed: {e}, retrying...")
                await asyncio.sleep(2)
                try:
                    await page.reload()
                    await page.wait_for_load_state('networkidle', timeout=10000)
                except:
                    pass
            else:
                debug_msgs.append(f"Failed after {max_retries} attempts: {e}")
                return False, 0, debug_msgs
    return False, 0, debug_msgs

async def click_search_button_with_retry(page: Page, max_retries=3) -> bool:
    """Click search button with retry logic to handle element detachment."""
    for attempt in range(max_retries):
        try:
            # Try different search button selectors
            search_button = await page.query_selector('#btnSearch1, #btnSearch2, #btnSearsh, input[value="Search"]')
            if search_button:
                # Check if element is still attached and enabled
                is_attached = await search_button.is_visible()
                if is_attached:
                    await search_button.click()
                    return True
                else:
                    print(f"    ⚠️ Search button not visible, retrying... (attempt {attempt + 1})")
            else:
                # Fallback: try clicking by value
                await page.click('input[type="button"][value="Search"]')
                return True
                
        except Exception as e:
            print(f"    ⚠️ Search button click failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)  # Wait before retry
                # Try to refresh the page state
                await page.wait_for_load_state('networkidle')
    
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
        return True
    except Exception as e:
        print(f"❌ Login failed for {site_name}: {e}")
        return False

async def main():
    print(f"\n{'='*60}")
    print("SALES DATA EXTRACTION - STEP 5: ADVANCED FILTERS")
    print(f"{'='*60}")
    
    # Initialize the pooling system
    pool = RoundRobinPool(auction_sites, manufacturer_configs)
    
    # Distribute workload across sites
    workloads = pool.distribute_workload()
    
    print(f"\n🚀 Launching concurrent processing with advanced filters...")
    print(f"📅 Filters: Year (Jamaica age limits), Scores (4, 4.5, 5, 6), Result (Sold)")
    
    # Create tasks for each site with its assigned workload
    tasks = []
    for site_name, site_config in auction_sites.items():
        site_workload = workloads[site_name]
        task = asyncio.create_task(
            process_site_workload(site_name, site_config, site_workload)
        )
        tasks.append(task)
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    print(f"\n{'='*60}")
    print("✅ All sites processed with advanced filters!")
    print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main()) 