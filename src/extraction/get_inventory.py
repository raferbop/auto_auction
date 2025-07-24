#!/usr/bin/env python3
"""
get_inventory.py - Check available make/models and vehicle counts for AutoPacific

Simple approach:
1. Submit make/model with filters on initial form
2. Detect when redirected to results page
3. Extract vehicle count from results
4. Use search form on results page for next model
5. Repeat
"""

import asyncio
import logging
import re
from datetime import date
from typing import Dict, List, Optional, Tuple
from playwright.async_api import async_playwright, Page
# Simple table formatter (no external dependencies)
def format_table(data, headers):
    """Format data as a simple table."""
    if not data:
        return "No data to display"
    
    # Calculate column widths
    col_widths = []
    for i, header in enumerate(headers):
        max_width = len(header)
        for row in data:
            max_width = max(max_width, len(str(row[i])))
        col_widths.append(max_width + 2)  # Add padding
    
    # Create separator line
    separator = "+" + "+".join("-" * width for width in col_widths) + "+"
    
    # Build table
    table_lines = []
    table_lines.append(separator)
    
    # Header row
    header_row = "|"
    for i, header in enumerate(headers):
        header_row += f" {header:<{col_widths[i]-1}}|"
    table_lines.append(header_row)
    table_lines.append(separator)
    
    # Data rows
    for row in data:
        data_row = "|"
        for i, cell in enumerate(row):
            if isinstance(cell, int):
                data_row += f" {cell:>{col_widths[i]-1}}|"
            else:
                data_row += f" {str(cell):<{col_widths[i]-1}}|"
        table_lines.append(data_row)
    
    table_lines.append(separator)
    return "\n".join(table_lines)

# Import configurations
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from config.auction_site_config import auction_sites
from config.manufacturer_config import manufacturer_configs

# Suppress verbose logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("src.core.db").setLevel(logging.WARNING)
logging.getLogger("supabase").setLevel(logging.WARNING)

def create_autopacific_batch() -> List[Dict]:
    """Create a single batch of all make/model combinations for AutoPacific only."""
    all_combinations = []
    for make, models in manufacturer_configs.items():
        for model in models.keys():
            all_combinations.append({'make': make, 'model': model})
    return all_combinations

def calculate_min_year_for_vehicle(make: str, model: str) -> int:
    """Calculate minimum year for a specific make/model combination."""
    try:
        if make in manufacturer_configs and model in manufacturer_configs[make]:
            age_limit = manufacturer_configs[make][model]['age_limit']
        else:
            age_limit = 6
        
        current_year = date.today().year
        min_year = current_year - age_limit
        return min_year
        
    except Exception as e:
        return date.today().year - 6

def extract_base_model_name(option_text: str) -> str:
    """
    Extract the base model name from option text, removing counts and extra info.
    
    Examples:
    - "ACCORD (267556)" -> "ACCORD"
    - "ACCORD PHEV (1)" -> "ACCORD PHEV"
    - "CR-V HYBRID (45)" -> "CR-V HYBRID"
    """
    # Remove everything in parentheses (vehicle counts)
    cleaned = re.sub(r'\s*\([^)]*\)', '', option_text).strip()
    return cleaned

def calculate_model_match_score(search_model: str, option_text: str) -> Tuple[int, str]:
    """
    Calculate a match score for a model option. Higher score = better match.
    
    Returns: (score, reason) where score is:
    100+ = Exact match
    90-99 = Base model match (preferred)
    80-89 = Variant match but good
    70-79 = Partial match
    0-69 = Poor/no match
    """
    search_upper = search_model.upper().strip()
    base_model = extract_base_model_name(option_text).upper().strip()
    
    # Score 100: Perfect exact match
    if search_upper == base_model:
        return (100, "exact_match")
    
    # Score 95: Base model match (search term matches exactly but option has extra info)
    # This handles: "ACCORD" matching "ACCORD (267556)" 
    if search_upper in base_model and base_model.startswith(search_upper):
        # Check if it's truly the base model (no additional descriptive words)
        base_words = base_model.split()
        search_words = search_upper.split()
        
        if len(base_words) == len(search_words):
            return (95, "base_model_exact")
    
    # Score 90: Base model with exact word boundary match
    # This ensures "ACCORD" matches "ACCORD" but not "ACCORD PHEV"
    search_pattern = r'\b' + re.escape(search_upper) + r'\b'
    if re.search(search_pattern, base_model):
        # Prefer shorter model names (base models over variants)
        words_in_base = len(base_model.split())
        words_in_search = len(search_upper.split())
        
        if words_in_base == words_in_search:
            return (90, "word_boundary_exact")
        elif words_in_base == words_in_search + 1:
            return (85, "word_boundary_close") 
        else:
            return (80, "word_boundary_variant")
    
    # Score 75: Partial match but search term is at the beginning
    if base_model.startswith(search_upper):
        return (75, "prefix_match")
    
    # Score 70: Contains search term but not at beginning
    if search_upper in base_model:
        return (70, "contains_match")
    
    # Score 0: No match
    return (0, "no_match")

def find_best_model_match_smart(search_model: str, model_options: List[Dict]) -> Tuple[Optional[str], Optional[str], str]:
    """
    Find the best matching model using intelligent scoring.
    
    Returns: (model_value, matched_text, match_reason)
    """
    if not model_options:
        return (None, None, "no_options")
    
    # Score all options
    scored_matches = []
    
    for option in model_options:
        score, reason = calculate_model_match_score(search_model, option['text'])
        if score > 0:  # Only include actual matches
            scored_matches.append({
                'value': option['value'],
                'text': option['text'],
                'score': score,
                'reason': reason,
                'base_model': extract_base_model_name(option['text'])
            })
    
    if not scored_matches:
        return (None, None, "no_matches_found")
    
    # Sort by score (descending), then by text length (ascending - prefer shorter names)
    scored_matches.sort(key=lambda x: (-x['score'], len(x['text'])))
    
    best_match = scored_matches[0]
    
    # Debug info for complex cases
    match_info = f"{best_match['reason']} (score: {best_match['score']})"
    if len(scored_matches) > 1:
        runner_up = scored_matches[1]
        match_info += f" [beat: {runner_up['text']} ({runner_up['score']})]"
    
    return (best_match['value'], best_match['text'], match_info)

async def set_score_filter(page: Page) -> bool:
    """Set score filter to select scores 4, 4.5, 5, and 6."""
    try:
        target_scores = ['4', '4.5', '5', '6']
        
        # Try multi-select dropdown first (results page form)
        score_select = await page.query_selector('select[name="score[]"]')
        if score_select:
            await page.evaluate("""
                (scores) => {
                    const select = document.querySelector('select[name="score[]"]');
                    if (select) {
                        Array.from(select.options).forEach(option => option.selected = false);
                        scores.forEach(score => {
                            const option = Array.from(select.options).find(opt => opt.value === score);
                            if (option) {
                                option.selected = true;
                            }
                        });
                    }
                }
            """, target_scores)
            return True
        
        # Try checkboxes (initial form)
        score_checkboxes = await page.query_selector_all('input[name="score[]"]')
        if score_checkboxes:
            for checkbox in score_checkboxes:
                await checkbox.uncheck()
            
            for checkbox in score_checkboxes:
                value = await checkbox.get_attribute('value')
                if value in target_scores:
                    await checkbox.check()
            return True
        
        return False
        
    except Exception as e:
        return False



async def submit_search_form(page: Page, make: str, model: str) -> bool:
    """Submit the search form with make and model - SMART MATCHING VERSION."""
    try:
        # Get all dropdown options from the page
        all_options = await page.evaluate('''() => {
            const options = [];
            const makeSelect = document.querySelector('select[name="mrk"]');
            if (makeSelect) {
                Array.from(makeSelect.options).forEach(opt => {
                    if (opt.value && opt.value !== '-1' && opt.value !== '') {
                        options.push({text: opt.text.trim(), value: opt.value});
                    }
                });
            }
            return options;
        }''')
        
        # Find the matching make option using partial matching
        make_value = None
        for option in all_options:
            if make.upper() in option['text'].upper():
                make_value = option['value']
                break
        
        if not make_value:
            print(f"    ❌ Make '{make}' not found")
            return False
        
        # Select make
        await page.select_option('select[name="mrk"]', make_value)
        print(f"    ✅ Make: {make}")
        
        # Wait for model dropdown to populate
        await page.wait_for_selector('select[name="mdl"]:not([disabled])', timeout=10000)
        
        # Additional wait to ensure form is fully loaded
        await asyncio.sleep(1)
        
        # Wait for model options to actually load
        await page.wait_for_function('''() => {
            const modelSelect = document.querySelector('select[name="mdl"]');
            if (!modelSelect) return false;
            const options = Array.from(modelSelect.options);
            return options.length > 1 && options.some(opt => opt.value !== '-1' && opt.value !== '');
        }''', timeout=10000)
        
        await asyncio.sleep(0.5)  # Brief additional wait
        
        # Get model options
        model_options = await page.evaluate('''() => {
            const options = [];
            const modelSelect = document.querySelector('select[name="mdl"]');
            if (modelSelect) {
                Array.from(modelSelect.options).forEach(opt => {
                    if (opt.value && opt.value !== '-1' && opt.value !== '') {
                        options.push({text: opt.text.trim(), value: opt.value});
                    }
                });
            }
            return options;
        }''')
        
        # Use smart model matching logic
        model_value, matched_text, match_info = find_best_model_match_smart(model, model_options)
        
        if not model_value:
            # Debug: Show available models for this make
            available_models = [opt['text'] for opt in model_options]
            print(f"    ❌ Model '{model}' not found. Available models for {make}: {available_models}")
            return False
        
        # Select model
        await page.select_option('select[name="mdl"]', model_value)
        
        # Enhanced logging to show what was matched
        if matched_text.upper() != model.upper():
            print(f"    ✅ Model: {model} → matched: '{matched_text}' ({match_info})")
        else:
            print(f"    ✅ Model: {model} ({match_info})")
        
        # Set Result to 'Sold' (optional - don't fail if not available)
        try:
            result_select = await page.query_selector('select[name="result"]')
            if result_select:
                # Check if the element is visible and enabled
                is_visible = await result_select.is_visible()
                if is_visible:
                    await page.select_option('select[name="result"]', '1')
                    print(f"    ✅ Set Result to 'Sold'")
                else:
                    print(f"    ⚠️ Result dropdown not visible, skipping...")
        except Exception as e:
            print(f"    ⚠️ Could not set Result dropdown: {e}")
        
        # Set year range
        min_year = calculate_min_year_for_vehicle(make, model)
        current_year = date.today().year
        await page.fill('input[name="year1"]', str(min_year))
        await page.fill('input[name="year2"]', str(current_year))
        
        # Set score filter
        await set_score_filter(page)
        
        # Click search button
        search_button = await page.query_selector('#btnSearch1, #btnSearch2, #btnSearsh, input[value="Search"]')
        if search_button:
            await search_button.click()
        else:
            await page.click('input[type="button"][value="Search"]')
        
        # Wait for results to load
        await page.wait_for_load_state('networkidle')
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error submitting search form: {e}")
        return False

async def change_model_and_search(page: Page, model: str) -> bool:
    """Change model in results page search form and search - SMART MATCHING VERSION."""
    try:
        # Get model options from results page
        model_options = await page.evaluate('''() => {
            const options = [];
            const modelSelect = document.querySelector('select[name="mdl"]');
            if (modelSelect) {
                Array.from(modelSelect.options).forEach(opt => {
                    if (opt.value && opt.value !== '-1' && opt.value !== '') {
                        options.push({text: opt.text.trim(), value: opt.value});
                    }
                });
            }
            return options;
        }''')
        
        # Use smart model matching logic
        model_value, matched_text, match_info = find_best_model_match_smart(model, model_options)
        
        if not model_value:
            # Debug: Show available models (only for key models)
            if model in ["ACCORD", "CIVIC", "CR-V"]:
                available_models = [opt['text'] for opt in model_options]
                print(f"    ❌ Model '{model}' not found. Available models: {available_models}")
            else:
                print(f"    ❌ Model: {model}")
            return False
        
        # Select model
        await page.select_option('select[name="mdl"]', model_value)
        
        # Enhanced logging
        if matched_text.upper() != model.upper():
            print(f"    ✅ Model: {model} → matched: '{matched_text}' ({match_info})")
        else:
            print(f"    ✅ Model: {model} ({match_info})")
        
        # Click search button on results page
        search_button = await page.query_selector('#btnSearch2')
        if search_button:
            await search_button.click()
        else:
            await page.click('input[type="button"][value="Search"]')
        
        # Wait for results to load
        await page.wait_for_load_state('networkidle')
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error changing model and searching: {e}")
        return False

async def extract_vehicle_count(page: Page) -> Optional[int]:
    """Extract vehicle count from results page."""
    try:
        # Wait for results table to load
        await page.wait_for_selector('#mainTable', timeout=10000)
        
        # Look for the red font element with the count
        count_element = await page.query_selector('font[color="red"]')
        if count_element:
            count_text = await count_element.text_content()
            if count_text and count_text.isdigit():
                count = int(count_text)
                return count
        
        # Fallback: try to get from header text
        total_results_text = await page.text_content('.Header2')
        if total_results_text:
            # Extract number from "Found total lots: <b><font color="red">8</font></b>"
            match = re.search(r'Found total lots:\s*<b><font[^>]*>(\d+)</font></b>', total_results_text)
            if match:
                count = int(match.group(1))
                return count
        
        return 0
        
    except Exception as e:
        print(f"    ❌ Error extracting vehicle count: {e}")
        return None

async def is_results_page(page: Page) -> bool:
    """Check if we're on the results page."""
    try:
        results_table = await page.query_selector('#mainTable')
        return results_table is not None
    except:
        return False

async def check_model_inventory(page: Page, make: str, model: str, sales_data_url: str) -> Optional[int]:
    """Check inventory for a specific make/model using SMART MATCHING."""
    try:
        # Check if we're on the results page or need to submit initial search
        is_results = await page.query_selector('select[name="mdl"]')
        
        if is_results:
            # We're on results page, but we need to check if the make is correct
            # Get current make selection
            current_make = await page.evaluate('''() => {
                const makeSelect = document.querySelector('select[name="mrk"]');
                if (makeSelect) {
                    const selectedOption = makeSelect.options[makeSelect.selectedIndex];
                    return selectedOption ? selectedOption.text.trim() : null;
                }
                return null;
            }''')
            
            # If make is different, we need to submit a new search form
            if not current_make or make.upper() not in current_make.upper():
                print(f"    🔄 Make changed from '{current_make}' to '{make}', submitting new search...")
                success = await submit_search_form(page, make, model)
            else:
                # Same make, just change model
                success = await change_model_and_search(page, model)
        else:
            # We need to submit the initial search form
            success = await submit_search_form(page, make, model)
        
        if not success:
            return None
        
        # Extract vehicle count from results
        count = await extract_vehicle_count(page)
        return count
        
    except Exception as e:
        print(f"    ❌ Error checking inventory for {make} {model}: {e}")
        return None

def display_inventory_table(results: List[Dict], show_all: bool = False):
    """Display inventory results in a formatted table."""
    
    # Filter results based on show_all parameter
    if show_all:
        display_results = results
    else:
        # Show only models with available vehicles
        display_results = [r for r in results if r['count'] > 0]
    
    if not display_results:
        print("  📊 No results to display")
        return
    
    # Prepare table data
    table_data = []
    for result in display_results:
        table_data.append([
            result['make'],
            result['model'],
            result['count']
        ])
    
    # Sort by count (descending)
    table_data.sort(key=lambda x: x[2], reverse=True)
    
    # Add header
    headers = ["Make", "Model", "Available Vehicles"]
    
    # Display table
    print(f"\n  📊 Inventory Results ({len(display_results)} models):")
    print(format_table(table_data, headers))
    
    # Summary statistics
    total_vehicles = sum(r['count'] for r in display_results)
    avg_vehicles = total_vehicles / len(display_results) if display_results else 0
    
    print(f"\n  📈 Summary:")
    print(f"    Total vehicles: {total_vehicles:,}")
    print(f"    Average per model: {avg_vehicles:.1f}")
    print(f"    Models with vehicles: {len(display_results)}")

async def launch_inventory_check():
    """Main function to check inventory for AutoPacific."""
    print("🚀 Starting AutoPacific Inventory Check...")
    print(f"Available makes: {list(manufacturer_configs.keys())}")
    
    # Create AutoPacific batch
    combinations = create_autopacific_batch()
    
    print(f"Total combinations to check: {len(combinations)}")
    print("=" * 60)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        
        async def process_autopacific():
            print(f"Processing site: AutoPacific")
            
            # Get sales data URL from auction_site_config
            if "AutoPacific" not in auction_sites:
                print(f"  ❌ AutoPacific not found in auction_sites config")
                return
            
            site_config = auction_sites["AutoPacific"]
            sales_data_url = site_config['scraping']['sales_data_url']
            
            print(f"  📍 Sales data URL: {sales_data_url}")
            
            # Create browser context and page
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Navigate to sales data URL
                print(f"  🚀 Navigating to sales data URL...")
                await page.goto(sales_data_url, wait_until='networkidle', timeout=30000)
                
                # Check if we need to login (look for login form)
                login_form = await page.query_selector('form')
                if login_form:
                    print(f"  🔐 Login form detected, attempting to login...")
                    
                    # Get login credentials from config
                    username = site_config['username']
                    password = site_config['password']
                    
                    # Fill login form
                    try:
                        await page.fill('#usr_name', username)
                        await page.fill('#usr_pwd', password)
                        
                        # Submit login form
                        await page.click('input[name="Submit"][value="Sign in"]')
                        
                        # Wait for redirect after login
                        await page.wait_for_load_state('networkidle')
                        
                        print(f"  ✅ Login successful")
                        
                    except Exception as e:
                        print(f"  ❌ Login failed: {e}")
                        return
                else:
                    print(f"  ℹ️ No login form found, proceeding...")
                
                # Get page title after login (or if no login was needed)
                title = await page.title()
                print(f"  ✅ Page loaded: {title}")
                print(f"  📊 Found {len(combinations)} combinations to check")
                
                # Process each make/model combination
                results = []
                total_vehicles = 0
                models_with_vehicles = 0
                
                print(f"  🔍 Checking {len(combinations)} make/model combinations...")
                print(f"  📊 Real-time counter:")
                print(f"  {'='*50}")
                
                for i, combination in enumerate(combinations, 1):
                    make = combination['make']
                    model = combination['model']
                    
                    # Check inventory count
                    count = await check_model_inventory(page, make, model, sales_data_url)
                    count = count if count is not None else 0
                    
                    results.append({
                        'make': make,
                        'model': model,
                        'count': count
                    })
                    
                    # Update counters
                    if count > 0:
                        total_vehicles += count
                        models_with_vehicles += 1
                    
                    # Display real-time result
                    if count > 0:
                        print(f"  ✅ {make} {model}: {count:,} vehicles")
                    else:
                        print(f"  ❌ {make} {model}: 0 vehicles")
                    
                    # Show summary every 50 items
                    if i % 50 == 0 or i == len(combinations):
                        print(f"  {'-'*50}")
                        print(f"  📈 Progress: {i}/{len(combinations)} | Total: {total_vehicles:,} vehicles | Models: {models_with_vehicles}")
                        print(f"  {'='*50}")
                    
                    # Add a small delay between searches to be respectful
                    await asyncio.sleep(1)
                
                # Final summary
                print(f"\n{'='*60}")
                print("📊 FINAL SUMMARY")
                print(f"{'='*60}")
                print(f"  Total combinations checked: {len(combinations)}")
                print(f"  Models with vehicles: {models_with_vehicles}")
                print(f"  Total vehicles found: {total_vehicles:,}")
                print(f"  Average vehicles per model: {total_vehicles/models_with_vehicles:.1f}" if models_with_vehicles > 0 else "  Average vehicles per model: 0")
                
                # Display detailed results table
                print(f"\n{'='*60}")
                print("📊 DETAILED RESULTS TABLE")
                print(f"{'='*60}")
                
                # Show only models with available vehicles
                display_inventory_table(results, show_all=False)
                
                # Show top 20 models by vehicle count
                if results:
                    print(f"\n🏆 Top 20 Models by Vehicle Count:")
                    sorted_results = sorted(results, key=lambda x: x['count'], reverse=True)
                    for i, result in enumerate(sorted_results[:20], 1):
                        if result['count'] > 0:
                            print(f"  {i:2d}. {result['make']} {result['model']}: {result['count']:,} vehicles")
                
                # Show breakdown by make
                print(f"\n{'='*60}")
                print("📊 BREAKDOWN BY MAKE")
                print(f"{'='*60}")
                
                make_totals = {}
                for result in results:
                    if result['count'] > 0:
                        make = result['make']
                        if make not in make_totals:
                            make_totals[make] = {'total': 0, 'models': 0}
                        make_totals[make]['total'] += result['count']
                        make_totals[make]['models'] += 1
                
                if make_totals:
                    # Sort makes by total vehicles
                    sorted_makes = sorted(make_totals.items(), key=lambda x: x[1]['total'], reverse=True)
                    
                    print(f"{'Make':<15} {'Total Vehicles':<15} {'Models':<10} {'Avg per Model':<15}")
                    print("-" * 60)
                    for make, data in sorted_makes:
                        avg = data['total'] / data['models']
                        print(f"{make:<15} {data['total']:<15,} {data['models']:<10} {avg:<15.1f}")
                
                # Save results to file
                import json
                from datetime import datetime
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"inventory_results_{timestamp}.json"
                
                # Save detailed results
                with open(filename, 'w') as f:
                    json.dump({
                        'timestamp': datetime.now().isoformat(),
                        'total_combinations': len(combinations),
                        'models_with_vehicles': models_with_vehicles,
                        'total_vehicles': total_vehicles,
                        'average_per_model': total_vehicles/models_with_vehicles if models_with_vehicles > 0 else 0,
                        'results': results,
                        'make_breakdown': make_totals
                    }, f, indent=2)
                
                print(f"\n💾 Results saved to: {filename}")
                
            except Exception as e:
                print(f"  ❌ Error processing AutoPacific: {e}")
            finally:
                await context.close()
        
        # Process AutoPacific
        await process_autopacific()
        
        print(f"{'='*60}")
        print("AUTOPACIFIC INVENTORY CHECK COMPLETED")
        print(f"{'='*60}")
        
        await browser.close()
        await playwright.stop()

if __name__ == "__main__":
    asyncio.run(launch_inventory_check()) 