#!/usr/bin/env python3
"""
Test script for inventory checker - processes first 5 models for each make
"""

import asyncio
import sys
import os
import re
from typing import List, Dict, Tuple, Optional
from datetime import date

# Add the project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.extraction.get_inventory import check_model_inventory, calculate_min_year_for_vehicle
from config.auction_site_config import auction_sites
from config.manufacturer_config import manufacturer_configs
from playwright.async_api import async_playwright, Page

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

async def submit_search_form_smart(page: Page, make: str, model: str) -> bool:
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
        from src.extraction.get_inventory import set_score_filter
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

async def change_model_and_search_smart(page: Page, model: str) -> bool:
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

async def check_model_inventory_smart(page: Page, make: str, model: str, sales_data_url: str) -> Optional[int]:
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
                success = await submit_search_form_smart(page, make, model)
            else:
                # Same make, just change model
                success = await change_model_and_search_smart(page, model)
        else:
            # We need to submit the initial search form
            success = await submit_search_form_smart(page, make, model)
        
        if not success:
            return None
        
        # Extract vehicle count from results
        from src.extraction.get_inventory import extract_vehicle_count
        count = await extract_vehicle_count(page)
        return count
        
    except Exception as e:
        print(f"    ❌ Error checking inventory for {make} {model}: {e}")
        return None

def test_model_matching():
    """Test the model matching logic with sample data."""
    
    # Sample model options that might appear in the dropdown
    test_options = [
        {'value': 'accord_phev', 'text': 'ACCORD PHEV (1)'},
        {'value': 'accord_base', 'text': 'ACCORD (267556)'},
        {'value': 'accord_hybrid', 'text': 'ACCORD HYBRID (45)'},
        {'value': 'accord_sport', 'text': 'ACCORD SPORT (123)'},
    ]
    
    # Test cases
    test_cases = [
        'ACCORD',
        'ACCORD PHEV', 
        'ACCORD HYBRID',
        'CIVIC',
    ]
    
    print("🧪 Testing Model Matching Logic")
    print("=" * 50)
    
    for search_term in test_cases:
        print(f"\nSearching for: '{search_term}'")
        model_value, matched_text, match_info = find_best_model_match_smart(search_term, test_options)
        
        if model_value:
            print(f"  ✅ Best match: '{matched_text}' ({match_info})")
            
            # Show all scored options for this search
            print(f"  📊 All matches:")
            for option in test_options:
                score, reason = calculate_model_match_score(search_term, option['text'])
                if score > 0:
                    print(f"    - {option['text']}: {score} ({reason})")
        else:
            print(f"  ❌ No match found")
    
    print("\n" + "=" * 50)

def create_test_batch() -> list:
    """Create a test batch with first 5 models for Toyota, Nissan, Honda, and Suzuki."""
    test_combinations = []
    
    # Define the makes to test
    target_makes = ['TOYOTA', 'NISSAN', 'HONDA', 'SUZUKI']
    
    for make in target_makes:
        if make in manufacturer_configs:
            # Get first 5 models for this make
            model_list = list(manufacturer_configs[make].keys())[:5]
            
            for model in model_list:
                test_combinations.append({
                    'make': make,
                    'model': model
                })
        else:
            print(f"⚠️ Warning: {make} not found in manufacturer_configs")
    
    return test_combinations

async def test_inventory_check():
    """Test the inventory checker with a small subset."""
    
    print("🧪 Testing Inventory Checker - First 5 Models for Toyota, Nissan, Honda, Suzuki")
    print("=" * 60)
    
    # Create test batch
    test_combinations = create_test_batch()
    
    print(f"📊 Test Configuration:")
    print(f"  Makes to test: Toyota, Nissan, Honda, Suzuki")
    print(f"  Models per make: 5")
    print(f"  Total combinations: {len(test_combinations)}")
    print("=" * 60)
    
    # Show what we're testing
    print("🔍 Test combinations:")
    for i, combo in enumerate(test_combinations, 1):
        print(f"  {i:3d}. {combo['make']} {combo['model']}")
    print("=" * 60)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        
        try:
            # Get AutoPacific configuration
            if "AutoPacific" not in auction_sites:
                print("❌ AutoPacific not found in auction_sites config")
                return
            
            site_config = auction_sites["AutoPacific"]
            sales_data_url = site_config['scraping']['sales_data_url']
            
            print(f"🌐 Connecting to AutoPacific...")
            print(f"  📍 URL: {sales_data_url}")
            
            # Create browser context and page
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Navigate to sales data URL
                await page.goto(sales_data_url, wait_until='networkidle', timeout=30000)
                
                # Check if we need to login
                login_form = await page.query_selector('form')
                if login_form:
                    print(f"🔐 Login form detected, attempting to login...")
                    
                    username = site_config['username']
                    password = site_config['password']
                    
                    try:
                        await page.fill('#usr_name', username)
                        await page.fill('#usr_pwd', password)
                        await page.click('input[name="Submit"][value="Sign in"]')
                        await page.wait_for_load_state('networkidle')
                        print(f"✅ Login successful")
                    except Exception as e:
                        print(f"❌ Login failed: {e}")
                        return
                else:
                    print(f"ℹ️ No login form found, proceeding...")
                
                # Process test combinations
                results = []
                total_vehicles = 0
                models_with_vehicles = 0
                
                print(f"\n🔍 Testing {len(test_combinations)} combinations with SMART MATCHING...")
                print(f"📊 Real-time results:")
                print(f"{'='*50}")
                
                for i, combination in enumerate(test_combinations, 1):
                    make = combination['make']
                    model = combination['model']
                    
                    # Check inventory count using SMART MATCHING
                    count = await check_model_inventory_smart(page, make, model, sales_data_url)
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
                        print(f"✅ {make} {model}: {count:,} vehicles")
                    else:
                        print(f"❌ {make} {model}: 0 vehicles")
                    
                    # Show progress every 10 items
                    if i % 10 == 0 or i == len(test_combinations):
                        print(f"{'-'*50}")
                        print(f"📈 Progress: {i}/{len(test_combinations)} | Total: {total_vehicles:,} vehicles | Models: {models_with_vehicles}")
                        print(f"{'='*50}")
                    
                    # Small delay between searches
                    await asyncio.sleep(1)
                
                # Final summary
                print(f"\n{'='*60}")
                print("📊 TEST SUMMARY")
                print(f"{'='*60}")
                print(f"  Total combinations tested: {len(test_combinations)}")
                print(f"  Models with vehicles: {models_with_vehicles}")
                print(f"  Total vehicles found: {total_vehicles:,}")
                if models_with_vehicles > 0:
                    print(f"  Average vehicles per model: {total_vehicles/models_with_vehicles:.1f}")
                else:
                    print(f"  Average vehicles per model: 0")
                
                # Show top 10 models by vehicle count
                if results:
                    print(f"\n🏆 Top 10 Models by Vehicle Count:")
                    sorted_results = sorted(results, key=lambda x: x['count'], reverse=True)
                    for i, result in enumerate(sorted_results[:10], 1):
                        if result['count'] > 0:
                            print(f"  {i:2d}. {result['make']} {result['model']}: {result['count']:,} vehicles")
                
                # Display final table
                print(f"\n{'='*60}")
                print("📊 FINAL RESULTS TABLE")
                print(f"{'='*60}")
                
                # Import the table display function
                from src.extraction.get_inventory import display_inventory_table
                
                # Show only models with available vehicles
                display_inventory_table(results, show_all=False)
                
                # Show all results (including zero counts)
                print(f"\n{'='*60}")
                print("📋 ALL RESULTS (including zero counts)")
                print(f"{'='*60}")
                display_inventory_table(results, show_all=True)
                
            except Exception as e:
                print(f"❌ Error during testing: {e}")
                import traceback
                traceback.print_exc()
            finally:
                await context.close()
        
        finally:
            await browser.close()
            await playwright.stop()
        
        print(f"\n{'='*60}")
        print("✅ TEST COMPLETED")
        print(f"{'='*60}")

if __name__ == "__main__":
    # Uncomment to test the matching logic first
    # test_model_matching()
    
    asyncio.run(test_inventory_check()) 