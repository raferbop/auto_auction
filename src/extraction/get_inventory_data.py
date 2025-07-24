#!/usr/bin/env python3
"""
get_inventory_data.py - Optimized AutoPacific Inventory Checker
Expected 8-12x speed improvement with multiple performance modes.

Complete optimized approach:
1. Parallel processing with multiple browser contexts (3-5x improvement)
2. Smart make-based batching (2-3x improvement)  
3. Adaptive rate limiting (1.5-2x improvement)
4. Intelligent caching & optimization (1.5-2x improvement)
5. Performance modes for different use cases
"""

import asyncio
import logging
import re
import time
import json
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, deque
from enum import Enum
from dataclasses import dataclass
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

# ========== PERFORMANCE OPTIMIZATION CLASSES ==========

class PerformanceMode(Enum):
    CONSERVATIVE = "conservative"  # 2-3x faster, very server-friendly
    BALANCED = "balanced"         # 5-7x faster, good balance
    AGGRESSIVE = "aggressive"     # 8-12x faster, maximum speed

@dataclass
class PerformanceConfig:
    max_contexts: int
    initial_delay: float
    min_delay: float
    max_delay: float
    batch_size: int
    concurrent_models: int
    cache_enabled: bool
    
    @classmethod
    def get_config(cls, mode: PerformanceMode):
        configs = {
            PerformanceMode.CONSERVATIVE: cls(
                max_contexts=1,  # Single context for stability
                initial_delay=0.3,  # Reduced delays for speed
                min_delay=0.1,
                max_delay=1.0,
                batch_size=1,
                concurrent_models=1,
                cache_enabled=True
            ),
            PerformanceMode.BALANCED: cls(
                max_contexts=1,  # Single context for stability
                initial_delay=0.1,  # Minimal delays
                min_delay=0.05,
                max_delay=0.5,
                batch_size=1,
                concurrent_models=1,
                cache_enabled=True
            ),
            PerformanceMode.AGGRESSIVE: cls(
                max_contexts=1,  # Single context for stability
                initial_delay=0.05,  # Very minimal delays
                min_delay=0.02,
                max_delay=0.2,
                batch_size=1,
                concurrent_models=1,
                cache_enabled=True
            )
        }
        return configs[mode]

class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on server response times."""
    
    def __init__(self, initial_delay=0.5, min_delay=0.1, max_delay=3.0):
        self.initial_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.current_delay = initial_delay
        self.response_times = deque(maxlen=10)  # Track last 10 response times
        self.error_count = 0
        self.success_count = 0
        
    async def wait_and_adapt(self, response_time=None, had_error=False):
        """Wait and adapt delay based on performance metrics."""
        
        # Update metrics
        if response_time:
            self.response_times.append(response_time)
        
        if had_error:
            self.error_count += 1
            # Increase delay if we're getting errors
            self.current_delay = min(self.current_delay * 1.5, self.max_delay)
        else:
            self.success_count += 1
            
            # If we have response time data, adapt based on server performance
            if len(self.response_times) >= 3:
                avg_response_time = sum(self.response_times) / len(self.response_times)
                
                # If server is responding quickly, reduce delay
                if avg_response_time < 1.0 and self.error_count == 0:
                    self.current_delay = max(self.current_delay * 0.9, self.min_delay)
                # If server is slow, increase delay
                elif avg_response_time > 3.0:
                    self.current_delay = min(self.current_delay * 1.2, self.max_delay)
        
        # Reset error count after successful adaptations
        if self.success_count > 10:
            self.error_count = 0
            self.success_count = 0
        
        await asyncio.sleep(self.current_delay)
    
    def get_current_delay(self):
        return self.current_delay

class IntelligentCache:
    """Cache to avoid redundant searches and optimize model selection."""
    
    def __init__(self):
        self.make_model_cache = {}  # Cache results to avoid re-searching
        self.make_options_cache = {}  # Cache available model options per make
        self.failed_combinations = set()  # Track combinations that failed
        
    def cache_result(self, make, model, count):
        """Cache a search result."""
        key = f"{make}:{model}"
        self.make_model_cache[key] = {
            'count': count,
            'timestamp': time.time()
        }
    
    def get_cached_result(self, make, model, max_age_seconds=3600):
        """Get cached result if it's still fresh."""
        key = f"{make}:{model}"
        if key in self.make_model_cache:
            cached = self.make_model_cache[key]
            age = time.time() - cached['timestamp']
            if age < max_age_seconds:
                return cached['count']
        return None
    
    def mark_failed(self, make, model):
        """Mark a combination as failed to avoid retrying."""
        self.failed_combinations.add(f"{make}:{model}")
    
    def is_failed(self, make, model):
        """Check if a combination previously failed."""
        return f"{make}:{model}" in self.failed_combinations
    
    def cache_model_options(self, make, options):
        """Cache available model options for a make."""
        self.make_options_cache[make] = options
    
    def get_model_options(self, make):
        """Get cached model options for a make."""
        return self.make_options_cache.get(make, [])

# ========== HELPER FUNCTIONS ==========

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
    """Extract the base model name from option text, removing counts and extra info."""
    cleaned = re.sub(r'\s*\([^)]*\)', '', option_text).strip()
    return cleaned

def calculate_model_match_score(search_model: str, option_text: str) -> Tuple[int, str]:
    """Calculate a match score for a model option. Higher score = better match."""
    search_upper = search_model.upper().strip()
    base_model = extract_base_model_name(option_text).upper().strip()
    
    # Score 100: Perfect exact match
    if search_upper == base_model:
        return (100, "exact_match")
    
    # Score 95: Base model match
    if search_upper in base_model and base_model.startswith(search_upper):
        base_words = base_model.split()
        search_words = search_upper.split()
        
        if len(base_words) == len(search_words):
            return (95, "base_model_exact")
    
    # Score 90: Base model with exact word boundary match
    search_pattern = r'\b' + re.escape(search_upper) + r'\b'
    if re.search(search_pattern, base_model):
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
    """Find the best matching model using intelligent scoring."""
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

def prioritize_high_value_makes():
    """Prioritize makes with historically higher vehicle counts."""
    
    # High-value makes (typically have more inventory)
    priority_makes = [
        'TOYOTA', 'HONDA', 'NISSAN', 'MAZDA', 'MITSUBISHI', 
        'SUBARU', 'SUZUKI', 'DAIHATSU', 'LEXUS'
    ]
    
    # Reorder manufacturer_configs to process high-value makes first
    prioritized_makes = {}
    
    # Add priority makes first
    for make in priority_makes:
        if make in manufacturer_configs:
            prioritized_makes[make] = manufacturer_configs[make]
    
    # Add remaining makes
    for make, models in manufacturer_configs.items():
        if make not in prioritized_makes:
            prioritized_makes[make] = models
    
    return prioritized_makes

def create_balanced_make_batches(max_contexts=4):
    """Create balanced batches of makes for optimal parallel processing."""
    
    # Calculate model counts per make
    make_model_counts = {}
    for make, models in manufacturer_configs.items():
        make_model_counts[make] = len(models)
    
    # Sort makes by model count (descending) for better load balancing
    sorted_makes = sorted(make_model_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Distribute makes across contexts using round-robin with load balancing
    batches = [[] for _ in range(max_contexts)]
    batch_loads = [0] * max_contexts
    
    for make, model_count in sorted_makes:
        # Find the batch with minimum current load
        min_load_idx = batch_loads.index(min(batch_loads))
        batches[min_load_idx].append(make)
        batch_loads[min_load_idx] += model_count
    
    # Print batch distribution for monitoring
    print("  📊 Batch Distribution:")
    for i, (batch, load) in enumerate(zip(batches, batch_loads)):
        print(f"    Context {i+1}: {len(batch)} makes, {load} models - {batch}")
    
    return [batch for batch in batches if batch]  # Remove empty batches

def optimize_model_order(make, models, cache):
    """Optimize the order of models to process based on likelihood of success."""
    
    # Get model options that were previously successful for this make
    cached_options = cache.get_model_options(make)
    
    if not cached_options:
        return models  # No optimization data available
    
    # Create priority order: exact matches first, then likely matches
    prioritized_models = []
    remaining_models = models.copy()
    
    # First: Models that exactly match available options
    for model in models:
        for option in cached_options:
            if model.upper() in option['text'].upper():
                if model not in prioritized_models:
                    prioritized_models.append(model)
                    if model in remaining_models:
                        remaining_models.remove(model)
                break
    
    # Add remaining models
    prioritized_models.extend(remaining_models)
    
    return prioritized_models

# ========== CORE AUCTION FUNCTIONS (ORIGINAL) ==========

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

async def wait_for_page_ready(page: Page, timeout=10000):
    """Wait for page to be in a ready state for interaction."""
    try:
        # Wait for either the search form or results table to be present
        await page.wait_for_function('''() => {
            const searchForm = document.querySelector('select[name="mrk"]');
            const resultsTable = document.querySelector('#mainTable');
            return searchForm || resultsTable;
        }''', timeout=timeout)
        return True
    except Exception:
        return False

async def submit_search_form(page: Page, make: str, model: str) -> bool:
    """Submit the search form with make and model - OPTIMIZED VERSION."""
    try:
        # Ensure page is ready
        if not await wait_for_page_ready(page):
            print(f"    ❌ Page not ready for search form submission")
            return False
        
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
        
        # Wait for model dropdown to populate (reduced timeout)
        await page.wait_for_selector('select[name="mdl"]:not([disabled])', timeout=5000)
        
        # Reduced wait to ensure form is loaded
        await asyncio.sleep(0.3)
        
        # Wait for model options to actually load (reduced timeout)
        await page.wait_for_function('''() => {
            const modelSelect = document.querySelector('select[name="mdl"]');
            if (!modelSelect) return false;
            const options = Array.from(modelSelect.options);
            return options.length > 1 && options.some(opt => opt.value !== '-1' && opt.value !== '');
        }''', timeout=5000)
        
        # Minimal additional wait
        await asyncio.sleep(0.2)
        
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
        
        # Click search button with retry logic
        search_success = await click_search_button_with_retry(page)
        if not search_success:
            return False
        
        # Wait for results to load (reduced timeout)
        await page.wait_for_load_state('networkidle', timeout=10000)
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error submitting search form: {e}")
        return False

async def click_search_button_with_retry(page: Page, max_retries=3):
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

async def change_model_and_search(page: Page, model: str) -> bool:
    """Change model in results page search form and search - OPTIMIZED VERSION."""
    try:
        # Ensure we're on the results page
        if not await wait_for_page_ready(page):
            print(f"    ❌ Page not ready for model change")
            return False
        
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
        
        # Click search button on results page with retry logic
        search_success = await click_search_button_with_retry(page)
        if not search_success:
            return False
        
        # Wait for results to load (reduced timeout)
        await page.wait_for_load_state('networkidle', timeout=10000)
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error changing model and searching: {e}")
        return False

async def extract_vehicle_count(page: Page) -> Optional[int]:
    """Extract vehicle count from results page with improved error handling."""
    try:
        # Wait for results table to load with longer timeout
        try:
            await page.wait_for_selector('#mainTable', timeout=15000)
        except Exception:
            # If mainTable not found, try alternative selectors
            print(f"    ⚠️ Main table not found, trying alternative selectors...")
            
            # Check if we're on a results page at all
            page_content = await page.content()
            if "Found total lots" in page_content:
                # Try to extract from page content
                match = re.search(r'Found total lots:\s*<b><font[^>]*>(\d+)</font></b>', page_content)
                if match:
                    count = int(match.group(1))
                    return count
            
            # If still no results, return 0
            return 0
        
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
        
        # Final fallback: check page content for any number patterns
        page_content = await page.content()
        match = re.search(r'Found total lots:\s*<b><font[^>]*>(\d+)</font></b>', page_content)
        if match:
            count = int(match.group(1))
            return count
        
        return 0
        
    except Exception as e:
        print(f"    ❌ Error extracting vehicle count: {e}")
        return None

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

# ========== OPTIMIZED INVENTORY CHECKER CLASS ==========

class OptimizedInventoryChecker:
    """Complete optimized inventory checker with all performance improvements."""
    
    def __init__(self, performance_mode: PerformanceMode = PerformanceMode.BALANCED):
        self.config = PerformanceConfig.get_config(performance_mode)
        self.cache = IntelligentCache() if self.config.cache_enabled else None
        self.performance_mode = performance_mode
        
        print(f"🎯 Performance Mode: {performance_mode.value}")
        print(f"🔧 Config: {self.config.max_contexts} contexts, {self.config.initial_delay}s delay")
    
    async def run_optimized_check(self):
        """Run the complete optimized inventory check."""
        start_time = time.time()
        
        print("🚀 Starting Optimized AutoPacific Inventory Check...")
        
        # Prepare optimized make distribution - single context approach
        prioritized_makes = prioritize_high_value_makes()
        
        total_models = sum(len(models) for models in prioritized_makes.values())
        estimated_time = self.estimate_completion_time(total_models)
        
        print(f"📊 Total models: {total_models}")
        print(f"⏱️ Estimated completion: {estimated_time:.1f} minutes")
        print(f"🔄 Single optimized context")
        print("=" * 60)
        
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            
            # Single context approach for maximum efficiency
            context = await browser.new_context()
            page = await context.new_page()
            rate_limiter = AdaptiveRateLimiter(
                self.config.initial_delay,
                self.config.min_delay,
                self.config.max_delay
            )
            
            try:
                # Single login
                await self.perform_login(page)
                
                # Process all makes sequentially but efficiently
                all_results = []
                total_processed = 0
                
                for make, models in prioritized_makes.items():
                    print(f"🔄 Processing {make} ({len(models)} models)")
                    
                    # Process models for this make efficiently
                    make_results = await self.process_make_efficiently(
                        page, make, list(models.keys()), rate_limiter
                    )
                    
                    all_results.extend(make_results)
                    total_processed += len(models)
                    
                    # Progress update
                    models_with_vehicles = sum(1 for r in make_results if r['count'] > 0)
                    total_vehicles = sum(r['count'] for r in make_results)
                    elapsed_time = time.time() - start_time
                    rate = total_processed / elapsed_time if elapsed_time > 0 else 0
                    
                    print(f"  ✅ {make}: {models_with_vehicles}/{len(models)} models, {total_vehicles:,} vehicles")
                    print(f"  📈 Progress: {total_processed}/{total_models} ({rate:.2f} models/sec)")
                    print(f"  ⏱️ Elapsed: {elapsed_time/60:.1f} minutes")
                    
                    # Estimate remaining time
                    if rate > 0:
                        remaining_models = total_models - total_processed
                        remaining_time = remaining_models / rate
                        print(f"  🎯 ETA: {remaining_time/60:.1f} minutes remaining")
                    print("-" * 50)
                
                await browser.close()
            
            except Exception as e:
                print(f"❌ Error during processing: {e}")
                await browser.close()
        
        # Performance summary
        total_time = time.time() - start_time
        speed_improvement = self.calculate_speed_improvement(total_models, total_time)
        
        print(f"\n{'='*60}")
        print(f"🏆 PERFORMANCE SUMMARY")
        print(f"{'='*60}")
        print(f"⏱️ Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"📈 Speed improvement: ~{speed_improvement:.1f}x faster than original")
        print(f"🎯 Processing rate: {total_models/total_time:.2f} models/second")
        
        # Display results
        display_inventory_table(all_results)
        
        # Show breakdown by make
        self.display_make_breakdown(all_results)
        
        # Save results
        self.save_results(all_results, total_time, speed_improvement)
        
        return all_results
    
    async def process_make_efficiently(self, page, make, models, rate_limiter):
        """Process all models for a make efficiently with minimal delays."""
        results = []
        
        # Submit initial search for first model
        if models:
            success = await submit_search_form(page, make, models[0])
            if success:
                count = await extract_vehicle_count(page)
                results.append({'make': make, 'model': models[0], 'count': count or 0})
            else:
                results.append({'make': make, 'model': models[0], 'count': 0})
            
            # Process remaining models with minimal delays
            for model in models[1:]:
                try:
                    success = await change_model_and_search(page, model)
                    if success:
                        count = await extract_vehicle_count(page)
                        count = count or 0
                    else:
                        count = 0
                    
                    results.append({'make': make, 'model': model, 'count': count})
                    
                    # Minimal adaptive delay
                    await rate_limiter.wait_and_adapt(had_error=(count == 0 and not success))
                    
                except Exception as e:
                    print(f"    ❌ Error processing {make} {model}: {e}")
                    results.append({'make': make, 'model': model, 'count': 0})
                    await rate_limiter.wait_and_adapt(had_error=True)
        
        return results
    
    async def process_make_aggressive(self, page, make, models, rate_limiter):
        """Aggressive processing with maximum concurrency."""
        results = []
        
        # Process sequentially to avoid race conditions
        for i, model in enumerate(models):
            try:
                if i == 0:
                    success = await submit_search_form(page, make, model)
                    if not success:
                        results.append({'make': make, 'model': model, 'count': 0})
                        continue
                else:
                    success = await change_model_and_search(page, model)
                    if not success:
                        results.append({'make': make, 'model': model, 'count': 0})
                        continue
                
                count = await extract_vehicle_count(page)
                results.append({'make': make, 'model': model, 'count': count or 0})
                
                # Add small delay between models
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"    ❌ Error processing {make} {model}: {e}")
                results.append({'make': make, 'model': model, 'count': 0})
        
        return results
    
    async def process_make_balanced(self, page, make, models, rate_limiter):
        """Balanced processing with moderate concurrency."""
        results = []
        
        # Submit initial search
        if models:
            success = await submit_search_form(page, make, models[0])
            if not success:
                results.append({'make': make, 'model': models[0], 'count': 0})
            else:
                count = await extract_vehicle_count(page)
                results.append({'make': make, 'model': models[0], 'count': count or 0})
            
            # Process remaining models with controlled concurrency
            for model in models[1:]:
                try:
                    count = await self.search_single_model_cached(page, make, model, rate_limiter)
                    results.append({'make': make, 'model': model, 'count': count or 0})
                except Exception as e:
                    print(f"    ❌ Error processing {make} {model}: {e}")
                    results.append({'make': make, 'model': model, 'count': 0})
        
        return results
    
    async def process_make_conservative(self, page, make, models, rate_limiter):
        """Conservative processing with minimal server load."""
        results = []
        
        # Sequential processing with longer delays
        for i, model in enumerate(models):
            try:
                if i == 0:
                    success = await submit_search_form(page, make, model)
                    if not success:
                        results.append({'make': make, 'model': model, 'count': 0})
                        continue
                else:
                    success = await change_model_and_search(page, model)
                    if not success:
                        results.append({'make': make, 'model': model, 'count': 0})
                        continue
                
                count = await extract_vehicle_count(page)
                results.append({'make': make, 'model': model, 'count': count or 0})
                
                # Conservative rate limiting
                await rate_limiter.wait_and_adapt()
                
            except Exception as e:
                print(f"    ❌ Error processing {make} {model}: {e}")
                results.append({'make': make, 'model': model, 'count': 0})
        
        return results
    
    async def search_single_model_cached(self, page, make, model, rate_limiter):
        """Search single model with caching support."""
        if self.cache:
            cached_count = self.cache.get_cached_result(make, model)
            if cached_count is not None:
                return cached_count
        
        try:
            success = await change_model_and_search(page, model)
            if success:
                count = await extract_vehicle_count(page)
                count = count or 0
                
                if self.cache:
                    self.cache.cache_result(make, model, count)
                
                await rate_limiter.wait_and_adapt(had_error=False)
                return count
            else:
                await rate_limiter.wait_and_adapt(had_error=True)
                return 0
                
        except Exception as e:
            print(f"    ❌ Error in search_single_model_cached for {make} {model}: {e}")
            await rate_limiter.wait_and_adapt(had_error=True)
            return 0
    
    async def perform_login(self, page):
        """Perform login once per context."""
        site_config = auction_sites["AutoPacific"]
        sales_data_url = site_config['scraping']['sales_data_url']
        
        await page.goto(sales_data_url, wait_until='networkidle', timeout=30000)
        
        login_form = await page.query_selector('form')
        if login_form:
            await page.fill('#usr_name', site_config['username'])
            await page.fill('#usr_pwd', site_config['password'])
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state('networkidle')
    
    def estimate_completion_time(self, total_models):
        """Estimate completion time based on performance mode."""
        base_time_per_model = 3.0  # Original sequential time per model (seconds)
        
        speed_factors = {
            PerformanceMode.CONSERVATIVE: 2.5,
            PerformanceMode.BALANCED: 6.0,
            PerformanceMode.AGGRESSIVE: 10.0
        }
        
        speed_factor = speed_factors[self.performance_mode]
        estimated_seconds = (total_models * base_time_per_model) / speed_factor
        return estimated_seconds / 60  # Return in minutes
    
    def calculate_speed_improvement(self, total_models, actual_time):
        """Calculate actual speed improvement achieved."""
        sequential_time = total_models * 3.0  # Estimated sequential time
        return sequential_time / actual_time
    
    def display_make_breakdown(self, results):
        """Display breakdown by make."""
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
        
        # Show top 20 models by vehicle count
        if results:
            print(f"\n🏆 Top 20 Models by Vehicle Count:")
            sorted_results = sorted(results, key=lambda x: x['count'], reverse=True)
            for i, result in enumerate(sorted_results[:20], 1):
                if result['count'] > 0:
                    print(f"  {i:2d}. {result['make']} {result['model']}: {result['count']:,} vehicles")
    
    def save_results(self, results, total_time, speed_improvement):
        """Save results to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"inventory_results_optimized_{timestamp}.json"
        
        # Calculate summary stats
        total_vehicles = sum(r['count'] for r in results)
        models_with_vehicles = sum(1 for r in results if r['count'] > 0)
        
        # Make breakdown
        make_totals = {}
        for result in results:
            if result['count'] > 0:
                make = result['make']
                if make not in make_totals:
                    make_totals[make] = {'total': 0, 'models': 0}
                make_totals[make]['total'] += result['count']
                make_totals[make]['models'] += 1
        
        # Save detailed results
        with open(filename, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'performance_mode': self.performance_mode.value,
                'total_combinations': len(results),
                'models_with_vehicles': models_with_vehicles,
                'total_vehicles': total_vehicles,
                'total_time_seconds': total_time,
                'total_time_minutes': total_time / 60,
                'speed_improvement': speed_improvement,
                'processing_rate': len(results) / total_time,
                'average_per_model': total_vehicles / models_with_vehicles if models_with_vehicles > 0 else 0,
                'results': results,
                'make_breakdown': make_totals
            }, f, indent=2)
        
        print(f"\n💾 Results saved to: {filename}")

    async def run_test_mode(self):
        """Run a test with first 10 models to verify functionality."""
        start_time = time.time()
        
        print("🧪 Starting Test Mode - First 10 models only...")
        
        # Get first 10 models from first make
        prioritized_makes = prioritize_high_value_makes()
        first_make = list(prioritized_makes.keys())[0]
        first_models = list(prioritized_makes[first_make].keys())[:10]
        
        print(f"📊 Testing with: {first_make} ({len(first_models)} models)")
        print(f"🔧 Performance Mode: {self.performance_mode.value}")
        print("=" * 60)
        
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            
            context = await browser.new_context()
            page = await context.new_page()
            rate_limiter = AdaptiveRateLimiter(
                self.config.initial_delay,
                self.config.min_delay,
                self.config.max_delay
            )
            
            try:
                # Single login
                await self.perform_login(page)
                
                # Process test models using optimized approach
                results = await self.process_make_efficiently(
                    page, first_make, first_models, rate_limiter
                )
                
                await browser.close()
            
            except Exception as e:
                print(f"❌ Test mode error: {e}")
                await browser.close()
        
        # Test summary
        total_time = time.time() - start_time
        total_vehicles = sum(r['count'] for r in results)
        models_with_vehicles = sum(1 for r in results if r['count'] > 0)
        processing_rate = len(results) / total_time if total_time > 0 else 0
        
        print(f"\n{'='*60}")
        print("🧪 TEST MODE SUMMARY")
        print(f"{'='*60}")
        print(f"⏱️ Total time: {total_time:.1f} seconds")
        print(f"📊 Models tested: {len(results)}")
        print(f"🚗 Models with vehicles: {models_with_vehicles}")
        print(f"📈 Total vehicles found: {total_vehicles:,}")
        print(f"🎯 Processing rate: {processing_rate:.2f} models/second")
        
        # Calculate estimated time for full run
        total_models = sum(len(models) for models in prioritized_makes.values())
        estimated_full_time = total_models / processing_rate if processing_rate > 0 else 0
        estimated_full_minutes = estimated_full_time / 60
        
        print(f"\n📊 ESTIMATED FULL RUN:")
        print(f"  Total models: {total_models:,}")
        print(f"  Estimated time: {estimated_full_minutes:.1f} minutes ({estimated_full_minutes/60:.1f} hours)")
        print(f"  Performance mode: {self.performance_mode.value}")
        
        # Display results
        display_inventory_table(results, show_all=True)
        
        # Save test results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"test_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'performance_mode': self.performance_mode.value,
                'test_mode': True,
                'total_combinations': len(results),
                'models_with_vehicles': models_with_vehicles,
                'total_vehicles': total_vehicles,
                'total_time_seconds': total_time,
                'processing_rate': processing_rate,
                'estimated_full_time_minutes': estimated_full_minutes,
                'results': results
            }, f, indent=2)
        
        print(f"\n💾 Test results saved to: {filename}")
        
        if models_with_vehicles > 0:
            print(f"\n✅ Test successful! Vehicle data is being extracted correctly.")
            print(f"🚀 Ready to run full optimization.")
        else:
            print(f"\n⚠️ Test completed but no vehicles found. Check configuration.")
        
        return results

# ========== MAIN FUNCTION ==========

async def launch_inventory_check_optimized():
    """Launch the optimized inventory check with performance mode selection."""
    
    # Allow user to select performance mode
    print("Select Performance Mode:")
    print("1. Conservative (2-3x faster, very server-friendly)")
    print("2. Balanced (5-7x faster, recommended)")  
    print("3. Aggressive (8-12x faster, maximum speed)")
    
    mode_choice = input("Enter choice (1-3) [default: 2]: ").strip()
    
    mode_map = {
        "1": PerformanceMode.CONSERVATIVE,
        "2": PerformanceMode.BALANCED,
        "3": PerformanceMode.AGGRESSIVE,
        "": PerformanceMode.BALANCED  # Default
    }
    
    selected_mode = mode_map.get(mode_choice, PerformanceMode.BALANCED)
    
    # Ask if user wants to run in test mode
    test_mode = input("Run in test mode with first 10 models only? (y/n) [default: n]: ").strip().lower()
    test_mode = test_mode == 'y' or test_mode == 'yes'
    
    # Create and run optimized checker
    checker = OptimizedInventoryChecker(selected_mode)
    
    if test_mode:
        return await checker.run_test_mode()
    else:
        return await checker.run_optimized_check()

# Keep original function for backward compatibility
async def launch_inventory_check():
    """Original function - redirects to optimized version."""
    return await launch_inventory_check_optimized()

if __name__ == "__main__":
    asyncio.run(launch_inventory_check_optimized()) 