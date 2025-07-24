#!/usr/bin/env python3
"""
Test script for get_sales_data extraction: Toyota Prius only
"""
import asyncio
import sys
import os

# Add the project root to the path so we can import from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.core.config import auction_sites, manufacturer_configs
from playwright.async_api import async_playwright, Page
from src.extraction.get_sales_data import extract_sales_data_from_results, set_session_filters, get_dropdown_options, find_best_match, calculate_min_year_for_vehicle, fill_search_form_with_filters, login_to_site

async def test_extract_toyota_prius():
    # Get Toyota Prius from manufacturers config
    toyota_config = manufacturer_configs.get("TOYOTA", {})
    
    # Find Prius in the config (it's stored as a key, not in a models list)
    prius_model = None
    for model_name in toyota_config.keys():
        if "PRIUS" in model_name.upper():
            prius_model = model_name
            break
    
    if not prius_model:
        print("❌ Toyota Prius not found in manufacturers config")
        return
    
    site_name = list(auction_sites.keys())[0]  # Test with the first site
    site_config = auction_sites[site_name]
    make = "TOYOTA"
    model = prius_model
    description = f"{make} {model}"
    print(f"Testing extraction for {description} on {site_name}")
    print(f"Found in config: {prius_model}")
    
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=False)
    try:
        context = await browser.new_context()
        page = await context.new_page()
        login_success = await login_to_site(page, site_name, site_config)
        if not login_success:
            print(f"❌ Login failed for {site_name}")
            return
        await set_session_filters(page)
        form_success, sales_count, debug_msgs = await fill_search_form_with_filters(page, make, model)
        if not form_success:
            print(f"❌ Search form failed for {description}")
            for msg in debug_msgs:
                print(f"  ⚠️ {msg}")
            return
        print(f"✅ Search complete, {sales_count} records found for {description}")
        sales_data = await extract_sales_data_from_results(page, debug_msgs)
        # Print debug messages for pagination and extraction
        print("\n--- DEBUG MESSAGES ---")
        for msg in debug_msgs:
            print(msg)
        print("--- END DEBUG ---\n")
        print(f"Total vehicles extracted: {len(sales_data)}")
        await page.close()
        await context.close()
    finally:
        await browser.close()
        await playwright.stop()

if __name__ == "__main__":
    asyncio.run(test_extract_toyota_prius()) 