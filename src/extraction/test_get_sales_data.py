#!/usr/bin/env python3
"""
Test script for get_sales_data extraction: Toyota Aqua only
"""
import asyncio
import sys
import os

# Add the project root to the path so we can import from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.core.config import auction_sites, manufacturer_configs
from src.extraction.get_sales_data import process_site_session

async def test_extract_toyota_aqua():
    # Get Toyota Aqua from manufacturers config
    toyota_config = manufacturer_configs.get("TOYOTA", {})
    
    # Find Aqua in the config (it's stored as a key, not in a models list)
    aqua_model = None
    for model_name in toyota_config.keys():
        if "AQUA" in model_name.upper():
            aqua_model = model_name
            break
    
    if not aqua_model:
        print("❌ Toyota Aqua not found in manufacturers config")
        print("Available Toyota models:")
        for model_name in toyota_config.keys():
            print(f"  - {model_name}")
        return
    
    site_name = list(auction_sites.keys())[0]  # Test with the first site
    site_config = auction_sites[site_name]
    make = "TOYOTA"
    model = aqua_model
    description = f"{make} {model}"
    print(f"Testing extraction for {description} on {site_name}")
    print(f"Found in config: {aqua_model}")
    
    # Create a workload with just Toyota Aqua
    workload = [(make, model, description)]
    
    # Use the existing process_site_session function which handles everything
    # including database saving
    print(f"Starting extraction process...")
    try:
        saved_count, failed_count = await process_site_session(
            site_name=site_name,
            site_config=site_config,
            workload_chunk=workload,
            session_name="Test-Session",
            headless=False  # Show browser to verify page navigation
        )
        
        print(f"\n--- FINAL RESULTS ---")
        print(f"✅ Successfully saved: {saved_count} records")
        print(f"❌ Failed to save: {failed_count} records")
        print(f"Total processed: {saved_count + failed_count}")
        
        # Additional verification
        if saved_count > 0:
            print(f"🎉 Test PASSED: {saved_count} records saved successfully!")
        else:
            print(f"⚠️ Test WARNING: No records were saved")
            
    except Exception as e:
        print(f"❌ Test FAILED with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_extract_toyota_aqua()) 