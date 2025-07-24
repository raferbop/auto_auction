#!/usr/bin/env python3
"""
Script to audit all makes and models from AutoPacific website and compare with manufacturer config.
This will identify:
1. Models in config but not on website
2. Models on website but not in config
3. Missing makes entirely
"""

import asyncio
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from playwright.async_api import async_playwright
from config.auction_site_config import auction_sites
from config.manufacturer_config import manufacturer_configs

async def audit_manufacturer_config():
    """Audit all makes and models from the website vs config."""
    print("🔍 Starting comprehensive manufacturer config audit...")
    
    # Get AutoPacific config
    if "AutoPacific" not in auction_sites:
        print("❌ AutoPacific not found in auction_sites config")
        return
    
    site_config = auction_sites["AutoPacific"]
    sales_data_url = site_config['scraping']['sales_data_url']
    
    print(f"📍 Sales data URL: {sales_data_url}")
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Navigate to sales data URL
            print("🚀 Navigating to sales data URL...")
            await page.goto(sales_data_url, wait_until='networkidle', timeout=30000)
            
            # Check if we need to login
            login_form = await page.query_selector('form')
            if login_form:
                print("🔐 Login form detected, attempting to login...")
                
                username = site_config['username']
                password = site_config['password']
                
                await page.fill('input[name="username"]', username)
                await page.fill('input[name="password"]', password)
                await page.click('input[type="submit"]')
                await page.wait_for_load_state('networkidle')
                print("✅ Login completed")
            
            await asyncio.sleep(2)
            
            # Get all make options
            print("📋 Getting all available makes...")
            make_options = await page.evaluate('''() => {
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
            
            print(f"✅ Found {len(make_options)} makes on website")
            
            # Get config makes
            config_makes = set(manufacturer_configs.keys())
            # For website makes, we'll use partial matching instead of exact matching
            # So we don't need to clean them - we'll match against the full text
            website_makes = {opt['text'] for opt in make_options}
            
            # Debug: Show some examples
            print(f"\n🔍 Debug - Make name comparison:")
            print(f"Config makes (first 5): {sorted(list(config_makes))[:5]}")
            print(f"Website makes (first 5): {sorted(list(website_makes))[:5]}")
            
            # Check for specific makes using partial matching
            test_makes = ["HONDA", "TOYOTA", "NISSAN", "MAZDA", "SUBARU"]
            for make in test_makes:
                in_config = make in config_makes
                # Check if make exists in website using partial matching
                in_website = any(make.upper() in website_make.upper() for website_make in website_makes)
                print(f"  {make}: Config={in_config}, Website={in_website}")
            
            # Find missing makes using partial matching
            missing_makes = set()
            for config_make in config_makes:
                found = False
                for website_make in website_makes:
                    if config_make.upper() in website_make.upper():
                        found = True
                        break
                if not found:
                    missing_makes.add(config_make)
            
            # Find extra makes (website makes that don't match any config make)
            extra_makes = set()
            for website_make in website_makes:
                found = False
                for config_make in config_makes:
                    if config_make.upper() in website_make.upper():
                        found = True
                        break
                if not found:
                    extra_makes.add(website_make)
            
            print(f"\n📊 Make Analysis:")
            print(f"   Config makes: {len(config_makes)}")
            print(f"   Website makes: {len(website_makes)}")
            print(f"   Missing from website: {len(missing_makes)}")
            print(f"   Extra on website: {len(extra_makes)}")
            
            if missing_makes:
                print(f"\n❌ Makes in config but NOT on website:")
                for make in sorted(missing_makes):
                    print(f"    - {make}")
            
            if extra_makes:
                print(f"\n✅ Makes on website but NOT in config:")
                for make in sorted(extra_makes):
                    print(f"    - {make}")
            
            # Now check models for each make in config
            print(f"\n🔍 Checking models for each make...")
            print("=" * 80)
            
            all_missing_models = []
            all_extra_models = []
            total_config_models = 0
            total_website_models = 0
            
            for make in sorted(config_makes):
                # Check if make exists on website using partial matching
                make_found = any(make.upper() in website_make.upper() for website_make in website_makes)
                if not make_found:
                    print(f"⏭️  Skipping {make} (not on website)")
                    continue
                
                print(f"\n📋 Checking {make}...")
                
                # Find make value
                make_value = None
                for option in make_options:
                    # Use partial matching - look for make name anywhere in the text
                    if make.upper() in option['text'].upper():
                        make_value = option['value']
                        break
                
                if not make_value:
                    print(f"    ❌ Could not find make value for {make}")
                    continue
                
                # Select make
                await page.select_option('select[name="mrk"]', make_value)
                
                # Wait for model dropdown to populate
                try:
                    await page.wait_for_selector('select[name="mdl"]:not([disabled])', timeout=10000)
                    await page.wait_for_function('''() => {
                        const modelSelect = document.querySelector('select[name="mdl"]');
                        if (!modelSelect) return false;
                        const options = Array.from(modelSelect.options);
                        return options.length > 1 && options.some(opt => opt.value !== '-1' && opt.value !== '');
                    }''', timeout=10000)
                    await asyncio.sleep(0.5)
                except Exception as e:
                    print(f"    ❌ Error waiting for model dropdown: {e}")
                    continue
                
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
                
                # Compare with config using partial matching
                config_models = set(manufacturer_configs.get(make, {}).keys())
                # Clean website models - remove vehicle counts in parentheses
                website_models_clean = set()
                website_models_raw = {opt['text'] for opt in model_options}
                for website_model in website_models_raw:
                    # Remove vehicle count in parentheses, e.g., "ACCORD PHEV (1)" -> "ACCORD PHEV"
                    if '(' in website_model:
                        clean_model = website_model.split('(')[0].strip()
                    else:
                        clean_model = website_model
                    website_models_clean.add(clean_model)
                
                # Find missing and extra models using exact matching on cleaned names
                missing_models = config_models - website_models_clean
                extra_models = website_models_clean - config_models
                
                total_config_models += len(config_models)
                total_website_models += len(website_models_clean)
                
                print(f"    Config models: {len(config_models)}")
                print(f"    Website models: {len(website_models_clean)}")
                print(f"    Missing from website: {len(missing_models)}")
                print(f"    Extra on website: {len(extra_models)}")
                
                if missing_models:
                    for model in sorted(missing_models):
                        all_missing_models.append((make, model))
                        print(f"      ❌ Missing: {model}")
                
                if extra_models:
                    for model in sorted(extra_models):
                        all_extra_models.append((make, model))
                        print(f"      ✅ Extra: {model}")
            
            # Summary report
            print(f"\n" + "=" * 80)
            print(f"📊 COMPREHENSIVE AUDIT SUMMARY")
            print(f"=" * 80)
            
            print(f"\n🔢 Totals:")
            print(f"   Config makes: {len(config_makes)}")
            print(f"   Website makes: {len(website_makes)}")
            print(f"   Config models: {total_config_models}")
            print(f"   Website models: {total_website_models}")
            
            print(f"\n❌ Issues Found:")
            print(f"   Missing makes: {len(missing_makes)}")
            print(f"   Missing models: {len(all_missing_models)}")
            print(f"   Extra makes: {len(extra_makes)}")
            print(f"   Extra models: {len(all_extra_models)}")
            
            if all_missing_models:
                print(f"\n❌ Models in config but NOT on website ({len(all_missing_models)}):")
                for make, model in sorted(all_missing_models):
                    print(f"    {make}: {model}")
            
            if all_extra_models:
                print(f"\n✅ Models on website but NOT in config ({len(all_extra_models)}):")
                for make, model in sorted(all_extra_models):
                    print(f"    {make}: {model}")
            
            # Generate config update suggestions
            if all_extra_models:
                print(f"\n💡 Suggested config additions:")
                print(f"# Add these models to manufacturer_config.py:")
                current_make = None
                for make, model in sorted(all_extra_models):
                    if make != current_make:
                        if current_make:
                            print("    },")
                        current_make = make
                        print(f'    "{make}": {{')
                    print(f'        "{model}": {{"category": "cars", "age_limit": 6}},')
                if current_make:
                    print("    },")
            
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(audit_manufacturer_config()) 