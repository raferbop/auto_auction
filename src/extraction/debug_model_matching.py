#!/usr/bin/env python3
"""
Debug script to test model matching logic.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config.manufacturer_config import manufacturer_configs

def debug_model_matching():
    """Debug the model matching logic."""
    
    # Sample data to simulate the issue
    config_models = {
        "ACCORD", "CIVIC", "CR-V", "FIT", "INSIGHT", "ODYSSEY", "PILOT", "RIDGELINE"
    }
    
    website_models = {
        "ACCORD (12345)", "CIVIC (67890)", "CR-V (11111)", "FIT (22222)", 
        "INSIGHT (33333)", "ODYSSEY (44444)", "PILOT (55555)", "RIDGELINE (66666)",
        "BALLADE (34)", "CAPA (57959)", "CB400 (79)", "CLARITY (11)"
    }
    
    print(f"🔍 Debug Model Matching:")
    print(f"Config models: {len(config_models)}")
    print(f"Website models: {len(website_models)}")
    
    print(f"\nConfig models: {sorted(config_models)}")
    print(f"Website models: {sorted(website_models)}")
    
    # Test the current logic
    print(f"\n🔍 Testing current logic:")
    
    # Find missing models using partial matching
    missing_models = set()
    for config_model in config_models:
        found = False
        for website_model in website_models:
            if config_model.upper() in website_model.upper():
                found = True
                break
        if not found:
            missing_models.add(config_model)
    
    # Find extra models using partial matching
    extra_models = set()
    for website_model in website_models:
        found = False
        for config_model in config_models:
            if config_model.upper() in website_model.upper():
                found = True
                break
        if not found:
            extra_models.add(website_model)
    
    print(f"Missing models: {len(missing_models)}")
    print(f"Extra models: {len(extra_models)}")
    
    # Find matching models
    matching_models = set()
    for config_model in config_models:
        for website_model in website_models:
            if config_model.upper() in website_model.upper():
                matching_models.add((config_model, website_model))
    
    print(f"Matching pairs: {len(matching_models)}")
    for config_model, website_model in sorted(matching_models):
        print(f"  {config_model} -> {website_model}")
    
    # Verify the math
    print(f"\n🔢 Math check:")
    print(f"Config models: {len(config_models)}")
    print(f"Website models: {len(website_models)}")
    print(f"Missing: {len(missing_models)}")
    print(f"Extra: {len(extra_models)}")
    print(f"Matching: {len(matching_models)}")
    print(f"Missing + Extra + Matching = {len(missing_models) + len(extra_models) + len(matching_models)}")
    print(f"Should equal website models: {len(website_models)}")
    
    # The issue: we're double-counting matching models!
    print(f"\n❌ PROBLEM IDENTIFIED:")
    print(f"We're counting matching models in both 'matching' and 'extra' categories!")
    print(f"Each matching config model should only count once, not once per website match.")

if __name__ == "__main__":
    debug_model_matching() 