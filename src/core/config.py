# config.py
"""
Unified Configuration for Auction Data Collection System
"""

# Import existing configurations
from config.auction_site_config import auction_sites
from config.manufacturer_config import manufacturer_configs

# Scraping settings
scraping_settings = {
    "request_delay": 2.0,
    "max_requests_per_hour": 1000,
    "concurrent_browsers": 3,
    "browser_timeout": 60,
    "page_load_timeout": 30,
    "retry_attempts": 3,
    "retry_delay": 5
}

# Export all configurations
__all__ = [
    "auction_sites",
    "manufacturer_configs", 
    "scraping_settings"
] 