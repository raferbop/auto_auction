# run_config.py
"""
Runtime Configuration for Auction Data Collection System
"""

import logging
import os

# Logging configuration
logging_config = {
    "level": logging.INFO,
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "directory": "logs",
    "files": {
        "main": "auction_data_main.log",
        "extraction": "auction_data_extraction.log", 
        "listing": "auction_data_listing.log",
        "errors": "auction_data_errors.log"
    }
}

# Runtime settings
runtime_settings = {
    "max_workers": 5,
    "batch_size": 100,
    "timeout": 30,
    "retry_attempts": 3,
    "concurrent_requests": 10,
    "memory_limit": "512MB",
    "temp_dir": "temp"
}

# Environment settings
env_settings = {
    "debug": os.getenv("DEBUG", "False").lower() == "true",
    "production": os.getenv("PRODUCTION", "False").lower() == "true",
    "log_level": os.getenv("LOG_LEVEL", "INFO").upper()
}

# Export configurations
__all__ = [
    "logging_config",
    "runtime_settings", 
    "env_settings"
] 