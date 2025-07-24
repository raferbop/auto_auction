# Auto Auction Data Collection System

A comprehensive system for collecting and analyzing auction data from various Japanese car auction sites.

## 🚀 Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   - Copy `.env.template` to `.env`
   - Add your Supabase credentials

3. **Run the system:**
   ```bash
   # Extract auction data
   python main.py extract
   
   # Compare URLs
   python main.py compare
   
   # Analyze missing URLs
   python main.py analyze
   ```

## 📁 Project Structure

```
auto_auction/
├── src/                    # Core application code
│   ├── core/              # Database, config, utilities
│   ├── extraction/        # Data extraction logic
│   ├── analysis/          # Analysis and comparison tools
│   └── utils/             # Utility scripts
├── config/                # Configuration files
├── reports/               # Generated reports
├── docs/                  # Documentation
├── logs/                  # Log files
└── email/                 # Email functionality
```

## 🔧 Configuration

- **Database**: Configured in `config/supabase_config.py`
- **Auction Sites**: Configured in `config/auction_site_config.py`
- **Manufacturers**: Configured in `config/manufacturer_config.py`

## 📊 Features

- **Multi-site scraping**: Supports multiple auction sites
- **Data standardization**: Consistent data format across sites
- **Error handling**: Comprehensive error tracking and recovery
- **Analysis tools**: URL comparison and missing data analysis
- **Email campaigns**: Automated email generation

## 🛠️ Development

The project is organized into logical modules:

- **Core**: Database operations, configuration, data standardization
- **Extraction**: Web scraping and data collection
- **Analysis**: Data analysis and comparison tools
- **Utils**: Utility scripts and helpers

## 📝 License

This project is for internal use only.
