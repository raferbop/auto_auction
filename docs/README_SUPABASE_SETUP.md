# Auction Data Collection System - Supabase Setup Guide

This document provides comprehensive instructions for setting up and using Supabase as the primary database for the auction data collection system.

## 📋 Overview

The system has been configured to use Supabase as the primary database with the following key features:

- **8 Core Tables**: Complete database schema for auction data
- **Direct Database Operations**: Bypasses staging for maximum performance
- **Automatic Data Processing**: Handles vehicle listings, URLs, and detailed auction data
- **Configuration Management**: Stores site and manufacturer configurations
- **Comprehensive Logging**: Tracks all operations and errors

## 🗃️ Database Schema

### Core Tables

| Table Name | Description | Key Features |
|------------|-------------|--------------|
| `vehicles` | Main vehicle listings | Site name, lot number, make, model, year, prices |
| `processed_urls` | URL processing tracking | Tracks extraction status, retry counts |
| `detailed_auction_data` | Detailed lot information | Full vehicle specifications, conditions, images |
| `staging_vehicles` | Temporary processing | Staging area for data validation |
| `auction_sites` | Site configurations | Credentials, rate limits, settings |
| `manufacturers` | Make/model configurations | All supported manufacturers and models |
| `extraction_logs` | Process logging | Operation tracking, performance metrics |
| `email_campaigns` | Generated emails | Marketing campaign data |

### Table Structure Details

#### `vehicles` Table
```sql
- id: Primary key
- site_name: Auction site identifier
- lot_number: Unique lot number per site
- make/model/year: Vehicle identification
- mileage: Vehicle mileage
- start_price/end_price: Auction prices
- grade/color/result: Vehicle condition info
- url/lot_link: Links to original listings
- search_date: When record was found
- created_at/last_updated: Timestamps
```

#### `processed_urls` Table
```sql
- id: Primary key
- site_name: Auction site
- url: Full URL to process
- processed: Processing status
- processing_started/completed: Timestamps
- error_message: Error details
- retry_count: Number of attempts
```

#### `detailed_auction_data` Table
```sql
- id: Primary key
- vehicle_id: Reference to vehicles table
- Complete vehicle specifications
- Condition assessments
- Auction results
- Images and additional data (JSONB)
- Extraction metadata
```

## 🚀 Setup Instructions

### 1. Create Supabase Project

1. Go to [supabase.com](https://supabase.com)
2. Create a new project
3. Wait for project initialization
4. Note your project URL and API keys

### 2. Configure Environment Variables

Create a `.env` file in your project root:

```bash
# Copy from .env.template
cp .env.template .env
```

Update `.env` with your Supabase credentials:

```env
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your-anon-key-here
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key-here
```

### 3. Install Dependencies

```bash
pip install supabase psycopg2-binary python-dotenv
```

### 4. Initialize Database

Run the initialization script:

```bash
python init_database.py
```

This will:
- ✅ Create all required tables
- ✅ Set up indexes and constraints
- ✅ Populate auction sites configuration
- ✅ Populate manufacturers data
- ✅ Verify database setup

### 5. Verify Setup

The script will show output like:
```
🚀 Starting Supabase Database Initialization
🔧 Creating Supabase tables...
✅ Table vehicles created successfully
✅ Table processed_urls created successfully
...
🎉 Database initialization completed successfully!
```

## 📊 Usage Examples

### Basic Database Operations

```python
from db import DatabaseHandler

# Initialize database handler
db = DatabaseHandler()
db.connect()

# Insert vehicle data
listings = [
    {
        "site_name": "AutoPacific",
        "lot_number": "12345",
        "make": "Toyota",
        "model": "Camry",
        "year": 2020,
        "mileage": 50000,
        "start_price": 2000000,
        "url": "https://...",
        "search_date": "2024-01-01"
    }
]

# Direct insertion
count = await db.bulk_insert_vehicles_direct(listings)
print(f"Inserted {count} vehicles")
```

### Using the Optimized Data Collection

```python
from get_data import truly_optimized_main
import asyncio

# Run the fully optimized data collection
asyncio.run(truly_optimized_main())
```

### Configuration Management

```python
from supabase_config import get_supabase_client

# Get configured client
client = get_supabase_client()

# Query auction sites
sites = client.get_client().table("auction_sites").select("*").execute()
print(f"Found {len(sites.data)} auction sites")

# Query manufacturers
manufacturers = client.get_client().table("manufacturers").select("*").execute()
print(f"Found {len(manufacturers.data)} manufacturer/model combinations")
```

## 🔧 Performance Optimizations

### 1. Direct Database Operations
- **TrulyDirectDatabaseHandler**: Bypasses staging for maximum speed
- **Batch Processing**: Processes records in optimized batches
- **Upsert Operations**: Handles duplicates efficiently

### 2. Connection Management
- **Connection Pooling**: Efficient resource utilization
- **Automatic Retries**: Handles temporary failures
- **Timeout Management**: Prevents hanging operations

### 3. Data Processing
- **Concurrent Processing**: Multiple sites processed simultaneously
- **Memory Optimization**: Efficient browser management
- **Rate Limiting**: Respectful site access

## 📈 Monitoring and Logging

### Extraction Logs
Monitor processing through the `extraction_logs` table:

```sql
SELECT 
    site_name,
    operation,
    status,
    records_processed,
    duration,
    created_at
FROM extraction_logs 
ORDER BY created_at DESC
LIMIT 10;
```

### Performance Metrics
Track system performance:

```sql
-- Processing rates by site
SELECT 
    site_name,
    AVG(records_processed) as avg_records,
    AVG(duration) as avg_duration,
    COUNT(*) as total_runs
FROM extraction_logs
WHERE operation = 'data_collection'
GROUP BY site_name;
```

### Error Tracking
Monitor errors and failures:

```sql
-- Recent errors
SELECT 
    site_name,
    error_message,
    retry_count,
    created_at
FROM processed_urls
WHERE error_message IS NOT NULL
ORDER BY created_at DESC;
```

## 🛠️ Maintenance Tasks

### Regular Cleanup
```python
# Clean up old staging data
cleaned = db.cleanup_staging()
print(f"Cleaned {cleaned} old records")

# Process pending URLs
urls = await db.get_unprocessed_urls_concurrent(limit=100)
print(f"Found {len(urls)} pending URLs")
```

### Data Verification
```python
# Verify data integrity
stats = db.verify_data_movement()
print(f"Main table: {stats['main_count']} records")
print(f"Processed URLs: {stats['urls_count']} records")

# Check URL processing status
url_stats = db.verify_url_processing()
print(f"Total URLs: {url_stats['total_vehicles']}")
print(f"Processed: {url_stats['processed_urls']}")
print(f"Remaining: {url_stats['unprocessed_urls']}")
```

## 🔍 Troubleshooting

### Common Issues

#### Connection Errors
```
Error: Failed to connect to Supabase
```
**Solution**: Check environment variables and network connectivity

#### Table Creation Errors
```
Error: Table creation failed
```
**Solution**: Verify service role key has sufficient permissions

#### Data Insertion Errors
```
Error: Batch insertion failed
```
**Solution**: Check data format and column constraints

### Debug Mode
Enable debug logging:
```bash
export DEBUG=true
export LOG_LEVEL=DEBUG
python get_data.py
```

## 📞 Support

For issues and questions:
1. Check the logs in the `logs/` directory
2. Review the `extraction_logs` table for processing details
3. Verify environment variables and credentials
4. Check Supabase dashboard for database status

## 🔮 Future Enhancements

- **Real-time Data Sync**: WebSocket connections for live updates
- **Advanced Analytics**: Built-in dashboards and reports
- **API Endpoints**: REST API for external integrations
- **Data Export**: Automated backup and export features
- **Machine Learning**: Predictive pricing and trend analysis

---

**Ready to collect auction data with Supabase!** 🚀 