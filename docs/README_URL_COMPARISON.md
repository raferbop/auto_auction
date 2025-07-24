# URL Comparison Analysis Tools

This directory contains tools to analyze discrepancies between the `processed_urls` table and the `vehicle_details` table in your auction data collection system.

## Overview

The system works as follows:
1. **`processed_urls` table**: Contains URLs that need to be processed for detailed vehicle information
2. **`vehicle_details` table**: Contains the detailed vehicle information extracted from those URLs

Since data in the `vehicle_details` table is derived from extracting data from the `processed_urls` table, there should be a 1:1 correspondence between processed URLs and vehicle details records.

## Files

### 1. `url_comparison_script.py`
A comprehensive Python script that performs detailed analysis of URL discrepancies.

**Features:**
- Counts records in all relevant tables
- Compares URLs between `processed_urls` and `vehicle_details` tables
- Identifies missing records in either table
- Analyzes processing status (processed, unprocessed, failed)
- Provides site-by-site breakdown
- Generates detailed recommendations
- Creates comprehensive reports

**Usage:**
```bash
python url_comparison_script.py
```

**Output:**
- Console output with summary
- Detailed log file: `url_comparison.log`
- Comprehensive report: `url_comparison_report_YYYYMMDD_HHMMSS.txt`

### 2. `url_comparison_queries.sql`
A collection of SQL queries you can run directly in your Supabase SQL Editor for quick analysis.

**Queries included:**
1. **Table counts** - Get record counts for all tables
2. **Processing status** - Check how many URLs are processed/unprocessed/failed
3. **Failed processing** - Identify URLs with processing errors
4. **Missing records** - Find URLs in `processed_urls` but not in `vehicle_details`
5. **Orphaned records** - Find URLs in `vehicle_details` but not in `processed_urls`
6. **Site distribution** - Compare counts by auction site
7. **Recent activity** - Check processing activity in the last 7 days
8. **Duplicate detection** - Find duplicate URLs in both tables
9. **Summary statistics** - Overall success rates and matching percentages

## Common Discrepancies and Solutions

### 1. URLs in `processed_urls` but missing from `vehicle_details`
**Possible causes:**
- Processing failed due to website changes
- Network errors during extraction
- Rate limiting or blocking
- Invalid URLs

**Solutions:**
- Check error messages in `processed_urls.error_message`
- Re-run extraction for failed URLs
- Update scraping logic if websites changed

### 2. URLs in `vehicle_details` but missing from `processed_urls`
**Possible causes:**
- Data was inserted directly without going through the URL processing pipeline
- Database integrity issues
- Manual data insertion

**Solutions:**
- Investigate how these records were created
- Consider adding them to `processed_urls` for consistency
- Check for data migration issues

### 3. High number of unprocessed URLs
**Possible causes:**
- Extraction process was interrupted
- Rate limiting prevented processing
- System resources were insufficient

**Solutions:**
- Re-run the extraction process
- Increase rate limiting delays
- Check system resources and logs

### 4. Processing failures
**Possible causes:**
- Website structure changes
- Authentication issues
- Network problems
- Invalid URLs

**Solutions:**
- Review error messages for patterns
- Update scraping selectors if needed
- Check authentication credentials
- Implement retry logic

## Running the Analysis

### Quick Analysis (SQL)
1. Open your Supabase dashboard
2. Go to SQL Editor
3. Copy and paste queries from `url_comparison_queries.sql`
4. Run queries one by one to get insights

### Detailed Analysis (Python)
1. Ensure your environment variables are set:
   ```bash
   export SUPABASE_URL="your-supabase-url"
   export SUPABASE_ANON_KEY="your-anon-key"
   ```
2. Install required packages:
   ```bash
   pip install supabase python-dotenv
   ```
3. Run the script:
   ```bash
   python url_comparison_script.py
   ```

## Interpreting Results

### Key Metrics to Watch:
- **Processing Success Rate**: Percentage of URLs successfully processed
- **URL Matching Rate**: Percentage of processed URLs that have corresponding vehicle details
- **Site-specific Issues**: Some auction sites may have more problems than others
- **Error Patterns**: Look for common error messages that indicate systematic issues

### Action Items:
1. **High failure rate**: Update scraping logic or check website changes
2. **Missing vehicle details**: Re-run extraction for failed URLs
3. **Site-specific issues**: Investigate problems with specific auction sites
4. **Duplicate URLs**: Clean up duplicate records
5. **Orphaned records**: Investigate data integrity issues

## Maintenance

Run this analysis regularly to:
- Monitor extraction success rates
- Identify new issues quickly
- Track improvements over time
- Ensure data quality

## Troubleshooting

### Common Issues:
1. **Connection errors**: Check Supabase credentials and network
2. **Permission errors**: Ensure your API key has read access to all tables
3. **Missing tables**: Verify that all required tables exist in your database
4. **Large datasets**: The script handles large datasets but may take time to process

### Getting Help:
- Check the log files for detailed error messages
- Review the generated reports for specific issues
- Use the SQL queries for targeted investigation 