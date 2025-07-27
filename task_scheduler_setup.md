# Task Scheduler Setup for get_sales_data.py

## Enhanced Logging System

The script now has a robust logging system that automatically creates timestamped log files in the `logs/` directory. No additional output redirection is needed in Task Scheduler.

## Task Scheduler Configuration

### **Program/script:**
```
C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe
```

### **Add arguments (optional):**
```
"C:\Users\Administrator\Desktop\auto_auction\src\extraction\get_sales_data.py"
```

### **Start in (optional):**
```
C:\Users\Administrator\Desktop\auto_auction
```

## Log File Location

Log files will be automatically created at:
```
C:\Users\Administrator\Desktop\auto_auction\logs\get_sales_data_YYYYMMDD_HHMMSS.log
```

Example: `get_sales_data_20250726_204431.log`

## What Gets Logged

- **Startup information** (Python version, working directory, start time)
- **Site workload distribution** (how many combinations per site)
- **Progress updates** (page processing, records extracted)
- **Database operations** (connection status, save operations)
- **Error messages** (with full stack traces)
- **Completion status** (total duration, success/failure)

## Benefits of Enhanced Logging

1. **Automatic timestamping** - Each run creates a new log file
2. **Comprehensive coverage** - All console output is captured
3. **Error tracking** - Full stack traces for debugging
4. **Progress monitoring** - Real-time updates on processing status
5. **No manual configuration** - Works automatically with Task Scheduler

## Monitoring Your Script

After the task runs, check the log file to see:
- If the script started successfully
- How many records were processed
- Any errors that occurred
- Total execution time
- Database connection status

## Example Log Output

```
[2025-07-26 20:44:31] ============================================================
[2025-07-26 20:44:31] SALES DATA EXTRACTION - STEP 5: ADVANCED FILTERS
[2025-07-26 20:44:31] ============================================================
[2025-07-26 20:44:31] Log file: C:\Users\Administrator\Desktop\auto_auction\logs\get_sales_data_20250726_204431.log
[2025-07-26 20:44:31] Start time: 2025-07-26 20:44:31.123456
[2025-07-26 20:44:31] Python version: 3.11.8
[2025-07-26 20:44:31] Working directory: C:\Users\Administrator\Desktop\auto_auction
[2025-07-26 20:44:31] Initializing pooling system
[2025-07-26 20:44:31] Distributing workload across sites
[2025-07-26 20:44:31] Site workload distribution:
[2025-07-26 20:44:31]   AutoPacific: 478 combinations
[2025-07-26 20:44:31]   Zervtek: 478 combinations
[2025-07-26 20:44:31] 🚀 Launching concurrent processing with advanced filters...
[2025-07-26 20:44:31] 📅 Filters: Year (Jamaica age limits), Scores (4, 4.5, 5, 6), Result (Sold)
[2025-07-26 20:44:31] 🔄 True round-robin distribution enabled
[2025-07-26 20:44:31] Creating task for AutoPacific with 478 combinations
[2025-07-26 20:44:31] Starting 5 concurrent tasks
[2025-07-26 20:44:32] 🌐 AutoPacific: 478 combinations
[2025-07-26 20:44:32] 🔐 AutoPacific: Logged in, setting session filters...
[2025-07-26 20:44:33] 📄 Processing page 1...
[2025-07-26 20:44:33] Page 1: Total data rows found: 50
[2025-07-26 20:44:33] ✅ Saved vehicle sale: ABC123 (Toyota Camry)
[2025-07-26 20:44:34] All sites processed successfully in 0:03:45
[2025-07-26 20:44:34] ============================================================
[2025-07-26 20:44:34] ✅ All sites processed with advanced filters!
[2025-07-26 20:44:34] ============================================================
[2025-07-26 20:44:34] get_sales_data.py script completed successfully
``` 