# Daily Data Collection Cron Setup Guide

## Overview
This guide will help you set up a cron job to run the options data collection script daily at 7:30 AM, Tuesday through Saturday, to capture Monday-Friday trading data.

## Prerequisites
- Virtual environment located at: `/home/cgraaaj/Projects/cgr-trades/venv`
- Project directory: `/home/cgraaaj/Projects/cgr-trades`
- Script: `src/data_collection/opt-stk-data-to-db-upstox.py`
- Wrapper script: `src/data_collection/daily_data_collector.sh`

## Setup Instructions

### 1. Make the wrapper script executable
```bash
chmod +x /home/cgraaaj/Projects/cgr-trades/src/data_collection/daily_data_collector.sh
```

### 2. Test the wrapper script manually
```bash
# Test the script to ensure it works correctly
/home/cgraaaj/Projects/cgr-trades/src/data_collection/daily_data_collector.sh
```

### 3. Set up the cron job

#### Open crontab editor:
```bash
crontab -e
```

#### Add the following cron job entry:
```bash
# Daily options data collection - runs Tuesday to Saturday at 7:30 AM
# This captures Monday-Friday trading data (yesterday's date)
30 7 * * 2-6 /home/cgraaaj/Projects/cgr-trades/src/data_collection/daily_data_collector.sh

# Optional: Add a comment line above for clarity
# Options Data Collection: Runs Tue-Sat at 7:30 AM to collect Mon-Fri data
```

### 4. Verify cron job is installed
```bash
# List all cron jobs to verify
crontab -l
```

## Cron Schedule Explanation

- `30 7 * * 2-6` means:
  - `30` - At 30 minutes past the hour
  - `7` - At 7 AM (24-hour format)
  - `*` - Every day of the month
  - `*` - Every month
  - `2-6` - Days 2-6 (Tuesday through Saturday)

## Schedule Overview

| Cron Runs On | Collects Data For | Reason |
|-------------|------------------|---------|
| Tuesday 7:30 AM | Monday | Previous trading day |
| Wednesday 7:30 AM | Tuesday | Previous trading day |
| Thursday 7:30 AM | Wednesday | Previous trading day |
| Friday 7:30 AM | Thursday | Previous trading day |
| Saturday 7:30 AM | Friday | Previous trading day |

## Log Files

The script creates detailed log files in:
```bash
/home/cgraaaj/Projects/cgr-trades/logs/
```

Log file format: `daily_data_collector_YYYYMMDD_HHMMSS.log`

## Monitoring and Troubleshooting

### Check if cron job is running
```bash
# Check system log for cron entries
grep CRON /var/log/syslog | tail -20

# Check recent log files
ls -la /home/cgraaaj/Projects/cgr-trades/logs/daily_data_collector_*.log | tail -5
```

### View recent log file
```bash
# View the most recent log file
tail -f /home/cgraaaj/Projects/cgr-trades/logs/daily_data_collector_$(date +%Y%m%d)_*.log
```

### Manual execution for testing
```bash
# Test the script manually with a specific date
cd /home/cgraaaj/Projects/cgr-trades
source venv/bin/activate
python src/data_collection/opt-stk-data-to-db-upstox.py 2025-01-13
```

## Discord Notifications

The system now includes Discord notifications for monitoring:

### Notification Types
- **Success**: Sent when data collection completes successfully
- **Failure**: Sent when data collection fails
- **Info**: Can be sent for general updates (manual use)

### Success Notification Content
- ✅ Trade date processed
- ⏱️ Execution time
- 📋 Log file location
- 📊 Confirmation message

### Failure Notification Content
- ❌ Trade date that failed
- 🔍 Error message
- 📋 Log file location
- ⚠️ Alert message

### Testing Discord Notifications
You can test the Discord notifications before setting up the cron job:

```bash
# Test all notification types
cd /home/cgraaaj/Projects/cgr-trades
source venv/bin/activate
python src/data_collection/test_discord_notification.py
```

### Manual Discord Notifications
You can also send manual notifications:

```bash
# Success notification
python src/data_collection/discord_notifier.py success --trade-date "2025-01-13" --execution-time "5 minutes 30 seconds" --log-file "/path/to/log.log"

# Failure notification
python src/data_collection/discord_notifier.py failure --trade-date "2025-01-13" --error-message "Database connection failed" --log-file "/path/to/log.log"

# Info notification
python src/data_collection/discord_notifier.py info --trade-date "2025-01-13" --message "Custom info message" --level "info"
```

## Error Handling

The wrapper script includes:
- ✅ Virtual environment activation
- ✅ Directory existence checks
- ✅ Day-of-week validation (only runs Tue-Sat)
- ✅ Comprehensive logging
- ✅ Error code propagation
- ✅ Automatic log file creation
- ✅ Discord notifications (success/failure)
- ✅ Execution time tracking

## Important Notes

1. **Virtual Environment**: Script automatically activates the venv at `/home/cgraaaj/Projects/cgr-trades/venv`
2. **Day Logic**: Script only runs Tuesday-Saturday to collect Monday-Friday data
3. **Date Calculation**: Automatically calculates yesterday's date
4. **Logging**: All output is logged to timestamped files
5. **Error Handling**: Script exits with proper error codes for monitoring

## Disabling the Cron Job

To temporarily disable:
```bash
# Edit crontab and comment out the line
crontab -e

# Add # at the beginning of the line:
# 30 7 * * 2-6 /home/cgraaaj/Projects/cgr-trades/src/data_collection/daily_data_collector.sh
```

To permanently remove:
```bash
# Edit crontab and delete the line
crontab -e
```

## Alternative: Using systemd timer (Advanced)

If you prefer systemd timers instead of cron, you can create a service and timer unit. Let me know if you'd like instructions for that setup.

## Next Steps

1. Follow the setup instructions above
2. Test the script manually first
3. Set up the cron job
4. Monitor the log files for the first few runs
5. Verify data is being collected in your database 