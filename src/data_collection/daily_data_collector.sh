#!/bin/bash

# Daily Data Collector Wrapper Script
# This script calculates yesterday's date and runs the opt-stk-data-to-db-upstox.py script
# Designed to run via cron Tuesday-Saturday at 7:30 AM to capture Monday-Friday data

# Set the project directory (adjust this path to your actual project location)
PROJECT_DIR="/home/cgraaaj/Projects/cgr-trades"
SCRIPT_DIR="$PROJECT_DIR/src/data_collection"
SCRIPT_NAME="opt-stk-data-to-db-upstox.py"
LOG_DIR="$PROJECT_DIR/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Generate log filename with current date
LOG_FILE="$LOG_DIR/daily_data_collector_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to log errors
log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$LOG_FILE" >&2
}

# Function to send Discord notification
send_discord_notification() {
    local action="$1"
    local trade_date="$2"
    local execution_time="$3"
    local error_message="$4"
    
    # Path to Discord notifier script
    DISCORD_NOTIFIER="$SCRIPT_DIR/discord_notifier.py"
    
    # Check if Discord notifier script exists
    if [ ! -f "$DISCORD_NOTIFIER" ]; then
        log_error "Discord notifier script not found at: $DISCORD_NOTIFIER"
        return 1
    fi
    
    # Build command based on action
    case "$action" in
        "success")
            python "$DISCORD_NOTIFIER" success --trade-date "$trade_date" --execution-time "$execution_time" --log-file "$LOG_FILE" 2>&1 | tee -a "$LOG_FILE"
            ;;
        "failure")
            python "$DISCORD_NOTIFIER" failure --trade-date "$trade_date" --error-message "$error_message" --log-file "$LOG_FILE" 2>&1 | tee -a "$LOG_FILE"
            ;;
        "info")
            python "$DISCORD_NOTIFIER" info --trade-date "$trade_date" --message "$error_message" --level "info" 2>&1 | tee -a "$LOG_FILE"
            ;;
        *)
            log_error "Unknown Discord notification action: $action"
            return 1
            ;;
    esac
}

# Record start time for execution time calculation
START_TIME=$(date +%s)

# Flag to track if data collection actually ran
DATA_COLLECTION_RAN=false

# Function to handle script exit (success or failure)
cleanup_and_exit() {
    local exit_code=$?
    
    # Calculate execution time
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    EXECUTION_TIME_FORMATTED=$(printf "%d minutes %d seconds" $((EXECUTION_TIME / 60)) $((EXECUTION_TIME % 60)))
    
    # Only send notification if data collection actually ran
    if [ "$DATA_COLLECTION_RAN" = true ] && [ -n "$YESTERDAY" ]; then
        if [ $exit_code -eq 0 ]; then
            log_message "Script completed successfully"
            send_discord_notification "success" "$YESTERDAY" "$EXECUTION_TIME_FORMATTED" ""
        else
            log_error "Script failed with exit code: $exit_code"
            send_discord_notification "failure" "$YESTERDAY" "" "Script failed with exit code: $exit_code"
        fi
    fi
    
    exit $exit_code
}

# Set up trap to handle script exit
trap cleanup_and_exit EXIT

# Start logging
log_message "=== Starting Daily Data Collection ==="
log_message "Project directory: $PROJECT_DIR"
log_message "Script directory: $SCRIPT_DIR"
log_message "Log file: $LOG_FILE"

# Check if project directory exists
if [ ! -d "$PROJECT_DIR" ]; then
    log_error "Project directory does not exist: $PROJECT_DIR"
    exit 1
fi

# Check if script directory exists
if [ ! -d "$SCRIPT_DIR" ]; then
    log_error "Script directory does not exist: $SCRIPT_DIR"
    exit 1
fi

# Check if script file exists
if [ ! -f "$SCRIPT_DIR/$SCRIPT_NAME" ]; then
    log_error "Script file does not exist: $SCRIPT_DIR/$SCRIPT_NAME"
    exit 1
fi

# Calculate yesterday's date in YYYY-MM-DD format
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
log_message "Yesterday's date: $YESTERDAY"

# Get current day of week (1=Monday, 7=Sunday)
DAY_OF_WEEK=$(date +%u)
DAY_NAME=$(date +%A)

log_message "Today is: $DAY_NAME (day $DAY_OF_WEEK)"

# Check if today is Tuesday through Saturday (2-6)
if [ "$DAY_OF_WEEK" -lt 2 ] || [ "$DAY_OF_WEEK" -gt 6 ]; then
    log_message "Script should only run Tuesday-Saturday. Today is $DAY_NAME. Exiting."
    
    # Send info notification for wrong day (not success)
    log_message "Sending info notification about schedule..."
    send_discord_notification "info" "$YESTERDAY" "" "Data collection skipped - runs Tuesday-Saturday only. Today is $DAY_NAME."
    
    # Disable trap and exit cleanly without success notification
    trap - EXIT
    exit 0
fi

# Change to project directory
cd "$PROJECT_DIR" || {
    log_error "Failed to change to project directory: $PROJECT_DIR"
    exit 1
}

# Activate virtual environment
VENV_PATH="$PROJECT_DIR/venv"
if [ -d "$VENV_PATH" ]; then
    log_message "Activating virtual environment at: $VENV_PATH"
    source "$VENV_PATH/bin/activate" || {
        log_error "Failed to activate virtual environment at $VENV_PATH"
        exit 1
    }
    log_message "Virtual environment activated successfully"
else
    log_error "Virtual environment not found at: $VENV_PATH"
    exit 1
fi

# Set Python path
export PYTHONPATH="$PROJECT_DIR:$PYTHONPATH"

# Set flag to indicate data collection is about to run
DATA_COLLECTION_RAN=true

# Run the data collection script
log_message "Starting data collection for date: $YESTERDAY"
log_message "Command: python $SCRIPT_DIR/$SCRIPT_NAME $YESTERDAY"

# Run the script and capture output
python "$SCRIPT_DIR/$SCRIPT_NAME" "$YESTERDAY" 2>&1 | tee -a "$LOG_FILE"
SCRIPT_EXIT_CODE=${PIPESTATUS[0]}

# Check if script executed successfully
if [ $SCRIPT_EXIT_CODE -eq 0 ]; then
    log_message "Data collection completed successfully for $YESTERDAY"
else
    log_error "Data collection failed for $YESTERDAY (exit code: $SCRIPT_EXIT_CODE)"
fi

# Deactivate virtual environment if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

log_message "=== Daily Data Collection Finished ==="
log_message "Log file saved to: $LOG_FILE"

# Disable trap before manual exit
trap - EXIT

# Calculate execution time and send notification
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))
EXECUTION_TIME_FORMATTED=$(printf "%d minutes %d seconds" $((EXECUTION_TIME / 60)) $((EXECUTION_TIME % 60)))

# Send Discord notification based on script result (only if data collection ran)
if [ "$DATA_COLLECTION_RAN" = true ]; then
    if [ $SCRIPT_EXIT_CODE -eq 0 ]; then
        log_message "Sending success notification to Discord..."
        send_discord_notification "success" "$YESTERDAY" "$EXECUTION_TIME_FORMATTED" ""
        
        if [ $? -eq 0 ]; then
            log_message "Discord success notification sent successfully"
        else
            log_error "Failed to send Discord success notification"
        fi
    else
        log_message "Sending failure notification to Discord..."
        send_discord_notification "failure" "$YESTERDAY" "" "Data collection script failed with exit code: $SCRIPT_EXIT_CODE"
        
        if [ $? -eq 0 ]; then
            log_message "Discord failure notification sent successfully"
        else
            log_error "Failed to send Discord failure notification"
        fi
    fi
else
    log_message "Data collection did not run - no Discord notification sent"
fi

# Exit with the same code as the Python script
exit $SCRIPT_EXIT_CODE 