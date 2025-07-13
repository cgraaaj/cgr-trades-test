#!/usr/bin/env python3
"""
Discord Notifier for Daily Data Collection
Sends notifications about the data collection process to Discord webhook.
"""

import sys
import os
import argparse
from datetime import datetime

# Add the Discord notifier library path to sys.path
sys.path.insert(0, '/home/cgraaaj/Projects/day-to-day-ms/python-discord-notifier')

try:
    from notifier.discord_notifier import DiscordNotifier
except ImportError as e:
    print(f"Error importing Discord notifier: {e}")
    sys.exit(1)

# Discord webhook URL
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1357456709064331284/My60-bmIAu6B63EBHbvbodtcLQnqfh8qSoNmoVwA3znnHfu_-n3JDKEeo4b1LiPne-se"

def send_success_notification(trade_date, execution_time=None, log_file=None):
    """Send success notification to Discord."""
    try:
        notifier = DiscordNotifier(DISCORD_WEBHOOK_URL)
        
        # Create notification message
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"""
        ✅ **Daily Data Collection Completed Successfully**
        
        **Trade Date:** {trade_date}
        **Completion Time:** {current_time}
        **Execution Time:** {execution_time or 'N/A'}
        **Log File:** {log_file or 'N/A'}
        
        📊 Options data has been successfully collected and stored in the database.
        """
        
        notifier.send_message(message.strip(), level="info")
        print(f"✅ Success notification sent to Discord for date: {trade_date}")
        
    except Exception as e:
        print(f"❌ Error sending success notification: {e}")
        sys.exit(1)

def send_failure_notification(trade_date, error_message=None, log_file=None):
    """Send failure notification to Discord."""
    try:
        notifier = DiscordNotifier(DISCORD_WEBHOOK_URL)
        
        # Create notification message
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"""
        ❌ **Daily Data Collection Failed**
        
        **Trade Date:** {trade_date}
        **Failure Time:** {current_time}
        **Error:** {error_message or 'Unknown error'}
        **Log File:** {log_file or 'N/A'}
        
        ⚠️ Please check the logs and investigate the issue.
        """
        
        notifier.send_message(message.strip(), level="error")
        print(f"❌ Failure notification sent to Discord for date: {trade_date}")
        
    except Exception as e:
        print(f"❌ Error sending failure notification: {e}")
        sys.exit(1)

def send_info_notification(message, level="info", trade_date=None):
    """Send general info notification to Discord."""
    try:
        notifier = DiscordNotifier(DISCORD_WEBHOOK_URL)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if trade_date:
            formatted_message = f"""
            📢 **Daily Data Collection Schedule Info**
            
            **Trade Date:** {trade_date}
            **Check Time:** {current_time}
            **Status:** {message}
            
            ℹ️ This is normal behavior - data collection follows the market schedule.
            """
        else:
            formatted_message = f"""
            📢 **Daily Data Collection Update**
            
            **Time:** {current_time}
            **Message:** {message}
            """
        
        notifier.send_message(formatted_message.strip(), level=level)
        print(f"📢 Info notification sent to Discord: {message}")
        
    except Exception as e:
        print(f"❌ Error sending info notification: {e}")
        sys.exit(1)

def main():
    """Main function to handle command line arguments and send notifications."""
    parser = argparse.ArgumentParser(description="Send Discord notifications for data collection")
    parser.add_argument("action", choices=["success", "failure", "info"], 
                       help="Type of notification to send")
    parser.add_argument("--trade-date", 
                       help="Trade date in YYYY-MM-DD format (required for success/failure, optional for info)")
    parser.add_argument("--execution-time", 
                       help="Execution time for success notifications")
    parser.add_argument("--error-message", 
                       help="Error message for failure notifications")
    parser.add_argument("--log-file", 
                       help="Path to log file")
    parser.add_argument("--message", 
                       help="Custom message for info notifications")
    parser.add_argument("--level", choices=["info", "warning", "error"], default="info",
                       help="Notification level for info messages")
    
    args = parser.parse_args()
    
    # Validate trade_date is provided for success and failure
    if args.action in ["success", "failure"] and not args.trade_date:
        print(f"❌ Error: --trade-date is required for {args.action} notifications")
        sys.exit(1)
    
    if args.action == "success":
        send_success_notification(
            trade_date=args.trade_date,
            execution_time=args.execution_time,
            log_file=args.log_file
        )
    elif args.action == "failure":
        send_failure_notification(
            trade_date=args.trade_date,
            error_message=args.error_message,
            log_file=args.log_file
        )
    elif args.action == "info":
        if not args.message:
            print("❌ Error: --message is required for info notifications")
            sys.exit(1)
        send_info_notification(args.message, args.level, args.trade_date)

if __name__ == "__main__":
    main() 