# functionalities/synchronize_database.py

import streamlit as st
import logging
import sqlite3
import pytz
from utils.db_manager import (
    insert_ticker_data_into_db,
    synchronize_database,
    partial_sync_ticker
)
from datetime import datetime, timedelta

# Configure logging with UTF-8 encoding to handle emojis
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove all existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Create a FileHandler with UTF-8 encoding
file_handler = logging.FileHandler('app.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)

# Create a Formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

# Helper function to find the last working day (excluding weekends)
def get_last_working_day(date):
    """
    Get the last working day before the given date (skips weekends).
    
    Args:
        date (datetime): The reference date.

    Returns:
        datetime: The last working day (Monday to Friday).
    """
    while date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        date -= timedelta(days=1)
    return date


# Helper function to check if current time is past 5 PM
def is_past_five_pm():
    """
    Checks if the current server time is past 5 PM.

    Returns:
        bool: True if past 5 PM, else False.
    """
    timezone = pytz.timezone("Asia/Karachi")  # Adjust based on your server's timezone
    current_time = datetime.now(timezone)
    return current_time.hour >= 17

def synchronize_database_ui(conn):
    """
    Streamlit UI for synchronizing the database.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
    """
    st.header("🔄 Synchronize Database")
    
    # Selection between Full Sync and Partial Sync
    sync_type = st.radio("Select Synchronization Type:", ("Full Sync", "Partial Sync"))
    
    if sync_type == "Full Sync":
        st.subheader("📅 Full Synchronization")
        st.write("This will perform a complete synchronization of the database, updating all tables.")
        
        if st.button("Start Full Synchronization"):
            try:
                # Initialize progress bar and status text
                progress_bar = st.progress(0.0)
                status_text = st.empty()
                log_container = st.container()
                
                # Determine the date for Full Sync (e.g., last working day)
                last_working_day = get_last_working_day(datetime.today() - timedelta(days=1))
                date_to_sync = last_working_day.strftime('%d %b %Y')  # e.g., '15 Sep 2024'
                
                st.write(f"📅 Synchronizing up to: **{date_to_sync}**")
                
                # Run synchronization
                with st.spinner('Full synchronization in progress...'):
                    # Call the synchronize_database function with the correct date
                    summary = synchronize_database(conn, date_to_sync, progress_bar, status_text, log_container)
                
                st.success("✅ Full synchronization has completed. Check the summary below for details.")
                logger.info("Full synchronization completed.")
                
                # Display summary of synchronization
                st.markdown("### 🔍 Synchronization Summary")
                if summary:
                    # Constituents Data
                    st.markdown("#### PSX Constituents Data")
                    if summary.get('constituents', {}).get('success'):
                        st.success(summary['constituents']['message'])
                    else:
                        st.error(summary.get('constituents', {}).get('message', 'No data'))
                    
                    # Market Watch Data
                    st.markdown("#### Market Watch Data")
                    if summary.get('market_watch', {}).get('success'):
                        st.success(summary['market_watch']['message'])
                    else:
                        st.error(summary.get('market_watch', {}).get('message', 'No data'))
                    
                    # Existing Tickers Data
                    st.markdown("#### Existing Tickers Data")
                    if summary.get('old_tickers', {}).get('success'):
                        st.success(summary['old_tickers']['message'])
                        if summary['old_tickers'].get('errors'):
                            st.warning(f"⚠️ Encountered errors with {len(summary['old_tickers']['errors'])} tickers.")
                            for error in summary['old_tickers']['errors']:
                                st.write(error)
                    else:
                        st.error(summary.get('old_tickers', {}).get('message', 'No data'))
                    
                    # New Tickers Data
                    st.markdown("#### New Tickers Data")
                    if summary.get('new_tickers', {}).get('success'):
                        st.success(summary['new_tickers']['message'])
                        if summary['new_tickers'].get('errors'):
                            st.warning(f"⚠️ Encountered errors with {len(summary['new_tickers']['errors'])} new tickers.")
                            for error in summary['new_tickers']['errors']:
                                st.write(error)
                    else:
                        st.error(summary.get('new_tickers', {}).get('message', 'No data'))
                else:
                    st.warning("⚠️ No synchronization summary available.")
            
            except Exception as e:
                logger.exception(f"Unexpected error during full synchronization: {e}")
                st.error(f"An unexpected error occurred during Full Sync: {e}")
    
    elif sync_type == "Partial Sync":
        st.subheader("📅 Partial Synchronization")
        st.write("This will synchronize data for a specific date only.")
        
        try:
            # Get the last working day before today
            today = datetime.today()
            last_working_day = get_last_working_day(today - timedelta(days=1))
            
            # Determine if today can be synced (past 5 PM)
            can_sync_today = is_past_five_pm()
            
            # Date input from user with default as last working day
            st.subheader("Select Date for Partial Synchronization")
            if can_sync_today:
                date_options = [last_working_day, today]
                date_labels = [last_working_day.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')]
            else:
                date_options = [last_working_day]
                date_labels = [last_working_day.strftime('%Y-%m-%d')]
                st.info("Today cannot be selected for synchronization as it's before 5 PM.")
            
            selected_date = st.selectbox(
                "Choose the date:",
                options=date_options,
                format_func=lambda x: x.strftime('%Y-%m-%d'),
                index=0
            )
            
            date_to_sync = selected_date.strftime('%Y-%m-%d')  # '2024-10-15'
            
            st.write(f"📅 Selected Date for Partial Synchronization: **{date_to_sync}**")
            
            if st.button("Start Partial Synchronization"):
                try:
                    # Initialize progress bar and status text
                    progress_bar = st.progress(0.0)
                    status_text = st.empty()
                    log_container = st.container()
                    
                    # Run partial synchronization
                    with st.spinner(f'Partial synchronization for {date_to_sync} in progress...'):
                        summary = partial_sync_ticker(conn, date_to_sync, progress_bar, status_text, log_container)
                    
                    if summary['success']:
                        st.success(f"✅ Partial synchronization for {date_to_sync} has completed successfully.")
                        logger.info(f"Partial synchronization for {date_to_sync} completed.")
                    else:
                        st.error(f"❌ Partial synchronization for {date_to_sync} failed.")
                        logger.error(f"Partial synchronization for {date_to_sync} failed.")
                    
                    # Display summary of partial synchronization
                    st.markdown("### 🔍 Partial Synchronization Summary")
                    if summary:
                        # Ticker Data
                        st.markdown("#### Ticker Data")
                        if summary['success']:
                            st.success(summary['message'])
                        else:
                            st.error(summary['message'])
                        
                        # Errors if any
                        if summary.get('errors'):
                            st.warning(f"⚠️ Encountered errors with {len(summary['errors'])} tickers.")
                            for error in summary['errors']:
                                st.write(error)
                    else:
                        st.warning("⚠️ No synchronization summary available.")
                
                except Exception as e:
                    logger.exception(f"Unexpected error during partial synchronization: {e}")
                    st.error(f"An unexpected error occurred during Partial Sync: {e}")
        
        except Exception as e:
            logger.exception(f"Unexpected error in Partial Sync UI: {e}")
            st.error(f"An unexpected error occurred: {e}")