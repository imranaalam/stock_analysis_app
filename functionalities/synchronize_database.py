# functionalities/synchronize_database.py

import streamlit as st
import logging
import sqlite3
from utils.db_manager import insert_ticker_data_into_db, synchronize_database
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




def synchronize_database_ui(conn):
    """
    Streamlit UI for synchronizing the database.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
    """
    st.header("🔄 Synchronize Database")
    
    # Get the last working day before today
    last_working_day = get_last_working_day(datetime.today() - timedelta(days=1))
    
    # Date input from user with default as last working day
    st.subheader("Select Date for Synchronization")
    selected_date = st.date_input(
        "Choose the date:",
        value=last_working_day,
        min_value=datetime(2000, 1, 1),
        max_value=datetime.today()
    )
    
    # Ensure selected_date is a datetime object
    if isinstance(selected_date, datetime):
        selected_date_obj = selected_date
    else:
        selected_date_obj = datetime.combine(selected_date, datetime.min.time())
    
    # Adjust the selected_date if it's a weekend
    selected_date_obj = get_last_working_day(selected_date_obj)
    date_to = selected_date_obj.strftime('%d %b %Y')  # '15 Sep 2024'
    
    st.write(f"📅 Selected Date for Synchronization: **{date_to}**")
    
    if st.button("Start Synchronization"):
        # Initialize progress bar and status text
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        log_container = st.container()
        
        # Run synchronization
        with st.spinner('Synchronization in progress...'):
            try:
                # Call the updated synchronize_database function
                summary = synchronize_database(conn, date_to, progress_bar, status_text, log_container)
            except Exception as e:
                logger.exception(f"Unexpected error during synchronization: {e}")
                st.error(f"An unexpected error occurred: {e}")
                st.stop()
        
        st.success("✅ Synchronization process has completed. Check the summary below for details.")
        logger.info("Database synchronization completed.")
        
        # Display summary of synchronization
        st.markdown("### 🔍 Synchronization Summary")
        if summary:
            # Constituents Data
            st.markdown("#### PSX Constituents Data")
            if summary['constituents']['success']:
                st.success(summary['constituents']['message'])
            else:
                st.error(summary['constituents']['message'])
            
            # Market Watch Data
            st.markdown("#### Market Watch Data")
            if summary['market_watch']['success']:
                st.success(summary['market_watch']['message'])
            else:
                st.error(summary['market_watch']['message'])
            
            # Existing Tickers Data
            st.markdown("#### Existing Tickers Data")
            if summary['old_tickers']['success']:
                st.success(summary['old_tickers']['message'])
                if summary['old_tickers']['errors']:
                    st.warning(f"⚠️ Encountered errors with {len(summary['old_tickers']['errors'])} tickers.")
                    for error in summary['old_tickers']['errors']:
                        st.write(error)
            else:
                st.error(summary['old_tickers']['message'])
            
            # New Tickers Data
            st.markdown("#### New Tickers Data")
            if summary['new_tickers']['success']:
                st.success(summary['new_tickers']['message'])
                if summary['new_tickers']['errors']:
                    st.warning(f"⚠️ Encountered errors with {len(summary['new_tickers']['errors'])} new tickers.")
                    for error in summary['new_tickers']['errors']:
                        st.write(error)
            else:
                st.error(summary['new_tickers']['message'])
        else:
            st.warning("⚠️ No synchronization summary available.")
