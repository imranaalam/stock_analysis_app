# functionalities/synchronize_database.py

import streamlit as st
import logging
import sqlite3
import pytz
from utils.db_manager import (
    insert_ticker_data_into_db,
    partial_sync_ticker,
    get_missing_dates,
    initialize_db_and_tables,
    is_data_present_for_date,
    get_last_five_working_days,
    synchronize_database  # Ensure this function is implemented
)
from datetime import datetime, timedelta

# Configure logging with UTF-8 encoding to handle emojis
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove all existing handlers to prevent duplicate logs
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

# Helper function to check if current time is past 5 PM
def is_past_five_pm():
    """
    Checks if the current server time is past 5 PM.

    Returns:
        bool: True if past 5 PM, else False.
    """
    timezone = pytz.timezone("Asia/Karachi")  # Adjust based on your server's timezone
    current_time = datetime.now(timezone)
    logger.debug(f"Current time in Asia/Karachi: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
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

        if st.button("Start Full Synchronization", key="full_sync_button"):
            try:
                # Initialize progress bar and status text
                progress_bar = st.progress(0.0)
                status_text = st.empty()
                log_container = st.container()

                # Determine the date for Full Sync (e.g., last working day)
                last_working_day = get_last_five_working_days(conn, num_days=1)[0]
                date_to_sync = datetime.strptime(last_working_day, '%Y-%m-%d').strftime('%d %b %Y')  # e.g., '15 Sep 2024'

                st.write(f"📅 Synchronizing up to: **{date_to_sync}**")

                # Run synchronization
                with st.spinner('Full synchronization in progress...'):
                    # Call the existing synchronize_database function with the correct date
                    # Ensure that 'synchronize_database' is properly implemented in db_manager.py
                    summary = synchronize_database(conn, date_to_sync, progress_bar, status_text, log_container)

                st.success("✅ Full synchronization has completed. Check the summary below for details.")
                logger.info("Full synchronization completed.")

                # Display summary of synchronization
                st.markdown("### 🔍 Synchronization Summary")
                if summary:
                    # Constituents Data
                    st.markdown("#### PSX Constituents Data")
                    if summary.get('constituents') and summary['constituents']['success']:
                        st.success(summary['constituents']['message'])
                    else:
                        st.error(summary.get('constituents', {}).get('message', "No data available."))

                    # Market Watch Data
                    st.markdown("#### Market Watch Data")
                    if summary.get('market_watch') and summary['market_watch']['success']:
                        st.success(summary['market_watch']['message'])
                    else:
                        st.error(summary.get('market_watch', {}).get('message', "No data available."))

                    # Existing Tickers Data
                    st.markdown("#### Existing Tickers Data")
                    if summary.get('old_tickers') and summary['old_tickers']['success']:
                        st.success(summary['old_tickers']['message'])
                        if summary['old_tickers'].get('errors'):
                            st.warning(f"⚠️ Encountered errors with {len(summary['old_tickers']['errors'])} tickers.")
                            for error in summary['old_tickers']['errors']:
                                st.write(error)
                    else:
                        st.error(summary.get('old_tickers', {}).get('message', "No data available."))

                    # New Tickers Data
                    st.markdown("#### New Tickers Data")
                    if summary.get('new_tickers') and summary['new_tickers']['success']:
                        st.success(summary['new_tickers']['message'])
                        if summary['new_tickers'].get('errors'):
                            st.warning(f"⚠️ Encountered errors with {len(summary['new_tickers']['errors'])} new tickers.")
                            for error in summary['new_tickers']['errors']:
                                st.write(error)
                    else:
                        st.error(summary.get('new_tickers', {}).get('message', "No data available."))
                else:
                    st.warning("⚠️ No synchronization summary available.")

            except Exception as e:
                logger.exception(f"Unexpected error during full synchronization: {e}")
                st.error(f"An unexpected error occurred during Full Sync: {e}")

    elif sync_type == "Partial Sync":
        st.subheader("📅 Partial Synchronization")
        st.write("This will synchronize data for specific missing dates only.")

        try:
            # Retrieve missing dates (last 5 working days)
            missing_dates = get_missing_dates(conn, num_days=5)
            logger.info(f"Missing dates retrieved: {missing_dates}")

            # Determine if today is a working day and after 5 PM
            timezone = pytz.timezone("Asia/Karachi")  # Adjust based on your server's timezone
            today = datetime.now(timezone)
            is_working_day = today.weekday() < 5  # Monday=0, Sunday=6
            is_after_5pm = is_past_five_pm()

            today_str = today.strftime('%Y-%m-%d')

            # If today is a working day and after 5 PM, check if data is missing
            if is_working_day and is_after_5pm:
                if not is_data_present_for_date(conn, today_str):
                    missing_dates.append(today_str)
                    logger.info(f"Today ({today_str}) is a working day after 5 PM and data is missing. Adding to sync buttons.")

            # Remove duplicates if any
            missing_dates = list(dict.fromkeys(missing_dates))

            # Add a date picker for users to select any date for synchronization
            st.markdown("### 📅 Select a Date to Synchronize")
            selected_date = st.date_input(
                "Choose a date",
                value=datetime.today() - timedelta(days=1),
                min_value=datetime(2000, 1, 1),
                max_value=datetime.today()
            )
            selected_date_str = selected_date.strftime('%Y-%m-%d')

            # Button to synchronize the selected date
            if st.button(f"Sync {selected_date_str}", key=f"sync_selected_{selected_date_str}"):
                with st.spinner(f"Synchronizing data for {selected_date_str}..."):
                    summary = partial_sync_ticker(
                        conn=conn,
                        date_to=selected_date_str,
                        progress_bar=st.progress(0),
                        status_text=st.empty(),
                        log_container=st.container()
                    )

                # Display summary
                if summary['success']:
                    st.success(summary['message'])
                else:
                    st.warning(summary['message'])
                    for error in summary['errors']:
                        st.error(error)

            # Display partial sync buttons for missing dates
            if missing_dates:
                st.markdown("### 🔄 Synchronize Missing Dates")
                log_container = st.container()
                progress_bar = st.progress(0)
                status_text = st.empty()

                st.write(f"🔍 Found {len(missing_dates)} dates missing data.")

                for date in missing_dates:
                    button_label = f"Sync {date}"
                    button_key = f"sync_missing_{date}"
                    if st.button(button_label, key=button_key):
                        with st.spinner(f"Synchronizing data for {date}..."):
                            summary = partial_sync_ticker(
                                conn=conn,
                                date_to=date,
                                progress_bar=progress_bar,
                                status_text=status_text,
                                log_container=log_container
                            )

                        # Display summary
                        if summary['success']:
                            st.success(summary['message'])
                        else:
                            st.warning(summary['message'])
                            for error in summary['errors']:
                                st.error(error)
            else:
                st.success("✅ All data up-to-date for the last five working days.")
                st.write("✅ All data up-to-date for the last five working days.")

        except Exception as e:
            logger.exception(f"Unexpected error in Partial Sync UI: {e}")
            st.error(f"An unexpected error occurred: {e}")
