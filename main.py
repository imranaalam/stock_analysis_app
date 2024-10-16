# main.py

import streamlit as st
import logging
import sqlite3

from functionalities.synchronize_database import synchronize_database_ui
# Removed import for add_new_ticker
from functionalities.analyze_tickers import analyze_tickers
from functionalities.manage_portfolios import manage_portfolios
from utils.db_manager import initialize_db_and_tables
from utils.logger import setup_logging

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(
    page_title="📈 PSX Scanner",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Title of the App
st.title("📈 PSX Scanner")

# Initialize database
conn = initialize_db_and_tables()

if conn is None:
    st.error("Failed to connect to the database. Please check the logs.")
    logger.error("Database connection failed.")
    st.stop()

# Sidebar for navigation
st.sidebar.header("Menu")
app_mode = st.sidebar.selectbox("Choose the Scanner mode",
    ["Synchronize Database", "Analyze Tickers", "Manage Portfolios"])  # Removed "Add New Ticker"

# Import functionality modules based on user selection
if app_mode == "Synchronize Database":
    synchronize_database_ui(conn)
elif app_mode == "Analyze Tickers":
    analyze_tickers(conn)
elif app_mode == "Manage Portfolios":
    manage_portfolios(conn)

# Close the connection when done
conn.close()
