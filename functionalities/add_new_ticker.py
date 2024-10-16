# functionalities/add_new_ticker.py

import streamlit as st
import logging
from utils.db_manager import get_unique_tickers_from_db, get_unique_tickers_from_mw, insert_ticker_data_into_db
from utils.data_fetcher import get_stock_data
import pandas as pd

def add_new_ticker_ui(conn):
    st.header("➕ Add New Ticker")
    ticker_input = st.text_input("Enter Ticker Symbol (e.g., AAPL, MSFT):").upper()
    if st.button("Add Ticker"):
        sync_all_tickers(conn)
    else:
        st.error("Please enter a valid ticker symbol.")
        logging.error("Empty ticker symbol entered.")


def sync_all_tickers(conn):
    """
    Syncs all unique tickers from MarketWatch to the ticker table by fetching stock data 
    and adding it into the ticker table if not already present.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
    """
    try:
        # Get the unique tickers from MarketWatch and the existing ticker table
        tickers_in_db = get_unique_tickers_from_db(conn)
        tickers_in_mw = get_unique_tickers_from_mw(conn)
        
        # Filter out the tickers that are already in the database
        tickers_to_add = [ticker for ticker in tickers_in_mw if ticker not in tickers_in_db]
        
        if not tickers_to_add:
            logging.info("No new tickers to sync from MarketWatch.")
            return
        
        logging.info(f"Tickers to sync: {tickers_to_add}")
        
        # Loop through each ticker and fetch its data
        for ticker in tickers_to_add:
            logging.info(f"Fetching data for ticker '{ticker}'...")
            
            try:
                # Fetch data for the ticker
                raw_data = get_stock_data(ticker, "01 Jan 2020", pd.Timestamp.today().strftime("%d %b %Y"))
                
                if raw_data:
                    # Insert the data into the ticker table
                    success, records_added = insert_ticker_data_into_db(conn, raw_data, ticker)
                    
                    if success:
                        if records_added > 0:
                            logging.info(f"Added {records_added} records for ticker '{ticker}'.")
                        else:
                            logging.info(f"No new records to add for ticker '{ticker}'.")
                    else:
                        logging.error(f"Failed to insert data for ticker '{ticker}'.")
                else:
                    logging.error(f"Failed to retrieve data for ticker '{ticker}'.")
                    
            except Exception as e:
                logging.error(f"Error fetching or inserting data for ticker '{ticker}': {e}")
    
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")