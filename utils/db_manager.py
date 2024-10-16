# utils/db_manager.py


import sqlite3
import logging
from datetime import datetime, timedelta
import pandas as pd

from utils.data_fetcher import (
    fetch_kse_market_watch,
    get_listings_data,
    get_defaulters_list,
    fetch_psx_transaction_data,
    fetch_psx_constituents,
    get_stock_data,
    async_get_stock_data, 
    fetch_all_tickers_data,
    parse_html_to_df,
    fetch_psx_historical

    
)

# when running main.py
from utils.logger import setup_logging
import asyncio
import aiohttp
from utils.data_fetcher import async_get_stock_data

setup_logging()
logger = logging.getLogger(__name__)



def initialize_db_and_tables(db_path='data/tick_data.db'):
    """
    Initializes the SQLite database and creates the necessary tables if they don't exist.
    This includes the Ticker table and the MarketWatch table, with a unique constraint on the MarketWatch table.
    
    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: A connection object to the SQLite database.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()


        # # ---- Drop MarketWatch table if it exists ---- #
        # cursor.execute("DROP TABLE IF EXISTS MarketWatch")
        # logging.info("MarketWatch table dropped.")


        # Create the Ticker table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Ticker (
                Ticker TEXT,
                Date TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Change REAL,
                "Change (%)" REAL,
                Volume INTEGER,
                PRIMARY KEY (Ticker, Date)
            );
        """)

        # Create the MarketWatch Table with updated 'listed_in' and without 'IS_INDEX'
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MarketWatch (
                SYMBOL TEXT,
                ISIN TEXT,
                COMPANY TEXT,
                SECTOR TEXT,
                LISTED_IN TEXT, 
                LDCP REAL,
                OPEN REAL,
                HIGH REAL,
                LOW REAL,
                CURRENT REAL,
                CHANGE REAL,
                "CHANGE (%)" REAL,
                VOLUME INTEGER,
                DEFAULTER BOOLEAN DEFAULT FALSE,
                DEFAULTING_CLAUSE TEXT,
                PRICE REAL,
                IDX_WT REAL,
                FF_BASED_SHARES INTEGER,
                FF_BASED_MCAP REAL,
                ORD_SHARES INTEGER,
                ORD_SHARES_MCAP REAL,
                SYMBOL_SUFFIX TEXT,
                Date TEXT, 
                PRIMARY KEY (SYMBOL, SECTOR, listed_in)  -- Updated primary key
            );
        """)

        # Create the Transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Transactions (
                Date TEXT,
                Settlement_Date TEXT,
                Buyer_Code TEXT,
                Seller_Code TEXT,
                Symbol_Code TEXT,
                Company TEXT,
                Turnover INTEGER,
                Rate REAL,
                Value REAL,
                Transaction_Type TEXT,
                PRIMARY KEY (Date, Symbol_Code, Buyer_Code, Seller_Code)
            );
        """)

        # Create the Portfolios table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Portfolios (
                Portfolio_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT UNIQUE NOT NULL,
                Stocks TEXT NOT NULL
            );
        """)

        # Create the PSXConstituents table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PSXConstituents (
                ISIN TEXT PRIMARY KEY,
                SYMBOL TEXT,
                COMPANY TEXT,
                PRICE REAL,
                IDX_WT REAL,
                FF_BASED_SHARES INTEGER,
                FF_BASED_MCAP REAL,
                ORD_SHARES INTEGER,
                ORD_SHARES_MCAP REAL,
                VOLUME INTEGER
            );
        """)

        # Commit initial table creation
        conn.commit()
        logging.info(f"Database initialized with necessary tables at {db_path}.")

        # # Add Date and IS_INDEX columns if they don't exist
        # add_date_column(conn)
        # add_is_index_column(conn)

        return conn
    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")
        return None



import re

def clean_numeric(value, field_name, ticker):
    """
    Cleans a numeric string by removing commas, percentage signs, and other non-numeric characters.
    
    Args:
        value (str): The numeric string to clean.
        field_name (str): The name of the field being cleaned (for logging purposes).
        ticker (str): The ticker symbol (for logging purposes).
    
    Returns:
        float or int: The cleaned numeric value.
    
    Raises:
        ValueError: If the cleaned string cannot be converted to float or int.
    """
    try:
        # Remove commas, percentage signs, and any other non-numeric characters except the decimal point
        cleaned_value = re.sub(r'[^\d\.]', '', value)
        
        if '.' in cleaned_value:
            return float(cleaned_value)
        else:
            return int(cleaned_value)
    except Exception as e:
        raise ValueError(f"Error cleaning field '{field_name}' for ticker '{ticker}': {e}")



def insert_market_watch_data_into_db(conn, date_to, batch_size=100):
    """
    Deletes old market watch data and inserts or updates new market watch data into the SQLite database in batches.
    Fetches data for the specified date and ensures the database has the latest information, including defaulter status
    and PSX constituent information.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        date_to (str): Date for synchronization in 'dd MMM yyyy' format (e.g., '15 Sep 2024').
        batch_size (int): Number of records to insert per batch.

    Returns:
        tuple: (success, records_added) where 'success' is a boolean and 'records_added' is the count of records inserted/updated.
    """
    try:
        cursor = conn.cursor()

        # ---- Step 1: Delete Previous Data ---- #
        logger.info("Deleting previous MarketWatch data...")
        cursor.execute("DELETE FROM MarketWatch")
        conn.commit()
        logger.info("Previous MarketWatch data deleted.")

        # ---- Step 2: Fetch Market Watch Data ---- #
        logger.info("Fetching market watch data...")
        market_data = fetch_kse_market_watch()

        if not market_data:
            logger.error("Failed to fetch market watch data.")
            return False, 0
        else:
            logger.info(f"Fetched {len(market_data)} market watch records.")

        # ---- Step 3: Fetch Defaulters Data ---- #
        logger.info("Fetching defaulters data...")
        defaulters_data = get_defaulters_list()
        defaulters_dict = {d['SYMBOL']: d for d in defaulters_data} if defaulters_data else {}

        # ---- Step 4: Fetch PSX Constituents Data ---- #
        logger.info(f"Fetching PSX constituents data for date: {date_to}...")
        psx_data = fetch_psx_constituents(date_to)  # Pass the correctly formatted date
        psx_constituents_dict = {d['SYMBOL']: d for d in psx_data} if psx_data else {}
        logger.info(f"Number of PSX constituent records fetched: {len(psx_data)}")

        if not psx_constituents_dict:
            logger.warning("No PSX constituents data fetched. Fields like ISIN and COMPANY will be NULL.")

        # ---- Step 5: Merge and Prepare Data for Insertion ---- #
        data_to_insert = []
        for record in market_data:
            try:
                symbol_original = record.get('SYMBOL')
                base_symbol, suffix = strip_symbol_suffix(symbol_original)

                sector = record.get('SECTOR')
                listed_in = record.get('LISTED IN')  # This will be split into multiple rows
                ldcp = round(float(record['LDCP']), 2) if record.get('LDCP') else None
                open_ = round(float(record['OPEN']), 2) if record.get('OPEN') else None
                high = round(float(record['HIGH']), 2) if record.get('HIGH') else None
                low = round(float(record['LOW']), 2) if record.get('LOW') else None
                current = round(float(record['CURRENT']), 2) if record.get('CURRENT') else None
                change = round(float(record['CHANGE']), 2) if record.get('CHANGE') else None
                change_p = round(float(record['CHANGE (%)']), 2) if record.get('CHANGE (%)') else None
                volume = int(record['VOLUME']) if record.get('VOLUME') else None
                defaulter = defaulters_dict.get(base_symbol, {}).get('DEFAULTING CLAUSE', None) is not None
                defaulting_clause = defaulters_dict.get(base_symbol, {}).get('DEFAULTING CLAUSE', None)

                # Fetch PSX constituent data
                psx_record = psx_constituents_dict.get(base_symbol, {})
                isin = psx_record.get('ISIN')  # New field
                company = psx_record.get('COMPANY')  # New field
                price = psx_record.get('PRICE')
                idx_wt = psx_record.get('IDX_WT')
                ff_based_shares = psx_record.get('FF_BASED_SHARES')
                ff_based_mcap = psx_record.get('FF_BASED_MCAP')
                ord_shares = psx_record.get('ORD_SHARES')
                ord_shares_mcap = psx_record.get('ORD_SHARES_MCAP')

                # Split the "LISTED IN" field by comma and insert one row per index
                listed_indices = listed_in.split(',') if listed_in else []
                for index in listed_indices:
                    index = index.strip()  # Remove any extra whitespace
                    data_to_insert.append((
                        base_symbol,          # SYMBOL without suffix
                        isin,                 # ISIN
                        company,              # COMPANY
                        sector,
                        index,
                        ldcp, 
                        open_, 
                        high, 
                        low, 
                        current, 
                        change, 
                        change_p, 
                        volume, 
                        defaulter, 
                        defaulting_clause, 
                        price, 
                        idx_wt, 
                        ff_based_shares, 
                        ff_based_mcap, 
                        ord_shares, 
                        ord_shares_mcap, 
                        suffix                # SYMBOL_SUFFIX
                    ))

            except (ValueError, KeyError) as e:
                logger.error(f"Error parsing market watch data record: {record}, error: {e}")
                continue

        # ---- Step 6: Insert a New Row for Defaulters Not in Market Data ---- #
        if defaulters_data:
            for symbol, defaulter in defaulters_dict.items():
                if symbol not in [record.get('SYMBOL') for record in market_data]:
                    # Fetch PSX constituent data
                    psx_record = psx_constituents_dict.get(symbol, {})
                    isin = psx_record.get('ISIN')  # ISIN from PSXConstituents
                    company = psx_record.get('COMPANY')  # COMPANY from PSXConstituents
                    price = psx_record.get('PRICE')
                    idx_wt = psx_record.get('IDX_WT')
                    ff_based_shares = psx_record.get('FF_BASED_SHARES')
                    ff_based_mcap = psx_record.get('FF_BASED_MCAP')
                    ord_shares = psx_record.get('ORD_SHARES')
                    ord_shares_mcap = psx_record.get('ORD_SHARES_MCAP')

                    base_symbol, suffix = strip_symbol_suffix(symbol)

                    data_to_insert.append((
                        base_symbol,          # SYMBOL without suffix
                        isin,                 # ISIN
                        company,              # COMPANY
                        None,                 # SECTOR (unknown)
                        "DEFAULT",            # LISTED IN
                        None,                 # LDCP
                        None,                 # OPEN
                        None,                 # HIGH
                        None,                 # LOW
                        None,                 # CURRENT
                        None,                 # CHANGE
                        None,                 # CHANGE (%)
                        None,                 # VOLUME
                        True,                 # DEFAULTER
                        defaulter['DEFAULTING CLAUSE'] if defaulter else None,  # DEFAULTING_CLAUSE
                        price,
                        idx_wt,
                        ff_based_shares,
                        ff_based_mcap,
                        ord_shares,
                        ord_shares_mcap,
                        suffix                # SYMBOL_SUFFIX
                    ))

        # ---- Step 7: Insert Data in Batches ---- #
        insert_query = """
            INSERT INTO MarketWatch 
            (SYMBOL, ISIN, COMPANY, SECTOR, "LISTED IN", LDCP, OPEN, HIGH, LOW, CURRENT, 
                CHANGE, "CHANGE (%)", VOLUME, DEFAULTER, DEFAULTING_CLAUSE, PRICE, IDX_WT, 
                FF_BASED_SHARES, FF_BASED_MCAP, ORD_SHARES, ORD_SHARES_MCAP, SYMBOL_SUFFIX)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(SYMBOL, SECTOR, "LISTED IN") 
            DO UPDATE SET 
                ISIN = excluded.ISIN,
                COMPANY = excluded.COMPANY,
                LDCP = excluded.LDCP,
                OPEN = excluded.OPEN,
                HIGH = excluded.HIGH,
                LOW = excluded.LOW,
                CURRENT = excluded.CURRENT,
                CHANGE = excluded.CHANGE,
                "CHANGE (%)" = excluded."CHANGE (%)",
                VOLUME = excluded.VOLUME,
                DEFAULTER = excluded.DEFAULTER,
                DEFAULTING_CLAUSE = excluded.DEFAULTING_CLAUSE,
                PRICE = excluded.PRICE,
                IDX_WT = excluded.IDX_WT,
                FF_BASED_SHARES = excluded.FF_BASED_SHARES,
                FF_BASED_MCAP = excluded.FF_BASED_MCAP,
                ORD_SHARES = excluded.ORD_SHARES,
                ORD_SHARES_MCAP = excluded.ORD_SHARES_MCAP,
                SYMBOL_SUFFIX = excluded.SYMBOL_SUFFIX;
        """

        total_records = len(data_to_insert)
        records_added = 0

        for i in range(0, total_records, batch_size):
            batch = data_to_insert[i:i + batch_size]
            logger.info(f"Inserting batch {i // batch_size + 1} with {len(batch)} records.")
            cursor.executemany(insert_query, batch)
            conn.commit()

            # Count records added or updated
            records_added += cursor.rowcount

        logger.info(f"Successfully inserted/updated {records_added} records for MarketWatch data.")

        # ---- Step 8: Confirm Database Status ---- #
        cursor.execute("SELECT COUNT(*) FROM MarketWatch;")
        total_in_db = cursor.fetchone()[0]
        logger.info(f"Total records in the MarketWatch table: {total_in_db}")

        return True, records_added

    except sqlite3.Error as e:
        logger.error(f"Failed to insert market watch data into database: {e}")
        return False, 0
def partial_sync_ticker(conn, date_to, progress_bar=None, status_text=None, log_container=None):
    """
    Performs a partial synchronization by fetching and updating ticker data for a specific date.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
        date_to (str): The date for synchronization in 'YYYY-MM-DD' format.
        progress_bar (streamlit.progress): Streamlit progress bar object.
        status_text (streamlit.empty): Streamlit empty object for status updates.
        log_container (streamlit.container): Streamlit container for logs.
    
    Returns:
        dict: Summary of partial synchronization results.
    """
    summary = {
        'success': False,
        'records_added': 0,
        'message': '',
        'errors': []
    }

    try:
        # Step 1: Fetch PSX historical data for the specific date
        logger.info(f"Fetching PSX historical data for date: {date_to}")
        if log_container:
            log_container.write(f"🔄 Fetching PSX historical data for date: {date_to}")
        
        try:
            html_data = fetch_psx_historical(date_to)
            if not html_data:
                raise ValueError("Failed to fetch PSX historical data.")
            logger.info(f"Successfully fetched historical data for date: {date_to}")
        except Exception as e:
            error_msg = f"Error fetching historical data for date {date_to}: {e}"
            summary['errors'].append(error_msg)
            logger.error(error_msg)
            if log_container:
                log_container.error(error_msg)
            return summary
        
        # Step 2: Parse the HTML data into a DataFrame
        logger.info("Parsing HTML data into DataFrame.")
        try:
            df = parse_html_to_df(html_data)
            if df.empty:
                raise ValueError("Parsed DataFrame is empty.")
            logger.info(f"Successfully parsed DataFrame with {len(df)} records.")
            if log_container:
                log_container.write(f"📊 Parsed DataFrame with {len(df)} records.")
        except Exception as e:
            error_msg = f"Error parsing HTML data for date {date_to}: {e}"
            summary['errors'].append(error_msg)
            logger.error(error_msg)
            if log_container:
                log_container.error(error_msg)
            return summary

        # Step 3: Clean and prepare the data
        # Adjust column names based on actual DataFrame
        required_columns = ['SYMBOL', 'LDCP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'CHANGE', 'CHANGE (%)', 'VOLUME']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing columns in parsed DataFrame: {missing_columns}"
            summary['errors'].append(error_msg)
            logger.error(error_msg)
            if log_container:
                log_container.error(error_msg)
            return summary

        # No 'Date' column in fetched data; set it manually
        df['Date'] = date_to  # Assign the synchronization date to all records

        # Rename 'SYMBOL' to 'Symbol_Code' to match database schema
        df.rename(columns={'SYMBOL': 'Symbol_Code'}, inplace=True)

        # Extract unique symbols from the fetched data
        unique_symbols = df['Symbol_Code'].unique()
        logger.info(f"Unique symbols fetched: {unique_symbols}")

        if log_container:
            log_container.write(f"🔍 Found {len(unique_symbols)} unique symbols to update.")
        
        # Step 4: Update the Ticker table for each symbol
        records_added_total = 0
        errors = []
        total_symbols = len(unique_symbols)
        
        for idx, symbol in enumerate(unique_symbols, start=1):
            try:
                # Filter data for the current symbol and date
                symbol_data = df[(df['Symbol_Code'] == symbol) & (df['Date'] == date_to)]
                if symbol_data.empty:
                    warning_msg = f"No data found for symbol '{symbol}' on date '{date_to}'."
                    logger.warning(warning_msg)
                    if log_container:
                        log_container.warning(warning_msg)
                    continue
                
                # Extract data for insertion
                record = symbol_data.iloc[0]
                ticker = symbol.upper()
                date = record['Date']
                try:
                    # Clean numeric fields
                    open_ = clean_numeric(record['OPEN'], 'OPEN', ticker)
                    high = clean_numeric(record['HIGH'], 'HIGH', ticker)
                    low = clean_numeric(record['LOW'], 'LOW', ticker)
                    close = clean_numeric(record['CLOSE'], 'CLOSE', ticker)
                    change = clean_numeric(record['CHANGE'], 'CHANGE', ticker)
                    change_p = clean_numeric(record['CHANGE (%)'], 'CHANGE (%)', ticker)
                    volume = clean_numeric(record['VOLUME'], 'VOLUME', ticker)
                except ValueError as ve:
                    error_msg = f"❌ Data type conversion error for ticker '{ticker}': {ve}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    if log_container:
                        log_container.error(error_msg)
                    continue
                
                # Insert or update the Ticker table
                success, records_added = insert_ticker_data_into_db(
                    conn, 
                    data=[{
                        'Date': date,
                        'Open': open_,
                        'High': high,
                        'Low': low,
                        'Close': close,
                        'Change': change,
                        'Change (%)': change_p,
                        'Volume': volume
                    }],
                    ticker=ticker
                )
                
                if success:
                    records_added_total += records_added
                    logger.info(f"Successfully synchronized data for ticker '{ticker}'. Records added: {records_added}")
                    if log_container:
                        log_container.success(f"✅ Synchronized data for ticker '{ticker}'. Records added: {records_added}")
                else:
                    error_msg = f"❌ Failed to synchronize data for ticker '{ticker}'."
                    errors.append(error_msg)
                    logger.error(error_msg)
                    if log_container:
                        log_container.error(error_msg)
                
                # Update progress bar
                if progress_bar and status_text:
                    progress = (idx / total_symbols) * 100
                    progress_bar.progress(progress / 100)
                    status_text.text(f"Synchronizing tickers: {idx}/{total_symbols} completed.")
            
            except Exception as e:
                error_msg = f"❌ Unexpected error for ticker '{symbol}': {e}"
                errors.append(error_msg)
                logger.exception(error_msg)
                if log_container:
                    log_container.error(error_msg)
                continue
        
        # Final summary
        summary['records_added'] = records_added_total
        summary['errors'] = errors
        summary['success'] = len(errors) == 0
        summary['message'] = f"✅ Partial synchronization completed with {records_added_total} records added."
        if errors:
            summary['message'] += f" ⚠️ Encountered errors with {len(errors)} tickers."
        
        logger.info(summary['message'])
        if log_container:
            log_container.write(summary['message'])
        
    except Exception as e:
        error_msg = f"Unexpected error during partial synchronization: {e}"
        summary['errors'].append(error_msg)
        logger.exception(error_msg)
        if log_container:
            log_container.error(error_msg)
    
    return summary


def insert_ticker_data_into_db(conn, data, ticker, batch_size=100):
    """
    Inserts the list of stock data into the SQLite database in batches.
    Returns a tuple of (success, records_added).
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
        data (list): List of dictionaries containing ticker data.
        ticker (str): The ticker symbol.
        batch_size (int): Number of records to insert per batch.
    
    Returns:
        tuple: (success (bool), records_added (int))
    """
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT OR REPLACE INTO Ticker 
            (Ticker, Date, Open, High, Low, Close, Change, "Change (%)", Volume) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        
        # Prepare data for insertion
        data_to_insert = []
        for record in data:
            try:
                # Ensure all required fields are present
                date = record.get('Date')
                open_ = record.get('Open')
                high = record.get('High')
                low = record.get('Low')
                close = record.get('Close')
                change = record.get('Change')
                change_p = record.get('Change (%)')
                volume = record.get('Volume')
                
                if all(v is not None for v in [date, open_, high, low, close, change, change_p, volume]):
                    data_to_insert.append((ticker, date, open_, high, low, close, change, change_p, volume))
                else:
                    logging.warning(f"Missing data for ticker '{ticker}' on date '{date}'. Skipping record.")
            except KeyError as e:
                logging.error(f"Missing key {e} in record: {record}. Skipping record.")
                continue
        
        # Insert data in batches
        total_records = len(data_to_insert)
        records_added = 0
        for i in range(0, total_records, batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()
            records_added += cursor.rowcount
        
        logging.info(f"Inserted {records_added} records for ticker '{ticker}'.")
        return True, records_added
    
    except sqlite3.Error as e:
        logging.error(f"Failed to insert data into database for ticker '{ticker}': {e}")
        return False, 0



def strip_symbol_suffix(symbol):
    """
    Strips the suffix from the stock symbol if it ends with XD, XB, XR, or DEF.

    Args:
        symbol (str): The original stock symbol.

    Returns:
        tuple: (base_symbol, suffix) where suffix is one of XD, XB, XR, DEF or None.
    """
    suffixes = ['XD', 'XB', 'XR', 'DEF']
    for suffix in suffixes:
        if symbol.endswith(suffix):
            return symbol[:-len(suffix)], suffix
    return symbol, None



def get_psx_off_market_transactions(conn, from_date, to_date=None):
    """
    Retrieves PSX off-market transactions for a given date or date range from the database.
    
    Args:
        conn: SQLite database connection object.
        from_date: The start date to filter transactions by (format: 'YYYY-MM-DD').
        to_date: The end date to filter transactions by (format: 'YYYY-MM-DD'). If not provided, retrieves for the single 'from_date'.
    
    Returns:
        DataFrame containing the off-market transactions for the given date or date range.
    """
    cursor = conn.cursor()

    if to_date is None:
        # If no to_date is provided, fetch data for just the from_date
        query = """
            SELECT * FROM Transactions WHERE Date = ?;
        """
        logging.info(f"Executing query: {query.strip()}")
        logging.info(f"Fetching transactions for date: {from_date}")
        cursor.execute(query, (from_date,))
    else:
        # If both from_date and to_date are provided, fetch data for the date range
        query = """
            SELECT * FROM Transactions WHERE Date BETWEEN ? AND ?;
        """
        logging.info(f"Executing query: {query.strip()}")
        logging.info(f"Fetching transactions from {from_date} to {to_date}")
        cursor.execute(query, (from_date, to_date))
    
    rows = cursor.fetchall()

    if rows:
        logging.info(f"Retrieved {len(rows)} records from Transactions table.")
        # Create a DataFrame to return the fetched data
        columns = ['Date', 'Settlement_Date', 'Buyer_Code', 'Seller_Code', 'Symbol_Code', 'Company', 'Turnover', 'Rate', 'Value', 'Transaction_Type']
        df = pd.DataFrame(rows, columns=columns)
        return df
    else:
        logging.error("No records found for the provided date or date range.")
        return None
    

def create_portfolio(conn, name, stocks):
    """
    Creates a new portfolio with the given name and list of stocks.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        name (str): Name of the portfolio.
        stocks (list or str): List of stock tickers or a comma-separated string.

    Returns:
        bool: True if creation was successful, False otherwise.
    """
    if isinstance(stocks, list):
        stocks_str = ','.join([stock.strip().upper() for stock in stocks])
    elif isinstance(stocks, str):
        stocks_str = ','.join([stock.strip().upper() for stock in stocks.split(',')])
    else:
        logger.error("Invalid format for stocks. Must be a list or comma-separated string.")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Portfolios (Name, Stocks) VALUES (?, ?);
        """, (name, stocks_str))
        conn.commit()
        logger.info(f"Portfolio '{name}' created with stocks: {stocks_str}.")
        return True
    except sqlite3.IntegrityError as e:
        logger.error(f"Failed to create portfolio '{name}': {e}")
        return False
    except sqlite3.Error as e:
        logger.error(f"Database error while creating portfolio '{name}': {e}")
        return False





def update_portfolio(conn, portfolio_id, new_name=None, new_stocks=None):
    """
    Updates an existing portfolio's name and/or stocks.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        portfolio_id (int): ID of the portfolio to update.
        new_name (str, optional): New name for the portfolio.
        new_stocks (list or str, optional): New list of stock tickers or a comma-separated string.

    Returns:
        bool: True if update was successful, False otherwise.
    """
    if not new_name and not new_stocks:
        logger.warning("No new data provided for update.")
        return False

    updates = []
    parameters = []

    if new_name:
        updates.append("Name = ?")
        parameters.append(new_name)

    if new_stocks:
        if isinstance(new_stocks, list):
            stocks_str = ','.join([stock.strip().upper() for stock in new_stocks])
        elif isinstance(new_stocks, str):
            stocks_str = ','.join([stock.strip().upper() for stock in new_stocks.split(',')])
        else:
            logger.error("Invalid format for new_stocks. Must be a list or comma-separated string.")
            return False
        updates.append("Stocks = ?")
        parameters.append(stocks_str)

    # If no updates were added (just a safeguard)
    if not updates:
        logger.warning("No valid updates found.")
        return False

    # Add Portfolio_ID to parameters for the WHERE clause
    parameters.append(portfolio_id)

    try:
        cursor = conn.cursor()
        query = f"UPDATE Portfolios SET {', '.join(updates)} WHERE Portfolio_ID = ?;"
        cursor.execute(query, tuple(parameters))

        if cursor.rowcount == 0:
            logger.warning(f"No portfolio found with Portfolio_ID = {portfolio_id}.")
            return False

        conn.commit()
        logger.info(f"Portfolio ID '{portfolio_id}' updated successfully.")
        return True
    except sqlite3.IntegrityError as e:
        logger.error(f"Failed to update portfolio ID '{portfolio_id}': {e}")
        return False
    except sqlite3.Error as e:
        logger.error(f"Database error while updating portfolio ID '{portfolio_id}': {e}")
        return False



def delete_portfolio(conn, portfolio_id):
    """
    Deletes a portfolio from the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        portfolio_id (int): ID of the portfolio to delete.

    Returns:
        bool: True if deletion was successful, False otherwise.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Portfolios WHERE Portfolio_ID = ?;", (portfolio_id,))
        if cursor.rowcount == 0:
            logger.warning(f"No portfolio found with Portfolio_ID = {portfolio_id}.")
            return False
        conn.commit()
        logger.info(f"Portfolio ID '{portfolio_id}' deleted successfully.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Failed to delete portfolio ID '{portfolio_id}': {e}")
        return False


def get_portfolio_by_name(conn, name):
    """
    Retrieves a portfolio by its name.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        name (str): Name of the portfolio.

    Returns:
        dict or None: Portfolio details if found, else None.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT Portfolio_ID, Name, Stocks FROM Portfolios WHERE Name = ?;", (name,))
        row = cursor.fetchone()
        if row:
            portfolio = {
                'Portfolio_ID': row[0],
                'Name': row[1],
                'Stocks': [stock.strip() for stock in row[2].split(',') if stock.strip()]
            }
            logger.info(f"Retrieved portfolio '{name}' with ID {row[0]}.")
            return portfolio
        else:
            logger.warning(f"No portfolio found with name '{name}'.")
            return None
    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve portfolio '{name}': {e}")
        return None




def get_all_portfolios(conn):
    """
    Retrieves all portfolios from the Portfolios table, including Portfolio_ID, Name, and Stocks.
    """
    cursor = conn.cursor()
    query = "SELECT Portfolio_ID, Name, Stocks FROM Portfolios;"  # Include Portfolio_ID in the query
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        portfolios = [{
            'Portfolio_ID': row[0],  # Portfolio ID
            'Name': row[1],          # Portfolio Name
            'Stocks': [ticker.strip().upper() for ticker in row[2].split(',')]  # Split and clean up Stocks
        } for row in results]
        logging.info(f"Retrieved {len(portfolios)} portfolios from the database.")
        return portfolios
    except sqlite3.Error as e:
        logging.error(f"Error retrieving portfolios: {e}")
        return []


def get_tickers_by_group(conn, group_type):
    """
    Retrieves tickers based on the specified group type.
    """
    cursor = conn.cursor()

    if group_type == 'topers_today':
        # Tickers with the highest positive change today
        query = """
            SELECT DISTINCT SYMBOL FROM MarketWatch
            ORDER BY "CHANGE (%)" DESC
            LIMIT 50;
        """
    elif group_type == 'decliners_today':
        # Tickers with the highest negative change today
        query = """
            SELECT DISTINCT SYMBOL FROM MarketWatch
            ORDER BY "CHANGE (%)" ASC
            LIMIT 50;
        """
    elif group_type == 'advancers_today':
        # Tickers with significant positive movement today
        query = """
            SELECT DISTINCT SYMBOL FROM MarketWatch
            ORDER BY "CHANGE (%)" DESC
            LIMIT 50;
        """
    else:
        logging.error(f"Unknown group type: {group_type}")
        return []

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        tickers = [row[0] for row in results]
        logging.info(f"Retrieved {len(tickers)} tickers for group '{group_type}'.")
        return tickers
    except sqlite3.Error as e:
        logging.error(f"Error retrieving tickers for group '{group_type}': {e}")
        return []

def get_all_indexes(conn):
    """
    Retrieves all unique indexes from the MarketWatch table.
    """
    cursor = conn.cursor()
    query = 'SELECT DISTINCT "LISTED IN" FROM MarketWatch WHERE "LISTED IN" IS NOT NULL AND "LISTED IN" != "";'
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        indexes = [row[0] for row in results]
        logging.info(f"Retrieved {len(indexes)} unique indexes.")
        return indexes
    except sqlite3.Error as e:
        logging.error(f"Error retrieving indexes: {e}")
        return []

def get_tickers_by_index(conn, index_name):
    """
    Retrieves all unique ticker symbols listed in a specific index.
    """
    cursor = conn.cursor()
    query = """
        SELECT DISTINCT SYMBOL FROM MarketWatch
        WHERE "LISTED IN" = ?;
    """
    try:
        cursor.execute(query, (index_name,))
        results = cursor.fetchall()
        tickers = [row[0] for row in results]
        logging.info(f"Retrieved {len(tickers)} tickers for index '{index_name}'.")
        return tickers
    except sqlite3.Error as e:
        logging.error(f"Error retrieving tickers for index '{index_name}': {e}")
        return []

def display_marketwatch_data(conn):
    """
    Retrieves the latest records from the MarketWatch table for debugging.
    """
    cursor = conn.cursor()
    query = """
        SELECT * FROM MarketWatch
        ORDER BY Date DESC
        LIMIT 10;
    """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
    except sqlite3.Error as e:
        logging.error(f"Error fetching MarketWatch data: {e}")
        return pd.DataFrame()



# ---- Search PSX Constituents by Name ---- #
def search_psx_constituents_by_name(conn, company_name):
    """
    Searches the PSX constituents by company name.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        company_name (str): The name of the company to search.

    Returns:
        list: A list of matching records.
    """
    cursor = conn.cursor()
    query = "SELECT * FROM PSXConstituents WHERE COMPANY LIKE ?"
    cursor.execute(query, ('%' + company_name + '%',))
    return cursor.fetchall()

# ---- Search PSX Constituents by Symbol ---- #
def search_psx_constituents_by_symbol(conn, symbol):
    """
    Searches the PSX constituents by stock symbol.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        symbol (str): The stock symbol to search.

    Returns:
        list: A list of matching records.
    """
    cursor = conn.cursor()
    query = "SELECT * FROM PSXConstituents WHERE SYMBOL = ?"
    cursor.execute(query, (symbol,))
    return cursor.fetchall()

def search_marketwatch_by_symbol(conn, symbol_query):
    """
    Searches MarketWatch symbols by symbol.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
        symbol_query (str): Exact or partial symbol to search.
    
    Returns:
        list: List of matching records as tuples.
    """
    try:
        with conn:
            cursor = conn.cursor()
            query = """
                SELECT DISTINCT SYMBOL
                FROM MarketWatch
                WHERE SYMBOL LIKE ?
                LIMIT 50;
            """
            cursor.execute(query, ('%' + symbol_query + '%',))
            results = cursor.fetchall()
            logging.info(f"Found {len(results)} MarketWatch symbols matching symbol '{symbol_query}'.")
            return results
    except sqlite3.Error as e:
        logging.error(f"Error searching MarketWatch symbols by symbol '{symbol_query}': {e}")
        return []

def get_unique_tickers_from_db(conn):
    """
    Retrieves a list of unique tickers from the Ticker table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: List of unique ticker symbols.
    """
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT Ticker FROM Ticker;")
            tickers = [row[0] for row in cursor.fetchall()]
            logging.info(f"Retrieved tickers from Ticker table: {tickers}")
            return tickers
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve unique tickers from Ticker table: {e}")
        return []


def get_unique_tickers_from_mw(conn):
    """
    Retrieves a list of unique tickers from the MarketWatch table.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
    
    Returns:
        list: List of unique ticker symbols.
    """
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT symbol FROM marketwatch;")
            tickers = [row[0] for row in cursor.fetchall()]
            logging.info(f"Retrieved tickers from MarketWatch: {tickers}")
            return tickers
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve unique tickers from MarketWatch: {e}")
        return []
  


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

def synchronize_database(conn, date_to, progress_bar=None, status_text=None, log_container=None):
    """
    Synchronizes the database by performing the following tasks in order:
    1. Synchronizes PSX Constituents data.
    2. Inserts or updates Market Watch data.
    3. Synchronizes all existing tickers in the Ticker table with up-to-date data.
    4. Adds new tickers based on Constituents not already in the Ticker table.

    If synchronization for the specified date fails, it will attempt the previous working day,
    up to a maximum of 5 attempts.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        date_to (str): The date for which to synchronize data in 'dd MMM YYYY' format.
        progress_bar (streamlit.progress): Streamlit progress bar object.
        status_text (streamlit.empty): Streamlit empty object for status updates.
        log_container (streamlit.container): Streamlit container for logs.

    Returns:
        dict: Summary of synchronization results with detailed messages.
    """
    summary = {
        'constituents': {'success': False, 'records_added': 0, 'message': ''},
        'market_watch': {'success': False, 'records_added': 0, 'message': ''},
        'old_tickers': {'success': False, 'records_added': 0, 'message': '', 'errors': []},
        'new_tickers': {'success': False, 'records_added': 0, 'message': '', 'errors': []}
    }

    max_attempts = 5
    attempt = 0
    date_obj = datetime.strptime(date_to, '%d %b %Y')

    while attempt < max_attempts:
        current_date_to = date_obj.strftime('%d %b %Y')
        try:
            # ---- Task 1: Synchronize PSX Constituents Data ---- #
            logger.info(f"Attempt {attempt + 1}: Synchronizing PSX Constituents data for date: {current_date_to}")
            if log_container:
                log_container.write(f"🔄 Attempt {attempt + 1}: Synchronizing PSX Constituents data for date: {current_date_to}")

            # Fetch PSX Constituents data for the current date
            psx_data = fetch_psx_constituents(current_date_to)

            if psx_data:
                insert_psx_constituents(conn, psx_data)
                summary['constituents']['success'] = True
                summary['constituents']['records_added'] = len(psx_data)
                summary['constituents']['message'] = f"✅ Synchronized PSX Constituents data for {current_date_to} with {len(psx_data)} records added/updated."
                logger.info(summary['constituents']['message'])
                if log_container:
                    log_container.success(summary['constituents']['message'])

                # Proceed to next tasks
                break
            else:
                raise ValueError("No PSX Constituents data fetched.")
        except Exception as e:
            logger.error(f"Error synchronizing PSX Constituents data for {current_date_to}: {e}")
            if log_container:
                log_container.error(f"⚠️ Error synchronizing PSX Constituents data for {current_date_to}: {e}")
            attempt += 1
            if attempt >= max_attempts:
                summary['constituents']['message'] = f"❌ Failed to synchronize PSX Constituents data after {max_attempts} attempts."
                logger.error(summary['constituents']['message'])
                if log_container:
                    log_container.error(summary['constituents']['message'])
                return summary  # Cannot proceed further without constituents data
            else:
                # Move to the previous working day
                date_obj -= timedelta(days=1)
                date_obj = get_last_working_day(date_obj)
                logger.info(f"Attempt {attempt} failed. Trying previous working day: {date_obj.strftime('%d %b %Y')}.")
                if log_container:
                    log_container.warning(f"Attempt {attempt} failed. Trying previous working day: {date_obj.strftime('%d %b %Y')}.")

    if not summary['constituents']['success']:
        return summary  # Early exit if constituents synchronization failed

    # ---- Task 2: Insert/Update Market Watch Data ---- #
    try:
        logger.info("Synchronizing Market Watch data.")
        if log_container:
            log_container.write("🔄 Synchronizing Market Watch data...")

        success, records_added = insert_market_watch_data_into_db(conn, date_to)
        summary['market_watch']['success'] = success
        summary['market_watch']['records_added'] = records_added
        if success:
            summary['market_watch']['message'] = f"✅ Synchronized Market Watch data with {records_added} records added/updated."
            logger.info(summary['market_watch']['message'])
            if log_container:
                log_container.success(summary['market_watch']['message'])
        else:
            summary['market_watch']['message'] = "❌ Failed to synchronize Market Watch data."
            logger.error(summary['market_watch']['message'])
            if log_container:
                log_container.error(summary['market_watch']['message'])

        # Update progress
        if progress_bar and status_text:
            progress_bar.progress(0.30)  # 30%
            status_text.text("Market Watch data synchronized.")
    except Exception as e:
        summary['market_watch']['message'] = f"⚠️ Exception during Market Watch synchronization: {str(e)}"
        logger.exception(summary['market_watch']['message'])
        if log_container:
            log_container.error(summary['market_watch']['message'])
        if progress_bar and status_text:
            progress_bar.progress(0.30)  # 30%
            status_text.text("Market Watch synchronization failed.")

    # ---- Task 3: Synchronize Existing (Old) Tickers ---- #
    try:
        logger.info("Synchronizing existing (old) tickers.")
        if log_container:
            log_container.write("🔄 Synchronizing existing (old) tickers...")

        existing_tickers = get_unique_tickers_from_db(conn)
        total_existing = len(existing_tickers)
        if total_existing == 0:
            summary['old_tickers']['message'] = "ℹ️ No existing tickers found to synchronize."
            logger.info(summary['old_tickers']['message'])
            if log_container:
                log_container.info(summary['old_tickers']['message'])
        else:
            updated_records_total = 0
            for idx, ticker in enumerate(existing_tickers, start=1):
                logger.info(f"Updating ticker {ticker} ({idx}/{total_existing})...")
                if log_container:
                    log_container.write(f"🔄 Updating ticker {ticker} ({idx}/{total_existing})...")

                # Fetch latest data for the ticker
                raw_data = get_stock_data(ticker, "01 Jan 2000", current_date_to)

                if progress_bar and status_text:
                    progress = 0.30 + (idx / total_existing) * 0.30  # Progress between 30% to 60%
                    progress = min(progress, 0.60)  # Ensure it doesn't exceed 60%
                    progress_bar.progress(progress)
                    status_text.text(f"Updating existing ticker {idx}/{total_existing}: {ticker}")

                if raw_data:
                    success, records_added = insert_ticker_data_into_db(conn, raw_data, ticker)
                    if success:
                        updated_records_total += records_added
                        logger.info(f"Added {records_added} records for ticker '{ticker}'.")
                        if log_container:
                            log_container.success(f"✅ Added {records_added} records for ticker '{ticker}'.")
                    else:
                        error_msg = f"❌ Failed to insert data for ticker '{ticker}'."
                        summary['old_tickers']['errors'].append(error_msg)
                        logger.error(error_msg)
                        if log_container:
                            log_container.error(error_msg)
                else:
                    logger.info(f"No new data fetched for ticker '{ticker}'.")
                    if log_container:
                        log_container.warning(f"⚠️ No new data fetched for ticker '{ticker}'.")

            if total_existing > 0:
                summary['old_tickers']['success'] = True
                summary['old_tickers']['records_added'] = updated_records_total
                summary['old_tickers']['message'] = f"✅ Synchronized existing tickers with {updated_records_total} new records added."

                if summary['old_tickers']['errors']:
                    summary['old_tickers']['message'] += f" ⚠️ Encountered errors with {len(summary['old_tickers']['errors'])} tickers."

                logger.info(summary['old_tickers']['message'])
                if log_container:
                    log_container.success(summary['old_tickers']['message'])

                if progress_bar and status_text:
                    progress_bar.progress(0.60)  # 60%
                    status_text.text("Existing tickers synchronization completed.")
    except Exception as e:
        summary['old_tickers']['message'] = f"⚠️ Exception during Existing Tickers synchronization: {str(e)}"
        logger.exception(summary['old_tickers']['message'])
        if log_container:
            log_container.error(summary['old_tickers']['message'])
        if progress_bar and status_text:
            progress_bar.progress(0.60)  # 60%
            status_text.text("Existing tickers synchronization failed.")

    # ---- Task 4: Synchronize New Tickers Based on Constituents ---- #
    try:
        logger.info("Synchronizing new tickers based on Constituents.")
        if log_container:
            log_container.write("🔄 Synchronizing new tickers based on Constituents...")

        # Retrieve tickers from Constituents
        constituents = fetch_psx_constituents(current_date_to)
        constituents_tickers = [record['SYMBOL'] for record in constituents]
        tickers_in_db = get_unique_tickers_from_db(conn)

        # Identify new tickers not in the Ticker table
        new_tickers = list(set(constituents_tickers) - set(tickers_in_db))
        total_new = len(new_tickers)
        if total_new == 0:
            summary['new_tickers']['message'] = "ℹ️ No new tickers to synchronize based on Constituents."
            logger.info(summary['new_tickers']['message'])
            if log_container:
                log_container.info(summary['new_tickers']['message'])
        else:
            data_total_added = 0
            for idx, ticker in enumerate(new_tickers, start=1):
                logger.info(f"Fetching data for new ticker {ticker} ({idx}/{total_new})...")
                if log_container:
                    log_container.write(f"🔄 Fetching data for new ticker {ticker} ({idx}/{total_new})...")

                # Fetch data for the new ticker
                raw_data = get_stock_data(ticker, "01 Jan 2000", current_date_to)

                if progress_bar and status_text:
                    progress = 0.60 + (idx / total_new) * 0.40  # Progress between 60% to 100%
                    progress = min(progress, 1.0)  # Ensure it doesn't exceed 100%
                    progress_bar.progress(progress)
                    status_text.text(f"Synchronizing new ticker {idx}/{total_new}: {ticker}")

                if raw_data:
                    success, records_added = insert_ticker_data_into_db(conn, raw_data, ticker)
                    if success:
                        data_total_added += records_added
                        logger.info(f"Added {records_added} records for new ticker '{ticker}'.")
                        if log_container:
                            log_container.success(f"✅ Added {records_added} records for new ticker '{ticker}'.")
                    else:
                        error_msg = f"❌ Failed to insert data for new ticker '{ticker}'."
                        summary['new_tickers']['errors'].append(error_msg)
                        logger.error(error_msg)
                        if log_container:
                            log_container.error(error_msg)
                else:
                    logger.info(f"No data fetched for new ticker '{ticker}'.")
                    if log_container:
                        log_container.warning(f"⚠️ No data fetched for new ticker '{ticker}'.")

            if new_tickers:
                summary['new_tickers']['success'] = True
                summary['new_tickers']['records_added'] = data_total_added
                summary['new_tickers']['message'] = f"✅ Synchronized {total_new} new tickers with {data_total_added} new records added."

                if summary['new_tickers']['errors']:
                    summary['new_tickers']['message'] += f" ⚠️ Encountered errors with {len(summary['new_tickers']['errors'])} tickers."

                logger.info(summary['new_tickers']['message'])
                if log_container:
                    log_container.success(summary['new_tickers']['message'])

                if progress_bar and status_text:
                    progress_bar.progress(1.0)  # 100%
                    status_text.text("All synchronization tasks completed.")
    except Exception as e:
        summary['new_tickers']['message'] = f"⚠️ Exception during New Tickers synchronization: {str(e)}"
        logger.exception(summary['new_tickers']['message'])
        if log_container:
            log_container.error(summary['new_tickers']['message'])
        if progress_bar and status_text:
            progress_bar.progress(1.0)  # 100%
            status_text.text("New tickers synchronization failed.")

    return summary



def insert_psx_constituents(conn, psx_data):
    """
    Inserts or updates PSX constituents data into the database.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        psx_data (list): A list of dictionaries containing PSX constituent data.
    """
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO PSXConstituents 
            (ISIN, SYMBOL, COMPANY, PRICE, IDX_WT, FF_BASED_SHARES, FF_BASED_MCAP, ORD_SHARES, ORD_SHARES_MCAP, VOLUME)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ISIN) 
            DO UPDATE SET 
                SYMBOL = excluded.SYMBOL,
                COMPANY = excluded.COMPANY,
                PRICE = excluded.PRICE,
                IDX_WT = excluded.IDX_WT,
                FF_BASED_SHARES = excluded.FF_BASED_SHARES,
                FF_BASED_MCAP = excluded.FF_BASED_MCAP,
                ORD_SHARES = excluded.ORD_SHARES,
                ORD_SHARES_MCAP = excluded.ORD_SHARES_MCAP,
                VOLUME = excluded.VOLUME;
        """
        
        # Prepare data for insertion
        data_to_insert = []
        for record in psx_data:
            try:
                data_to_insert.append((
                    record['ISIN'], 
                    record['SYMBOL'], 
                    record['COMPANY'], 
                    record['PRICE'], 
                    record['IDX_WT'],  # Updated column name
                    record['FF_BASED_SHARES'],  # Updated column name
                    record['FF_BASED_MCAP'],  # Updated column name
                    record['ORD_SHARES'],  # Updated column name
                    record['ORD_SHARES_MCAP'],  # Updated column name
                    record['VOLUME']
                ))
            except KeyError as e:
                logger.error(f"Missing key {e} in PSX Constituent record: {record}")
                continue

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Inserted/Updated {len(data_to_insert)} PSX Constituent records.")
    except sqlite3.Error as e:
        logger.error(f"Failed to insert/update PSX Constituents data: {e}")




def main():
    """
    Main function to orchestrate the fetching, processing, and testing of stock-related data.

    This function performs the following steps:
    1. Initializes an in-memory database for testing purposes.
    2. Fetches PSX constituents data for a specific date and inserts it into the database.
    3. Manages portfolios by creating, reading, updating, and deleting portfolio entries.
    4. Fetches and inserts PSX transaction data into the database.
    5. Retrieves and displays transaction data for verification.
    6. Inserts market watch data and retrieves top advancers, decliners, and active stocks.
    7. Fetches listings and defaulters data, merges them, and logs the results.
    8. Closes the database connection after all operations are complete.
    """
    print("main")
# Ensure that the main function runs only when the script is executed directly
if __name__ == "__main__":
    main()