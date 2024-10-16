# utils/data_fetcher.py

import requests
import json
import logging
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from io import BytesIO
import aiohttp
import asyncio

# testing this script
# from logger import setup_logging

# when running main.py
from utils.logger import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


# Base URLs for PSX data files
BASE_OFF_MARKET_CSV_URL = "https://dps.psx.com.pk/download/omts/{}.csv"
PSX_CONSTITUENT_URL = "https://dps.psx.com.pk/download/indhist/{}.xls"


# Data from the PDF parsed into a dictionary
internet_trading_subscribers = {
    '001': 'Altaf Adam Securities (Pvt.) Ltd.',
    '006': 'Sherman Securities (Pvt.) Ltd.',
    '008': 'Optimus Capital Management (Pvt.) Ltd.',
    '010': 'Sakarwala Capital Securities (Pvt.) Ltd.',
    '017': 'Summit Capital (Pvt.) Ltd.',
    '018': 'Ismail Iqbal Securities (Pvt.) Ltd.',
    '019': 'AKD Securities Ltd.',
    '021': 'Alpha Capital (Pvt.) Ltd.',
    '022': 'BMA Capital Management Ltd.',
    '025': 'Vector Securities (Pvt.) Ltd.',
    '027': 'Habib Metropolitan Financial Services',
    '037': 'H.H. Misbah Securities (Pvt.) Ltd.',
    '038': 'A.H.M. Securities (Pvt.) Ltd.',
    '044': 'IGI Finex Securities Ltd.',
    '046': 'Fortune Securities Ltd.',
    '047': 'Zillion Capital Securities (Pvt.) Ltd.',
    '048': 'Next Capital Ltd.',
    '049': 'Multiline Securities Ltd.',
    '050': 'Arif Habib Ltd.',
    '058': 'Dawood Equities Ltd.',
    '062': 'WE Financial Services Ltd.',
    '063': 'Al-Habib Capital Markets (Pvt.) Ltd.',
    '068': 'Zafar Securities (Pvt.) Ltd.',
    '077': 'Bawany Securities (Pvt.) Ltd.',
    '084': 'Munir Khanani Securities Ltd.',
    '088': 'Fawad Yusuf Securities (Pvt.) Ltd.',
    '090': 'Darson Securities Private Limited.',
    '091': 'Intermarket Securities Ltd.',
    '092': 'Memon Securities (Pvt.) Ltd.',
    '094': 'FDM Capital Securities (Pvt.) Ltd.',
    '096': 'Msmaniar Financials (Pvt.) Ltd.',
    '102': 'Growth Securities (Pvt.) Ltd.',
    '107': 'Seven Star Securities (Pvt.) Ltd.',
    '108': 'AZEE Securities (Pvt.) Ltd.',
    '112': 'Standard Capital Securities (Pvt.) Ltd.',
    '119': 'Axis Global Ltd.',
    '120': 'Alfalah Securities (Pvt.) Ltd.',
    '124': 'EFG Hermes Pakistan Ltd.',
    '129': 'Taurus Securities Ltd.',
    '140': 'M.M. Securities (Pvt.) Ltd.',
    '142': 'Foundation Securities (Pvt.) Ltd.',
    '145': 'Adam Securities Ltd.',
    '149': 'JS Global Capital Ltd.',
    '159': 'Rafi Securities (Pvt.) Ltd.',
    '162': 'Aba Ali Habib Securities (Pvt.) Ltd.',
    '163': 'Friendly Securities (Pvt.) Ltd.',
    '164': 'Interactive Securities (Pvt.) Ltd.',
    '166': 'Topline Securities Ltd.',
    '169': 'Pearl Securities Ltd.',
    '173': 'Spectrum Securities Ltd.',
    '175': 'First National Equities Ltd.',
    '183': 'R.T. Securities (Pvt.) Ltd.',
    '194': 'MRA Securities Ltd.',
    '199': 'Insight Securities (Pvt.) Ltd.',
    '248': 'Ktrade Securites Ltd.',
    '254': 'ShajarPak Securities (Pvt.) Ltd.',
    '275': 'Rahat Securities Ltd.',
    '293': 'Integrated Equities Ltd.',
    '311': 'Abbasi & Company (Pvt.) Ltd.',
    '332': 'Trust Securities & Brokerage Ltd.',
    '410': 'Zahid Latif Khan Securities (Pvt.) Ltd.',
    '524': 'Akik Capital (Pvt.) Ltd.',
    '525': 'Adam Usman Securities (Pvt.) Ltd.',
    '526': 'Chase Securities Pakistan (Pvt.) Ltd.',
    '528': 'H.G Markets (Private) Limited',
    '529': 'Orbit Securities (Pvt.) Ltd.',
    '531': 'T&G Securities Private Limited.',
    '601': 'JSK Securities Limited',
    '602': 'ZLK Islamic Financial Services Pvt.Ltd.',
    '603': 'Enrichers Securities (Pvt) Ltd.',
    '932': 'Ahsam Securities (Pvt.) Ltd.',
    '934': 'Falki Capital (Pvt.) Ltd.',
    '935': 'Salim Sozer Securities (Pvt.) Ltd.',
    '936': 'Saya Securities (Pvt.) Ltd.',
    '937': 'ASA Stocks (Pvt.) Ltd.',
    '938': 'Margalla Financial (Pvt.) Ltd.',
    '941': 'A.I. Securities (Pvt.) Ltd.',
    '942': 'M. Salim Kasmani Securities (Pvt.) Ltd.',
    '943': 'Z.A. Ghaffar Securities (Pvt.) Ltd.',
    '951': 'K.H.S. Securities (Pvt.) Ltd.',
    '957': '128 Securities (Pvt.) Ltd.',
    '961': 'KP Securities (Pvt.) Ltd.',
    '963': 'Yasir Mahmood Securities (Pvt.) Ltd.',
    '967': 'High Land Securities (Pvt.) Ltd.',
    '972': 'Pasha Securities (Pvt.) Ltd.',
    '973': 'Stocxinvest Securities (Pvt) Ltd',
    '975': 'Adeel Nadeem Securities Ltd.',
    '977': 'Gul Dhami Securities Pvt Ltd',
    '978': 'Dosslani\'s Securities Private Limited',
    '986': 'Progressive Securities Pvt Ltd.',
    '987': 'CMA Securities Pvt Ltd.',
    '988': 'Javed Iqbal Securities Pvt Ltd.',
    '992': 'Vector Securities Private Limited',
    '994': 'Unex Securities (Private) Limited',
    '995': 'Syed Faraz Equities (Private) Limited'
}


SECTOR_MAPPING = {
    "0801": "AUTOMOBILE ASSEMBLER",
    "0802": "AUTOMOBILE PARTS & ACCESSORIES",
    "0803": "CABLE & ELECTRICAL GOODS",
    "0804": "CEMENT",
    "0805": "CHEMICAL",
    "0806": "CLOSE - END MUTUAL FUND",
    "0807": "COMMERCIAL BANKS",
    "0808": "ENGINEERING",
    "0809": "FERTILIZER",
    "0810": "FOOD & PERSONAL CARE PRODUCTS",
    "0811": "GLASS & CERAMICS",
    "0812": "INSURANCE",
    "0813": "INV. BANKS / INV. COS. / SECURITIES COS.",
    "0814": "JUTE",
    "0815": "LEASING COMPANIES",
    "0816": "LEATHER & TANNERIES",
    "0818": "MISCELLANEOUS",
    "0819": "MODARABAS",
    "0820": "OIL & GAS EXPLORATION COMPANIES",
    "0821": "OIL & GAS MARKETING COMPANIES",
    "0822": "PAPER, BOARD & PACKAGING",
    "0823": "PHARMACEUTICALS",
    "0824": "POWER GENERATION & DISTRIBUTION",
    "0825": "REFINERY",
    "0826": "SUGAR & ALLIED INDUSTRIES",
    "0827": "SYNTHETIC & RAYON",
    "0828": "TECHNOLOGY & COMMUNICATION",
    "0829": "TEXTILE COMPOSITE",
    "0830": "TEXTILE SPINNING",
    "0831": "TEXTILE WEAVING",
    "0832": "TOBACCO",
    "0833": "TRANSPORT",
    "0834": "VANASPATI & ALLIED INDUSTRIES",
    "0835": "WOOLLEN",
    "0836": "REAL ESTATE INVESTMENT TRUST",
    "0837": "EXCHANGE TRADED FUNDS",
    "0838": "PROPERTY"
}



def get_stock_data(ticker, date_from, date_to):
    """
    Fetches stock data from the Investors Lounge API for a given ticker and date range.

    Args:
        ticker (str): The stock ticker symbol.
        date_from (str): Start date in 'DD MMM YYYY' format.
        date_to (str): End date in 'DD MMM YYYY' format.

    Returns:
        list: A list of dictionaries containing stock data.
    """
    url = "https://www.investorslounge.com/Default/SendPostRequest"

    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Content-Type": "application/json; charset=UTF-8",
        "Priority": "u=1, i",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    payload = {
        "url": "PriceHistory/GetPriceHistoryCompanyWise",
        "data": json.dumps({
            "company": ticker,
            "sort": "0",
            "DateFrom": date_from,
            "DateTo": date_to,
            "key": ""
        })
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for ticker '{ticker}': {e}")
        return None

    try:
        data = response.json()
        if not isinstance(data, list):
            logging.error(f"Unexpected JSON structure for ticker '{ticker}': Expected a list of records.")
            return None

        # Log the first three rows for debugging
        logging.info(f"First 3 records for ticker '{ticker}': {data[:3]}...")

        logging.info(f"Retrieved {len(data)} records for ticker '{ticker}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for ticker '{ticker}'.")
        return None




async def async_get_stock_data(session, ticker, date_from, date_to):
    """
    Asynchronously fetches stock data from the Investors Lounge API for a given ticker and date range.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        ticker (str): The stock ticker symbol.
        date_from (str): Start date in 'DD MMM YYYY' format.
        date_to (str): End date in 'DD MMM YYYY' format.

    Returns:
        list: List of stock data dictionaries or None if failed.
    """
    url = "https://www.investorslounge.com/Default/SendPostRequest"

    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Content-Type": "application/json; charset=UTF-8",
        "Priority": "u=1, i",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    payload = {
        "url": "PriceHistory/GetPriceHistoryCompanyWise",
        "data": json.dumps({
            "company": ticker,
            "sort": "0",
            "DateFrom": date_from,
            "DateTo": date_to,
            "key": ""
        })
    }

    try:
        async with session.post(url, headers=headers, json=payload, timeout=60) as response:
            response.raise_for_status()
            data = await response.json()
            if not isinstance(data, list):
                logging.error(f"Unexpected JSON structure for ticker '{ticker}': Expected a list of records.")
                return None
            logging.info(f"Retrieved {len(data)} records for ticker '{ticker}'.")
            return data
    except aiohttp.ClientError as e:
        logging.error(f"HTTP Request failed for ticker '{ticker}': {e}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for ticker '{ticker}'.")
        return None
    




async def fetch_all_tickers_data(tickers, date_from, date_to):
    """
    Asynchronously fetches stock data for all tickers.

    Args:
        tickers (list): List of ticker symbols.
        date_from (str): Start date in 'DD MMM YYYY' format.
        date_to (str): End date in 'DD MMM YYYY' format.

    Returns:
        dict: Dictionary with ticker symbols as keys and their data as values.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker in tickers:
            task = asyncio.ensure_future(async_get_stock_data(session, ticker, date_from, date_to))
            tasks.append((ticker, task))

        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

        ticker_data = {}
        for (ticker, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                logging.error(f"Exception occurred while fetching data for ticker '{ticker}': {result}")
                ticker_data[ticker] = None
            else:
                ticker_data[ticker] = result
        return ticker_data



def fetch_kse_market_watch():
    """
    Fetches and parses market watch data from the KSE website.
    
    Args:
        SECTOR_MAPPING (dict): Mapping of sector codes to sector names.
    
    Returns:
        list: List of dictionaries containing market watch data.
    """
    try:
        # URL for the market watch page
        url = "https://dps.psx.com.pk/market-watch"
        
        # Fetch the page
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table containing market data
        table = soup.find('table', {'class': 'tbl'})
        
        # Process each row of the table
        processed_data = []
        rows = table.find('tbody').find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) >= 11:  # Ensure there are enough columns
                processed_item = {
                    'SYMBOL': columns[0].text.strip(),
                    'SECTOR': SECTOR_MAPPING.get(columns[1].text.strip(), 'Unknown'),
                    'LISTED_IN': columns[2].text.strip(),
                    'LDCP': float(columns[3].text.strip().replace(',', '')),
                    'OPEN': float(columns[4].text.strip().replace(',', '')),
                    'HIGH': float(columns[5].text.strip().replace(',', '')),
                    'LOW': float(columns[6].text.strip().replace(',', '')),
                    'CURRENT': float(columns[7].text.strip().replace(',', '')),
                    'CHANGE': round(float(columns[8].text.strip().replace(',', '')), 2),  # Round to 2 decimal points
                    'CHANGE (%)': round(float(columns[9].text.strip().replace('%', '').replace(',', '')), 2),  # Round to 2 decimal points
                    'VOLUME': int(columns[10].text.strip().replace(',', '')),
                }
                processed_data.append(processed_item)
        
        logging.info(f"Fetched and processed {len(processed_data)} market watch records.")
        return processed_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch market watch data: {e}")
        return []
    except Exception as e:
        logging.error(f"Error processing market watch data: {e}")
        return []





def get_defaulters_list():
    """
    Scrapes the defaulters table from PSX to gather defaulters information.

    Returns:
        list: A list of dictionaries, each containing stock symbol, defaulting clause, and other details.
    """
    url = "https://dps.psx.com.pk/listings-table/main/dc"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for defaulters list: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tbl'})

    defaulters_data = []
    for row in table.find('tbody').find_all('tr'):
        cols = row.find_all('td')
        symbol = cols[0].text.strip()
        name = cols[1].text.strip()
        sector = cols[2].text.strip()
        defaulting_clause = cols[3].text.strip()
        clearing_type = cols[4].text.strip()
        shares = int(cols[5].text.strip().replace(',', ''))
        free_float = int(cols[6].text.strip().replace(',', ''))
        listed_in = [tag.text.strip() for tag in cols[7].find_all('div', class_='tag')]

        defaulters_data.append({
            'SYMBOL': symbol,
            'NAME': name,
            'SECTOR': sector,
            'DEFAULTING CLAUSE': defaulting_clause,
            'CLEARING TYPE': clearing_type,
            'SHARES': shares,
            'FREE FLOAT': free_float,
            'LISTED_IN': listed_in,
        })

    logging.info(f"Retrieved {len(defaulters_data)} records from defaulters list.")
    return defaulters_data




def merge_data(market_data, listings_data, defaulters_data):
    """
    Merges market watch data with listings and defaulters data to create a unified dataset.

    Args:
        market_data (list): List of dictionaries containing market watch data.
        listings_data (list): List of dictionaries containing listings data.
        defaulters_data (list): List of dictionaries containing defaulters data.

    Returns:
        list: A list of merged dictionaries with all available information. Includes a 'Defaulter' boolean flag.
    """
    symbol_to_data = {item['SYMBOL']: item for item in market_data}

    # Set of defaulter symbols for easy lookup
    defaulter_symbols = {item['SYMBOL'] for item in defaulters_data}

    # Merge listings data into the market watch data
    for listing in listings_data:
        symbol = listing['SYMBOL']
        if symbol in symbol_to_data:
            symbol_to_data[symbol].update({
                'NAME': listing['NAME'],
                'SHARES': listing['SHARES'],
                'FREE FLOAT': listing['FREE FLOAT'],
                # We no longer care about "CLEARING TYPE", instead, we set a Defaulter flag
                'DEFAULTER': symbol in defaulter_symbols  # Set True if in defaulters, else False
            })

    # Ensure any remaining defaulters that were not part of listings/market data are added
    for defaulter in defaulters_data:
        symbol = defaulter['SYMBOL']
        if symbol in symbol_to_data:
            # Update the defaulter flag if it wasn't already updated
            symbol_to_data[symbol].update({
                'DEFAULTER': True,
                'NAME': defaulter['NAME'],
                'SHARES': defaulter['SHARES'],
                'FREE FLOAT': defaulter['FREE FLOAT'],
                'DEFAULTING CLAUSE': defaulter['DEFAULTING CLAUSE'],
            })
        else:
            # Add missing defaulters directly
            symbol_to_data[symbol] = {
                'SYMBOL': symbol,
                'NAME': defaulter['NAME'],
                'SECTOR': defaulter['SECTOR'],
                'DEFAULTER': True,  # This is a defaulter
                'SHARES': defaulter['SHARES'],
                'FREE FLOAT': defaulter['FREE FLOAT'],
                'DEFAULTING CLAUSE': defaulter['DEFAULTING CLAUSE'],
                'LISTED_IN': defaulter['LISTED_IN'],
                'CHANGE (%)': 0,  # default or unknown for this source
                'CURRENT': 0,  # default or unknown for this source
                'VOLUME': 0,  # default or unknown for this source
            }

    return list(symbol_to_data.values())





def get_listings_data():
    """
    Scrapes the normal listings table from PSX to gather company name, shares, free float, and clearing type.

    Returns:
        list: A list of dictionaries, each containing stock symbol and other details.
    """
    url = "https://dps.psx.com.pk/listings-table/main/nc"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for listings data: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'tbl'})

    listings_data = []
    for row in table.find('tbody').find_all('tr'):
        cols = row.find_all('td')
        symbol = cols[0].text.strip()
        name = cols[1].text.strip()
        sector = cols[2].text.strip()
        clearing_type = cols[3].text.strip()
        shares = int(cols[4].text.strip().replace(',', ''))
        free_float = int(cols[5].text.strip().replace(',', ''))
        listed_in = [tag.text.strip() for tag in cols[6].find_all('div', class_='tag')]

        listings_data.append({
            'SYMBOL': symbol,
            'NAME': name,
            'SECTOR': sector,
            'CLEARING TYPE': clearing_type,
            'SHARES': shares,
            'FREE FLOAT': free_float,
            'LISTED_IN': listed_in,
        })

    logging.info(f"Retrieved {len(listings_data)} records from listings data.")
    return listings_data








def fetch_psx_transaction_data(date):
    """
    Fetches and processes PSX broker-to-broker (B2B) and institution-to-institution (I2I) transactions for a given date.

    Args:
        date (str): The date in 'YYYY-MM-DD' format.

    Returns:
        DataFrame: A single pandas DataFrame containing both B2B and I2I transactions with an additional 'Transaction_Type' field.
    """
    # Step 1: Construct URL based on the date provided
    # Step 1: Construct the URL using the provided date
    url = BASE_OFF_MARKET_CSV_URL.format(date)
    logging.info(f"Fetching PSX CSV data from {url}")
    
    # Step 2: Fetch the CSV data from the URL
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_data = response.text
        logging.info("CSV data fetched successfully.")
    except requests.RequestException as e:
        logging.error(f"Failed to fetch CSV data: {e}")
        return None

    # Step 3: Split the CSV data into 'Broker to Broker Transactions' and 'Institution to Institution Transactions' sections
    split_marker = "CROSS ,TRANSACTIONS, BETWEEN, CLIENT TO ,CLIENT & FINANCIAL, INSTITUTIONS"
    sections = csv_data.split(split_marker)
    
    if len(sections) != 2:
        logging.error("Data format not recognized. Unable to split sections.")
        return None
    
    broker_to_broker_section = sections[0].strip()
    institution_to_institution_section = sections[1].strip()

    # Define a helper function to parse each section
    def parse_section(section, transaction_type):
        """
        Parses a CSV section and returns a cleaned DataFrame.

        Args:
            section (str): The CSV section as a string.
            transaction_type (str): The type of transaction ('B2B' or 'I2I').

        Returns:
            DataFrame: Cleaned DataFrame with appropriate columns.
        """
        data = StringIO(section)
        # Read all rows without skipping to handle multiple headers
        df = pd.read_csv(data, names=columns, skip_blank_lines=True)
        
        # Remove any rows where 'Date' is not a valid date
        df = df[pd.to_datetime(df['Date'], format='%d-%b-%y', errors='coerce').notnull()]
        
        # Convert 'Date' and 'Settlement Date' to 'YYYY-MM-DD' format
        df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d')
        df['Settlement Date'] = pd.to_datetime(df['Settlement Date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d')
        
        # Add Transaction_Type
        df['Transaction_Type'] = transaction_type
        
        return df

    # Step 4: Clean and parse both sections
    columns = ['Date', 'Settlement Date', 'Member Code', 'Symbol Code', 'Company', 'Turnover', 'Rate', 'Value']
    broker_to_broker_df = parse_section(broker_to_broker_section, 'B2B')
    institution_to_institution_df = parse_section(institution_to_institution_section, 'I2I')

    # Step 5: Add member codes
    def add_member_codes(df, is_broker_to_broker):
        """
        Adds buyer and seller codes for broker-to-broker transactions or treats member codes for institution-to-institution transactions.
        """
        if is_broker_to_broker:
            # Assuming 'Member Code' format: "MEMBER +XXX -YYY"
            df[['Buyer Code', 'Seller Code']] = df['Member Code'].str.extract(r'MEMBER\s\+(\d+)\s\-(\d+)')
        else:
            # For institution-to-institution transactions, Member Code is the same for both buyer and seller
            df['Buyer Code'] = df['Member Code'].str.extract(r'(\d+)')
            df['Seller Code'] = df['Member Code'].str.extract(r'(\d+)')
        
        # Ensure that the buyer and seller codes are properly padded with leading zeros
        df['Buyer Code'] = df['Buyer Code'].apply(lambda x: str(x).zfill(3) if pd.notnull(x) else None)
        df['Seller Code'] = df['Seller Code'].apply(lambda x: str(x).zfill(3) if pd.notnull(x) else None)

        return df

    # Apply member code extraction
    broker_to_broker_df = add_member_codes(broker_to_broker_df, is_broker_to_broker=True)
    institution_to_institution_df = add_member_codes(institution_to_institution_df, is_broker_to_broker=False)

    # Step 6: Combine the dataframes into a single one
    combined_df = pd.concat([broker_to_broker_df, institution_to_institution_df], ignore_index=True)

    # Drop rows with missing essential fields
    combined_df.dropna(subset=['Date', 'Symbol Code'], inplace=True)

    # Return the combined DataFrame
    return combined_df

def fetch_psx_constituents(date=None):
    """
    Fetches the PSX constituents Excel file for the given date or today's date if no date is provided.
    
    Args:
        date (str): Optional date in format 'dd MMM yyyy'. If not provided, fetches data for today's date.

    Returns:
        list: A list of dictionaries with the PSX data.
    """
    try:
        from datetime import datetime
        import requests
        import pandas as pd
        import logging
        from io import BytesIO

        # Use today's date if no date is provided
        if date is None:
            date = datetime.today().strftime('%Y-%m-%d')
        else:
            # Expecting date in 'dd MMM yyyy' format
            date_obj = datetime.strptime(date, '%d %b %Y')
            date = date_obj.strftime('%Y-%m-%d')

        # Construct the URL with the given date
        url = PSX_CONSTITUENT_URL.format(date)
        logging.info(f"Fetching PSX data from {url}")

        # Download the Excel file
        response = requests.get(url)
        response.raise_for_status()  # Ensure the request was successful

        # Load the Excel content into a Pandas DataFrame
        excel_file = BytesIO(response.content)
        df = pd.read_excel(excel_file)

        # Rename columns to match db_manager.py expectations
        df.rename(columns={
            'IDX WT %': 'IDX_WT',
            'FF BASED SHARES': 'FF_BASED_SHARES',
            'FF BASED MCAP': 'FF_BASED_MCAP',
            'ORD SHARES': 'ORD_SHARES',
            'ORD SHARES MCAP': 'ORD_SHARES_MCAP'
        }, inplace=True)

        # Ensure the columns are in the expected format
        required_columns = [
            'ISIN', 'SYMBOL', 'COMPANY', 'PRICE', 
            'IDX_WT', 'FF_BASED_SHARES', 'FF_BASED_MCAP', 
            'ORD_SHARES', 'ORD_SHARES_MCAP', 'VOLUME'
        ]
        if not set(required_columns).issubset(df.columns):
            logging.error("Excel file does not contain the expected columns.")
            return []

        # Convert the DataFrame into a list of dictionaries for insertion
        psx_data = df[required_columns].to_dict(orient='records')
        logging.info(f"Fetched {len(psx_data)} records from PSX constituents.")
        return psx_data

    except Exception as e:
        logging.error(f"Error fetching PSX data: {e}")
        return []
    



def fetch_psx_historical(date):
    """
    Fetches historical PSX data for a specific date.

    Args:
        date (str): Date in 'dd-MMM-yyyy' format (e.g., '15-Oct-2024').

    Returns:
        str: HTML content of the fetched data.
    """
    url = "https://dps.psx.com.pk/historical"

    headers = {
        "accept": "text/html, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,ps;q=0.8",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "sec-ch-ua": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "referrer": "https://dps.psx.com.pk/historical",
        "referrerPolicy": "same-origin"
    }

    data = {
        'date': date  # Ensure this matches the expected format
    }

    try:
        # Making the POST request
        response = requests.post(url, headers=headers, data=data, timeout=60)

        # Logging the request details
        logger.info(f"POST Request to {url} with date={date}")

        # Checking if the request was successful
        if response.status_code == 200:
            logger.info(f"Successfully fetched historical data for date: {date}")

            # Optional: Log a snippet of the HTML response for debugging
            # html_snippet = response.text[:1000]  # First 1000 characters
            # logger.debug(f"HTML Response Snippet: {html_snippet}")

            return response.text  # Return the HTML content of the response
        else:
            logger.error(f"Failed to fetch data for date: {date}, Status Code: {response.status_code}")
            raise Exception(f"Error: Unable to fetch data, status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception while fetching historical data for date: {date}, Error: {e}")
        raise Exception(f"Error: Unable to fetch data for date {date}, {e}")

def parse_html_to_df(html_data):
    """
    Parses HTML data into a pandas DataFrame.

    Args:
        html_data (str): HTML content containing the data table.

    Returns:
        DataFrame: Parsed DataFrame with the data.
    """
    soup = BeautifulSoup(html_data, 'html.parser')

    # Attempt to find the first table with class 'tbl'
    table = soup.find('table', {'class': 'tbl'})

    if not table:
        logger.error("No table with class 'tbl' found in the HTML data.")
        return pd.DataFrame()

    rows = table.find_all('tr')

    if not rows:
        logger.error("No table rows found in the HTML data.")
        return pd.DataFrame()

    headers = [header.get_text().strip() for header in rows[0].find_all('th')]

    if not headers:
        logger.error("No table headers found in the HTML data.")
        return pd.DataFrame()

    table_data = []

    for row in rows[1:]:
        cols = row.find_all('td')
        if not cols:
            continue  # Skip rows without columns
        data = [col.get_text().strip() for col in cols]
        table_data.append(data)

    if not table_data:
        logger.warning("No data rows found in the table.")
        return pd.DataFrame()

    df = pd.DataFrame(table_data, columns=headers)

    logger.info("Successfully parsed HTML data into DataFrame.")
    return df




def main():
    """
    Main function to fetch and test stock data, KSE symbols, market watch data, listings, defaulters, 
    off-market and cross transactions, and merge them into a unified dataset.
    """

    print("main")

# This block ensures that the main function is only executed when the script is run directly.
if __name__ == "__main__":
    # Test with a known valid date (e.g., last working day)

    date_input = "2024-10-15"
    try:
        html_data = fetch_historical_data(date_input)  # Fetch the HTML for the specific date
        if html_data:
            df = parse_html_to_df(html_data)  # Parse and get the DataFrame
            if not df.empty:
                print("Parsed DataFrame:")
                print(df.head())
            else:
                print("Parsed DataFrame is empty.")
        else:
            print("Failed to fetch HTML data.")
    except Exception as e:
        print(f"An error occurred: {e}")

