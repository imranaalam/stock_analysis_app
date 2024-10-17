
def get_kse_index_historical_data(index_symbol):
    """
    Fetches historical data for a given index from the PSX Timeseries API.

    Args:
        index_symbol (str): The symbol identifier for the index (e.g., 'ACI').

    Returns:
        dict: Contains 'status', 'message', and 'data' where 'data' is a list of lists.
              Each inner list contains [timestamp, price, volume].
              Returns None if the fetch fails.

    Example Return:
        {
            "status": 1,
            "message": "",
            "data": [
                [1728648858, 12775.6255, 100],
                [1728648828, 12775.6255, 101],
                ...
            ]
        }
    """
    url = f"https://dps.psx.com.pk/timeseries/eod/{index_symbol}"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index historical data '{index_symbol}': {e}")
        return None

    try:
        data = response.json()
        if not isinstance(data, dict) or 'data' not in data:
            logging.error(f"Unexpected JSON structure for index historical data '{index_symbol}'.")
            return None
        logging.info(f"Retrieved {len(data['data'])} historical data points for index '{index_symbol}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for index historical data '{index_symbol}'.")
        return None








def get_all_kse_indices_historical_data(index_symbols):
    """
    Fetches historical data for a list of index symbols.

    Args:
        index_symbols (list of str): List of index symbol identifiers (e.g., ['ACI']).

    Returns:
        dict: Keys are index symbols, and values are historical data dictionaries.
              Returns None for any index symbol that fails to fetch.

    Example Return:
        {
            "ACI": {
                "status": 1,
                "message": "",
                "data": [
                    [1728648858, 12775.6255, 100],
                    [1728648828, 12775.6255, 101],
                    ...
                ]
            },
            ...
        }
    """
    historical_data = {}
    for symbol in index_symbols:
        data = get_kse_index_historical_data(symbol)
        if data is not None:
            historical_data[symbol] = data
        else:
            logging.warning(f"Failed to fetch historical data for index '{symbol}'.")
    return historical_data



def get_kse_ticker_detail(index_symbol):
    """
    Fetches the constituents of a given index from the PSX Indices page.

    Args:
        index_symbol (str): The symbol identifier for the index (e.g., 'JSGBKTI').

    Returns:
        list of dict: Each dictionary contains constituent information such as 'SYMBOL', 'NAME', 'LDCP', 'CURRENT',
                      'CHANGE', 'CHANGE (%)', 'IDX WTG (%)', 'IDX POINT', 'VOLUME', 'FREEFLOAT (M)', 'MARKET CAP (M)'.
                      Returns None if the fetch fails.

    Example Return:
        [
            {
                "SYMBOL": "BAFL",
                "NAME": "Bank Alfalah Limited",
                "LDCP": 66.98,
                "CURRENT": 67.86,
                "CHANGE": 0.88,
                "CHANGE (%)": 1.31,
                "IDX WTG (%)": 15.66,
                "IDX POINT": 48.55,
                "VOLUME": 278476,
                "FREEFLOAT (M)": 1311,
                "MARKET CAP (M)": 88984
            },
            ...
        ]
    """
    url = f"https://dps.psx.com.pk/indices/{index_symbol}"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://dps.psx.com.pk/indices",
        "Connection": "keep-alive"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index constituents '{index_symbol}': {e}")
        return None

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tbl'})
        if not table:
            logging.error(f"No table found for index constituents '{index_symbol}'.")
            return None

        # Extract table headers
        headers = []
        for th in table.find('thead').find_all('th'):
            header_text = th.get_text(strip=True)
            # Normalize header names
            header_text = re.sub(r'\s+', ' ', header_text)
            headers.append(header_text)

        # Extract table rows
        constituents = []
        for tr in table.find('tbody').find_all('tr'):
            cols = tr.find_all('td')
            if len(cols) != len(headers):
                continue  # Skip rows that don't match header length
            row = {}
            for header, td in zip(headers, cols):
                # Handle specific columns
                if header == "SYMBOL":
                    symbol = td.find('strong').get_text(strip=True)
                    row["SYMBOL"] = symbol
                elif header == "NAME":
                    name = td.get_text(strip=True)
                    row["NAME"] = name
                else:
                    # Extract numerical data
                    data_order = td.get('data-order')
                    if data_order is not None:
                        try:
                            if header in ["LDCP", "CURRENT", "CHANGE", "CHANGE (%)", "IDX WTG (%)", "IDX POINT", "FREEFLOAT (M)", "MARKET CAP (M)"]:
                                value = float(data_order)
                            elif header == "VOLUME":
                                value = int(data_order.replace(',', ''))
                            else:
                                value = td.get_text(strip=True)
                        except ValueError:
                            value = td.get_text(strip=True)
                    else:
                        value = td.get_text(strip=True)
                    row[header] = value
            constituents.append(row)

        logging.info(f"Retrieved {len(constituents)} constituents for index '{index_symbol}'.")
        return constituents
    except Exception as e:
        logging.error(f"Error parsing HTML for index constituents '{index_symbol}': {e}")
        return None






def get_kse_ticker_detail(index_symbol):
    """
    Fetches the constituents of a given index from the PSX Indices page.

    Args:
        index_symbol (str): The symbol identifier for the index (e.g., 'JSGBKTI').

    Returns:
        list of dict: Each dictionary contains constituent information such as 'SYMBOL', 'NAME', 'LDCP', 'CURRENT',
                      'CHANGE', 'CHANGE (%)', 'IDX WTG (%)', 'IDX POINT', 'VOLUME', 'FREEFLOAT (M)', 'MARKET CAP (M)'.
                      Returns None if the fetch fails.

    Example Return:
        [
            {
                "SYMBOL": "BAFL",
                "NAME": "Bank Alfalah Limited",
                "LDCP": 66.98,
                "CURRENT": 67.86,
                "CHANGE": 0.88,
                "CHANGE (%)": 1.31,
                "IDX WTG (%)": 15.66,
                "IDX POINT": 48.55,
                "VOLUME": 278476,
                "FREEFLOAT (M)": 1311,
                "MARKET CAP (M)": 88984
            },
            ...
        ]
    """
    url = f"https://dps.psx.com.pk/indices/{index_symbol}"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://dps.psx.com.pk/indices",
        "Connection": "keep-alive"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index constituents '{index_symbol}': {e}")
        return None

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tbl'})
        if not table:
            logging.error(f"No table found for index constituents '{index_symbol}'.")
            return None

        # Extract table headers
        headers = []
        for th in table.find('thead').find_all('th'):
            header_text = th.get_text(strip=True)
            # Normalize header names
            header_text = re.sub(r'\s+', ' ', header_text)
            headers.append(header_text)

        # Extract table rows
        constituents = []
        for tr in table.find('tbody').find_all('tr'):
            cols = tr.find_all('td')
            if len(cols) != len(headers):
                continue  # Skip rows that don't match header length
            row = {}
            for header, td in zip(headers, cols):
                # Handle specific columns
                if header == "SYMBOL":
                    symbol = td.find('strong').get_text(strip=True)
                    row["SYMBOL"] = symbol
                elif header == "NAME":
                    name = td.get_text(strip=True)
                    row["NAME"] = name
                else:
                    # Extract numerical data
                    data_order = td.get('data-order')
                    if data_order is not None:
                        try:
                            if header in ["LDCP", "CURRENT", "CHANGE", "CHANGE (%)", "IDX WTG (%)", "IDX POINT", "FREEFLOAT (M)", "MARKET CAP (M)"]:
                                value = float(data_order)
                            elif header == "VOLUME":
                                value = int(data_order.replace(',', ''))
                            else:
                                value = td.get_text(strip=True)
                        except ValueError:
                            value = td.get_text(strip=True)
                    else:
                        value = td.get_text(strip=True)
                    row[header] = value
            constituents.append(row)

        logging.info(f"Retrieved {len(constituents)} constituents for index '{index_symbol}'.")
        return constituents
    except Exception as e:
        logging.error(f"Error parsing HTML for index constituents '{index_symbol}': {e}")
        return None



def get_kse_index_historical_data(index_symbol):
    """
    Fetches historical data for a given index from the PSX Timeseries API.

    Args:
        index_symbol (str): The symbol identifier for the index (e.g., 'ACI').

    Returns:
        dict: Contains 'status', 'message', and 'data' where 'data' is a list of lists.
              Each inner list contains [timestamp, price, volume].
              Returns None if the fetch fails.

    Example Return:
        {
            "status": 1,
            "message": "",
            "data": [
                [1728648858, 12775.6255, 100],
                [1728648828, 12775.6255, 101],
                ...
            ]
        }
    """
    url = f"https://dps.psx.com.pk/timeseries/eod/{index_symbol}"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index historical data '{index_symbol}': {e}")
        return None

    try:
        data = response.json()
        if not isinstance(data, dict) or 'data' not in data:
            logging.error(f"Unexpected JSON structure for index historical data '{index_symbol}'.")
            return None
        logging.info(f"Retrieved {len(data['data'])} historical data points for index '{index_symbol}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for index historical data '{index_symbol}'.")
        return None




def get_kse_index_symbols(index_symbols):
    """
    Fetches constituents for a list of indices

    Args:
        index_symbols (list of str): List of index symbol identifiers (e.g., ['JSGBKTI', 'ACI']).

    Returns:
        dict: Keys are index symbols, and values are lists of constituent dictionaries.
              Returns None for any index symbol that fails to fetch.

    Example Return:
        {
            "JSGBKTI": [
                {
                    "SYMBOL": "BAFL",
                    "NAME": "Bank Alfalah Limited",
                    "LDCP": 66.98,
                    "CURRENT": 67.86,
                    "CHANGE": 0.88,
                    "CHANGE (%)": 1.31,
                    "IDX WTG (%)": 15.66,
                    "IDX POINT": 48.55,
                    "VOLUME": 278476,
                    "FREEFLOAT (M)": 1311,
                    "MARKET CAP (M)": 88984
                },
                ...
            ],
            "ACI": [
                ...
            ]
        }
    """
    constituents_data = {}
    for symbol in index_symbols:
        constituents = get_kse_ticker_detail(symbol)
        if constituents is not None:
            constituents_data[symbol] = constituents
        else:
            logging.warning(f"Failed to fetch constituents for index '{symbol}'.")
    return constituents_data





def get_all_kse_indices_historical_data(index_symbols):
    """
    Fetches historical data for a list of index symbols.

    Args:
        index_symbols (list of str): List of index symbol identifiers (e.g., ['ACI']).

    Returns:
        dict: Keys are index symbols, and values are historical data dictionaries.
              Returns None for any index symbol that fails to fetch.

    Example Return:
        {
            "ACI": {
                "status": 1,
                "message": "",
                "data": [
                    [1728648858, 12775.6255, 100],
                    [1728648828, 12775.6255, 101],
                    ...
                ]
            },
            ...
        }
    """
    historical_data = {}
    for symbol in index_symbols:
        data = get_kse_index_historical_data(symbol)
        if data is not None:
            historical_data[symbol] = data
        else:
            logging.warning(f"Failed to fetch historical data for index '{symbol}'.")
    return historical_data





def get_kse_symbols():
    """
    Fetches the list of all stock symbols from the PSX Symbols API.

    Args:
        None

    Returns:
        list of dict: Each dictionary contains index symbol information such as 'symbol', 'name', 'sectorName', 'isETF', 'isDebt'.
                      Returns None if the fetch fails.

    Example Return:
        [
            {
                "symbol": "AKBLTFC6",
                "name": "Askari Bank(TFC6)",
                "sectorName": "BILLS AND BONDS",
                "isETF": False,
                "isDebt": True
            },
            ...
        ]
    """
    url = "https://dps.psx.com.pk/symbols"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,ps;q=0.8",
        "Sec-CH-UA": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for index symbols: {e}")
        return None

    try:
        data = response.json()
        if not isinstance(data, list):
            logging.error("Unexpected JSON structure for index symbols: Expected a list of records.")
            return None
        logging.info(f"Retrieved {len(data)} index symbols.")
        return data
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON response for index symbols.")
        return None



def get_stock_data(ticker, date_from, date_to):
    """
    Fetches stock data from the Investors Lounge API for a given ticker and date range.
    
    Args:
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
        logging.info(f"Retrieved {len(data)} records for ticker '{ticker}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for ticker '{ticker}'.")
        return None
    


async def synchronize_tickers_async(conn, tickers, summary, progress_callback=None):
    """
    Asynchronously synchronizes tickers.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        tickers (list): List of ticker symbols.
        summary (dict): Summary dictionary to update.
        progress_callback (callable): Optional callback to update progress.
    """
    total_tickers = len(tickers)
    data_total_added = 0
    errors = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, ticker in enumerate(tickers, start=1):
            latest_date = get_latest_date_for_ticker(conn, ticker)
            # Convert latest_date to 'DD MMM YYYY' format
            if latest_date:
                latest_date_obj = datetime.strptime(latest_date, "%Y-%m-%d")
                date_from = latest_date_obj.strftime("%d %b %Y")
            else:
                # Define a default start date if no records exist
                date_from = "01 Jan 2000"
            date_to = datetime.today().strftime("%d %b %Y")

            task = asyncio.ensure_future(
                async_get_stock_data(session, ticker, date_from, date_to)
            )
            tasks.append((ticker, task, idx, total_tickers))

        for ticker, task, idx, total in tasks:
            data = await task
            if data:
                # Insert data into the database using a ThreadPoolExecutor to avoid blocking
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as pool:
                    success, records_added = await loop.run_in_executor(
                        pool, insert_ticker_data_into_db, conn, data, ticker
                    )
                if success:
                    data_total_added += records_added
                    logging.info(f"Added {records_added} new records for ticker '{ticker}'.")
                else:
                    error_msg = f"Failed to insert data for ticker '{ticker}'."
                    errors.append(error_msg)
                    logging.error(error_msg)
            else:
                logging.info(f"No new data fetched for ticker '{ticker}'.")

            # Update progress if callback is provided
            if progress_callback:
                progress_callback(idx, total)

    summary['tickers']['success'] = True
    summary['tickers']['records_added'] = data_total_added
    summary['tickers']['message'] = f"Successfully synchronized tickers with {data_total_added} new records added."
    if errors:
        summary['tickers']['message'] += f" Encountered errors with {len(errors)} tickers."
        summary['tickers']['errors'] = errors




    
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



def get_stocks_by_index(conn):
    """
    Retrieves a comma-separated list of stocks for each index from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        dict: A dictionary where the keys are indices and the values are comma-separated stock symbols.
    """
    try:
        cursor = conn.cursor()
        # SQL query to get distinct symbols for each index
        query = """
            SELECT "LISTED_IN", GROUP_CONCAT(DISTINCT SYMBOL, ', ') as symbols
            FROM MarketWatch
            GROUP BY "LISTED_IN"
            ORDER BY "LISTED_IN";
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Create a dictionary to store the results
        index_to_stocks = {row[0]: row[1] for row in rows}

        logging.info(f"Retrieved stocks for {len(index_to_stocks)} indices.")
        return index_to_stocks

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve stocks by index: {e}")
        return {}




def get_stocks_of_sector(conn, sector):
    """
    Retrieves all symbols for a given sector from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        sector (str): The name of the sector to filter by.

    Returns:
        list: A list of symbols that belong to the specified sector.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT DISTINCT SYMBOL FROM MarketWatch
            WHERE SECTOR = ?;
        """
        cursor.execute(query, (sector,))
        rows = cursor.fetchall()

        # Extract symbols from the result and return as a list
        symbols = [row[0] for row in rows]
        logging.info(f"Retrieved {len(symbols)} symbols for sector '{sector}'.")
        return symbols

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve symbols for sector '{sector}': {e}")
        return []



def get_top_advancers(conn):
    """
    Retrieves the top 10 stocks with the highest percentage change from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of dictionaries representing the top 10 advancers.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT SYMBOL, SECTOR, "LISTED_IN", "CHANGE (%)", CURRENT, VOLUME
            FROM MarketWatch
            ORDER BY "CHANGE (%)" DESC
            LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        advancers = []
        for row in rows:
            advancers.append({
                'SYMBOL': row[0],
                'SECTOR': row[1],
                'LISTED_IN': row[2],
                'CHANGE (%)': row[3],
                'CURRENT': row[4],
                'VOLUME': row[5]
            })
        return advancers

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve top advancers: {e}")
        return []


def get_top_decliners(conn):
    """
    Retrieves the top 10 stocks with the lowest percentage change from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of dictionaries representing the top 10 decliners.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT SYMBOL, SECTOR, "LISTED_IN", "CHANGE (%)", CURRENT, VOLUME
            FROM MarketWatch
            ORDER BY "CHANGE (%)" ASC
            LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        decliners = []
        for row in rows:
            decliners.append({
                'SYMBOL': row[0],
                'SECTOR': row[1],
                'LISTED_IN': row[2],
                'CHANGE (%)': row[3],
                'CURRENT': row[4],
                'VOLUME': row[5]
            })
        return decliners

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve top decliners: {e}")
        return []


def get_top_active(conn):
    """
    Retrieves the top 10 stocks with the highest volume from the MarketWatch table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of dictionaries representing the top 10 most active stocks by volume.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT SYMBOL, SECTOR, "LISTED_IN", VOLUME, CURRENT, "CHANGE (%)"
            FROM MarketWatch
            ORDER BY VOLUME DESC
            LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        most_active = []
        for row in rows:
            most_active.append({
                'SYMBOL': row[0],
                'SECTOR': row[1],
                'LISTED_IN': row[2],
                'VOLUME': row[3],
                'CURRENT': row[4],
                'CHANGE (%)': row[5]
            })
        return most_active

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve top active stocks: {e}")
        return []


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
                'DEFAULTER': symbol in defaulter_symbols,  # Set True if in defaulters, else False
                'DEFAULTING_CLAUSE': None  # Initialize as None; will be updated if defaulter
            })
        else:
            # If the symbol is not in market_data, add it with default values
            symbol_to_data[symbol] = {
                'SYMBOL': symbol,
                'SECTOR': listing['SECTOR'],
                'LISTED_IN': listing['LISTED_IN'],
                'CHANGE (%)': None,
                'CURRENT': None,
                'VOLUME': None,
                'NAME': listing['NAME'],
                'SHARES': listing['SHARES'],
                'FREE FLOAT': listing['FREE FLOAT'],
                'DEFAULTER': symbol in defaulter_symbols,
                'DEFAULTING_CLAUSE': None
            }

    # Update with defaulters data (if they exist, they overwrite the defaulter flag and add defaulting clause)
    for defaulter in defaulters_data:
        symbol = defaulter['SYMBOL']
        if symbol in symbol_to_data:
            symbol_to_data[symbol].update({
                'DEFAULTER': True,  # Mark as defaulter
                'DEFAULTING_CLAUSE': defaulter.get('DEFAULTING CLAUSE', None)
            })
        else:
            # Add missing defaulters directly
            symbol_to_data[symbol] = {
                'SYMBOL': symbol,
                'SECTOR': defaulter['SECTOR'],
                'LISTED_IN': defaulter['LISTED_IN'],
                'CHANGE (%)': None,  # Unknown
                'CURRENT': None,  # Unknown
                'VOLUME': None,  # Unknown
                'NAME': defaulter['NAME'],
                'SHARES': defaulter['SHARES'],
                'FREE FLOAT': defaulter['FREE FLOAT'],
                'DEFAULTER': True,
                'DEFAULTING_CLAUSE': defaulter['DEFAULTING CLAUSE']
            }

    return list(symbol_to_data.values())



# # for partial ticker sync
# def insert_ticker_data_into_db(conn, data, ticker, batch_size=100):
#     """
#     Inserts the list of stock data into the SQLite database in batches.
#     Returns a tuple of (success, records_added, errors).

#     Args:
#         conn (sqlite3.Connection): SQLite database connection.
#         data (list): List of dictionaries containing ticker data.
#         ticker (str): The ticker symbol.
#         batch_size (int): Number of records to insert per batch.

#     Returns:
#         tuple: (success (bool), records_added (int), errors (list))
#     """
#     logger = logging.getLogger(__name__)
#     errors = []
#     records_added = 0
#     try:
#         cursor = conn.cursor()
#         insert_query = """
#             INSERT OR REPLACE INTO Ticker 
#             (Ticker, Date, Open, High, Low, Close, Change, "Change (%)", Volume) 
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
#         """

#         # Prepare data for insertion
#         data_to_insert = []
#         for idx, record in enumerate(data, start=1):
#             try:
#                 logger.debug(f"Processing record {idx}/{len(data)} for ticker '{ticker}': {record}")

#                 # Clean and validate the date field
#                 date_str = record.get('Date')
#                 formatted_date = clean_date(date_str)
#                 if not formatted_date:
#                     error_msg = f"Data Preparation: Invalid or missing 'Date' field in record {idx} for ticker '{ticker}'. Skipping record."
#                     logger.error(error_msg)
#                     errors.append(error_msg)
#                     continue

#                 # Extract and clean numeric fields
#                 open_ = clean_numeric(record.get('Open'), 'Open')
#                 high = clean_numeric(record.get('High'), 'High')
#                 low = clean_numeric(record.get('Low'), 'Low')
#                 close = clean_numeric(record.get('Close'), 'Close')
#                 change = clean_numeric(record.get('Change'), 'Change')
#                 change_p = clean_numeric(record.get('Change (%)'), 'Change (%)')
#                 volume = clean_numeric(record.get('Volume'), 'Volume')

#                 # Convert fields to appropriate data types
#                 try:
#                     open_val = float(open_)
#                     high_val = float(high)
#                     low_val = float(low)
#                     close_val = float(close)
#                     change_val = float(change)
#                     change_p_val = round(float(change_p), 2)  # Round to two decimal places
#                     volume_val = int(float(volume))  # Ensure volume is an integer
#                 except ValueError as ve:
#                     error_msg = (
#                         f"Data Preparation: Data type conversion error for ticker '{ticker}' on date '{formatted_date}'. "
#                         f"Failed to convert fields. Details: {ve}. "
#                         f"Cleaned data - Open: '{open_}', High: '{high}', Low: '{low}', "
#                         f"Close: '{close}', Change: '{change}', ChangeP: '{change_p}', Volume: '{volume}'. "
#                         f"Original record: {record}."
#                     )
#                     logger.error(error_msg)
#                     errors.append(error_msg)
#                     continue

#                 # Append the cleaned and converted data as a tuple
#                 data_to_insert.append((ticker, formatted_date, open_val, high_val, low_val, close_val, change_val, change_p_val, volume_val))

#             except Exception as e:
#                 error_msg = f"Unexpected error while processing record {idx} for ticker '{ticker}': {e}. Skipping record."
#                 logger.exception(error_msg)
#                 errors.append(error_msg)
#                 continue

#         if not data_to_insert:
#             warning_msg = f"No valid records to insert for ticker '{ticker}'."
#             logger.warning(warning_msg)
#             errors.append(warning_msg)
#             return False, records_added, errors

#         # Insert data in batches
#         total_records = len(data_to_insert)
#         logger.info(f"Starting database insertion for ticker '{ticker}' with {total_records} records.")

#         for i in range(0, total_records, batch_size):
#             batch = data_to_insert[i:i + batch_size]
#             try:
#                 cursor.executemany(insert_query, batch)
#                 conn.commit()
#                 records_added += len(batch)  # Since executemany doesn't reliably return rowcount
#                 logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records for ticker '{ticker}'.")
#             except sqlite3.Error as e:
#                 error_msg = f"Database insertion error for ticker '{ticker}' in batch {i//batch_size + 1}: {e}."
#                 logger.error(error_msg)
#                 errors.append(error_msg)
#                 continue

#             # Log the first three inserted records in this batch for verification
#             if i == 0 and len(batch) >= 3:
#                 logger.debug(f"First 3 records inserted for ticker '{ticker}': {batch[:3]}")

#         logger.info(f"Successfully inserted/updated {records_added} records for ticker '{ticker}'.")
#         return True, records_added, errors

#     except Exception as e:
#         error_msg = f"Failed to insert data into database for ticker '{ticker}': {e}"
#         logger.exception(error_msg)
#         errors.append(error_msg)
#         return False, records_added, errors




# # for synchronize database
# def insert_ticker_data_into_db(conn, data, ticker, batch_size=100):
#     """
#     Inserts the list of stock data into the SQLite database in batches.
#     Returns a tuple of (success, records_added, errors).

#     Args:
#         conn (sqlite3.Connection): SQLite database connection.
#         data (list): List of dictionaries containing ticker data.
#         ticker (str): The ticker symbol.
#         batch_size (int): Number of records to insert per batch.

#     Returns:
#         tuple: (success (bool), records_added (int), errors (list))
#     """
#     errors = []
#     records_added = 0
#     try:
#         cursor = conn.cursor()
#         insert_query = """
#             INSERT OR REPLACE INTO Ticker 
#             (Ticker, Date, Open, High, Low, Close, Change, "Change (%)", Volume) 
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
#         """

#         # Prepare data for insertion
#         data_to_insert = []
#         for idx, record in enumerate(data, start=1):
#             try:
#                 logging.info(f"Processing record {idx}/{len(data)} for ticker '{ticker}': {record}")

#                 # Map 'Date_' to 'Date'
#                 date_str = record.get('Date_')
#                 if not date_str:
#                     error_msg = f"❌ 'Date_' field is missing in record {idx} for ticker '{ticker}'. Skipping record."
#                     logging.error(error_msg)
#                     errors.append(error_msg)
#                     continue

#                 # Attempt to parse and format the date
#                 try:
#                     # Handle ISO format dates
#                     if 'T' in date_str:
#                         parsed_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
#                     else:
#                         # Handle other possible date formats if necessary
#                         parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
#                     formatted_date = parsed_date.strftime('%d %b %Y')
#                     logging.info(f"Formatted date for ticker '{ticker}': {formatted_date}")
#                 except ValueError as ve:
#                     error_msg = f"❌ Date format error for ticker '{ticker}' with date '{date_str}': {ve}. Skipping record."
#                     logging.error(error_msg)
#                     errors.append(error_msg)
#                     continue

#                 # Extract and clean numeric fields
#                 open_ = record.get('Open')
#                 high = record.get('High')
#                 low = record.get('Low')
#                 close = record.get('Close')
#                 change = record.get('Change')
#                 change_p = record.get('ChangeP')  # Assuming 'ChangeP' is always present
#                 volume = record.get('Volume')

#                 # Log extracted fields
#                 logging.info(f"Extracted fields for ticker '{ticker}': Open={open_}, High={high}, Low={low}, Close={close}, Change={change}, ChangeP={change_p}, Volume={volume}")

#                 # Data Cleaning: Ensure numeric fields are correctly formatted
#                 def clean_numeric(value, field_name):
#                     if isinstance(value, str):
#                         cleaned_value = value.replace(',', '').strip()
#                         logging.debug(f"Cleaned '{field_name}': '{value}' -> '{cleaned_value}'")
#                         return cleaned_value
#                     return value

#                 open_ = clean_numeric(open_, 'Open')
#                 high = clean_numeric(high, 'High')
#                 low = clean_numeric(low, 'Low')
#                 close = clean_numeric(close, 'Close')
#                 change = clean_numeric(change, 'Change')
#                 change_p = clean_numeric(change_p, 'ChangeP')
#                 volume = clean_numeric(volume, 'Volume')

#                 # Convert fields to appropriate data types
#                 try:
#                     open_ = float(open_)
#                     high = float(high)
#                     low = float(low)
#                     close = float(close)
#                     change = float(change)
#                     change_p = round(float(change_p), 2)  # Round to two decimal places
#                     volume = int(float(volume))  # Ensure volume is an integer
#                 except ValueError as ve:
#                     error_msg = f"❌ Data type conversion error for ticker '{ticker}' on date '{formatted_date}': {ve}. Skipping record."
#                     logging.error(error_msg)
#                     errors.append(error_msg)
#                     continue

#                 logging.info(f"Converted fields for ticker '{ticker}': Open={open_}, High={high}, Low={low}, Close={close}, Change={change}, ChangeP={change_p}, Volume={volume}")

#                 # Append the cleaned and converted data as a tuple
#                 data_to_insert.append((ticker, formatted_date, open_, high, low, close, change, change_p, volume))

#             except Exception as e:
#                 error_msg = f"❌ Unexpected error while processing record {idx} for ticker '{ticker}': {e}. Skipping record."
#                 logging.exception(error_msg)
#                 errors.append(error_msg)
#                 continue

#         if not data_to_insert:
#             warning_msg = f"⚠️ No valid records to insert for ticker '{ticker}'."
#             logging.warning(warning_msg)
#             errors.append(warning_msg)
#             return False, records_added, errors

#         # Insert data in batches
#         total_records = len(data_to_insert)
#         logging.info(f"Starting database insertion for ticker '{ticker}' with {total_records} records.")

#         for i in range(0, total_records, batch_size):
#             batch = data_to_insert[i:i + batch_size]
#             try:
#                 cursor.executemany(insert_query, batch)
#                 conn.commit()
#                 records_added += cursor.rowcount
#                 logging.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records for ticker '{ticker}'.")
#             except sqlite3.Error as e:
#                 error_msg = f"❌ Database insertion error for ticker '{ticker}' in batch {i//batch_size + 1}: {e}."
#                 logging.error(error_msg)
#                 errors.append(error_msg)
#                 continue

#             # Log the first three inserted records in this batch for verification
#             if i == 0 and len(batch) >= 3:
#                 logging.debug(f"First 3 records inserted for ticker '{ticker}': {batch[:3]}")

#         logging.info(f"✅ Successfully inserted/updated {records_added} records for ticker '{ticker}'.")
#         return True, records_added, errors

#     except Exception as e:
#         error_msg = f"❌ Failed to insert data into database for ticker '{ticker}': {e}"
#         logging.exception(error_msg)
#         errors.append(error_msg)
#         return False, records_added, errors


# for synchronize database
def get_stock_data(ticker, date_from, date_to):
    """
    Fetches stock data from the Investors Lounge API for a given ticker and date range.

    Args:
        ticker (str): The stock ticker symbol.
        date_from (str): Start date in 'DD MMM YYYY' format.
        date_to (str): End date in 'DD MMM YYYY' format.

    Returns:
        list: A list of dictionaries containing stock data or None if failed.
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
        logging.debug(f"Raw data fetched for ticker '{ticker}': {data[:2]}...")  # Log first 2 records for brevity
        logging.info(f"Retrieved {len(data)} records for ticker '{ticker}'.")
        return data
    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON response for ticker '{ticker}'.")
        return None