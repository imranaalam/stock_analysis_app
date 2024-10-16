import requests
import pandas as pd
from bs4 import BeautifulSoup

def fetch_psx_historical(date):
    url = "https://dps.psx.com.pk/historical"
    
    # Headers based on your provided fetch request
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
    
    # The body with the date parameter
    data = {
        'date': date
    }

    # Making the POST request
    response = requests.post(url, headers=headers, data=data)

    # Checking if the request was successful
    if response.status_code == 200:
        return response.text  # Return the HTML content of the response
    else:
        raise Exception(f"Error: Unable to fetch data, status code: {response.status_code}")

def parse_html_to_df(html_data):
    # Parsing the HTML using BeautifulSoup
    soup = BeautifulSoup(html_data, 'html.parser')

    # Extracting the table rows
    rows = soup.find_all('tr')

    # Extracting table headers
    headers = [header.get_text() for header in rows[0].find_all('th')]

    # Initializing an empty list for storing table data
    table_data = []

    # Loop through each row and extract the cell data
    for row in rows[1:]:
        cols = row.find_all('td')
        data = [col.get_text() for col in cols]
        table_data.append(data)

    # Creating a DataFrame using the extracted data
    df = pd.DataFrame(table_data, columns=headers)

    # Displaying the head of the DataFrame
    return df.head()

# Example usage
date_input = "2024-10-15"
html_data = fetch_psx_historical(date_input)  # Fetch the HTML for the specific date
df_head = parse_html_to_df(html_data)  # Parse and display the head of the DataFrame
print(df_head)
