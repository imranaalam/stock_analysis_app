import pandas as pd
import logging
from datetime import datetime, timedelta

def format_date(date_input, output_format="%Y-%m-%d"):
    """
    Converts a date string to the desired format.
    
    Args:
        date_input (str or datetime-like): The date to format.
        output_format (str): The desired format of the date string.
    
    Returns:
        str: The formatted date string, or None if formatting fails.
    """
    try:
        # Let pandas infer the date format and handle time if it's present
        date = pd.to_datetime(date_input, errors='coerce')
        
        # Return the formatted date (ignores the time component)
        return date.strftime(output_format)
    
    except Exception as e:
        logging.error(f"Date formatting failed for '{date_input}': {e}")
        return None



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


def clean_date(date_str: str) -> str:
    """
    Cleans and formats the date string to 'dd MMM YYYY'.
    
    Args:
        date_str (str): The original date string.
    
    Returns:
        str: Formatted date string or None if invalid.
    """
    if not date_str:
        return None
    try:
        # Try parsing common date formats
        for fmt in ('%Y-%m-%d', '%d %b %Y', '%Y-%m-%dT%H:%M:%S'):
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%d %b %Y')
            except ValueError:
                continue
        # If none of the formats match, return None
        return None
    except Exception:
        return None


def clean_numeric(value, field_name: str):
    """
    Cleans numeric fields by removing commas and stripping whitespace.
    
    Args:
        value: The original value.
        field_name (str): The name of the field being cleaned (for logging purposes).
    
    Returns:
        float or int: Cleaned numeric value.
    """
    try:
        if isinstance(value, str):
            cleaned_value = value.replace(',', '').strip()
            return cleaned_value
        return value
    except Exception as e:
        logging.debug(f"Error cleaning numeric field '{field_name}': {e}")
        return value
    


def get_last_five_working_days(reference_date=None, num_days=5):
    """
    Retrieves the last 'num_days' working days before the reference_date.
    
    Args:
        reference_date (datetime or str, optional): The date to calculate from. Defaults to today.
        num_days (int, optional): Number of working days to retrieve. Defaults to 5.
    
    Returns:
        list: List of dates in 'YYYY-MM-DD' format.
    """
    if reference_date:
        if isinstance(reference_date, str):
            reference_date = datetime.strptime(reference_date, '%Y-%m-%d')
    else:
        reference_date = datetime.now()
    
    # Use pandas bdate_range to get business days (Monday-Friday)
    end_date = reference_date - timedelta(days=1)  # Start from the day before the reference date
    start_date = end_date - timedelta(days=num_days*2)  # Approximate start date
    
    business_days = pd.bdate_range(start=start_date, end=end_date)
    last_five = business_days[-num_days:]
    
    return [date.strftime('%Y-%m-%d') for date in last_five]
