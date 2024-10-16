import pandas as pd
import logging

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
