import sqlite3
import pandas as pd
import os
import random

def get_all_databases(folder_path='data'):
    """
    Finds all SQLite database files in the specified folder.
    
    Args:
        folder_path (str): Path to the folder where database files are located.
    
    Returns:
        list: List of database file paths.
    """
    return [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.db')]

def get_table_structure(conn):
    """
    Retrieves the structure of all tables in the database.
    
    Args:
        conn (sqlite3.Connection): SQLite connection object.
    
    Returns:
        dict: Dictionary containing table names as keys and their structure as values.
    """
    table_structure = {}
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table_name in tables:
        table_name = table_name[0]
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        table_structure[table_name] = columns
    
    return table_structure

def sample_rows_from_all_tables(conn, sample_size=5):
    """
    Randomly samples rows from all tables in the database.
    
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        sample_size (int): Number of rows to sample from each table.
    
    Returns:
        dict: Dictionary containing table names as keys and sampled rows as DataFrames.
    """
    sampled_data = {}
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table_name in tables:
        table_name = table_name[0]
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            sampled_data[table_name] = df.sample(min(sample_size, len(df)), random_state=random.randint(0, 100))
        else:
            sampled_data[table_name] = pd.DataFrame()
    
    return sampled_data

def save_results_to_file(structure, sampled_data, db_name, output_file='database_structure_and_samples.txt'):
    """
    Saves the table structure and sampled data to a file.
    
    Args:
        structure (dict): Dictionary containing the structure of all tables.
        sampled_data (dict): Dictionary containing sampled data for each table.
        db_name (str): Name of the database.
        output_file (str): Path to the output file.
    """
    with open(output_file, 'a') as f:  # Append to file
        f.write(f"\n\nDatabase: {db_name}\n")
        f.write("=" * (11 + len(db_name)) + "\n")
        f.write("Table Structure:\n")
        for table, columns in structure.items():
            f.write(f"\nTable: {table}\n")
            f.write("Columns:\n")
            for column in columns:
                f.write(f" - {column}\n")
        
        f.write("\n\nSampled Data from Tables:\n")
        for table, df in sampled_data.items():
            f.write(f"\nTable: {table}\n")
            if not df.empty:
                f.write(df.to_string(index=False))
            else:
                f.write("No data available.")
            f.write("\n\n")

def process_all_databases(folder_path='data', sample_size=5, output_file='database_structure_and_samples.txt'):
    """
    Processes all SQLite databases in the specified folder.
    
    Args:
        folder_path (str): Path to the folder where database files are located.
        sample_size (int): Number of rows to sample from each table.
        output_file (str): Path to the output file for saving results.
    """
    # Clear the output file before starting
    with open(output_file, 'w') as f:
        f.write("Database Analysis Report\n")
        f.write("=" * 24 + "\n")

    # Get all database files
    db_files = get_all_databases(folder_path)
    if not db_files:
        print(f"No database files found in the folder '{folder_path}'.")
        return
    
    # Process each database
    for db_path in db_files:
        db_name = os.path.basename(db_path)
        try:
            conn = sqlite3.connect(db_path)
            
            # Get the structure of all tables
            table_structure = get_table_structure(conn)
            
            # Sample rows from all tables
            sampled_data = sample_rows_from_all_tables(conn, sample_size)
            
            # Save the results to a file
            save_results_to_file(table_structure, sampled_data, db_name, output_file)
            
            # Display a message
            print(f"Processed database: {db_name}")
            
            conn.close()
        except sqlite3.Error as e:
            print(f"Failed to process the database '{db_name}': {e}")

if __name__ == "__main__":
    process_all_databases()
