"""
This script processes ground data from a SQLite database, fetches corresponding satellite data
from the Digital Earth Australia STAC catalog, and updates the database with processed results.
It supports both sequential and parallel processing modes and can initialize the database from
a CSV file if needed.

Dependencies:
    - sqlite3: For database operations
    - pandas: For data manipulation
    - pystac_client: For accessing STAC catalog
    - odc.stac: For loading satellite data
"""

import sqlite3
import pandas as pd
import pystac_client
import odc.stac

from multiprocessing import Pool, Manager
from functools import partial

import sys
import argparse

def get_all_years(cursor):
    """
    Retrieve all distinct years from the ground_data table.
    
    Args:
        cursor: SQLite database cursor
        
    Returns:
        list: List of years as strings
    """
    sql_query = '''
    SELECT DISTINCT strftime('%Y', month) as year FROM ground_data
    ORDER BY year
    '''
    cursor.execute(sql_query)
    return [row[0] for row in cursor.fetchall()]

def get_month_lat_long(cursor, year):
    """
    Retrieve records that need processing for a given year.
    
    Args:
        cursor: SQLite database cursor
        year (str): Year to process
        
    Returns:
        list: List of tuples containing (month, latitude, longitude, grid_id)
              for records with NAN values in processing columns
    """
    sql_query = '''
    SELECT month, lat, lon, grid_id 
    FROM ground_data 
    WHERE strftime('%Y', month) = ? 
    AND (bs_pc_10 IS 'NAN' OR bs_pc_50 IS 'NAN' OR bs_pc_90 IS 'NAN' 
    OR pv_pc_10 IS 'NAN' OR pv_pc_50 IS 'NAN' OR pv_pc_90 IS 'NAN' 
    OR npv_pc_10 IS 'NAN' OR npv_pc_50 IS 'NAN' OR npv_pc_90 IS 'NAN')
    '''
    cursor.execute(sql_query, (year,))
    return cursor.fetchall()

def column_exists(cursor, table_name, column_name):
    """
    Check if a column exists in a specified table.
    
    Args:
        cursor: SQLite database cursor
        table_name (str): Name of the table
        column_name (str): Name of the column to check
        
    Returns:
        bool: True if column exists, False otherwise
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    return column_name in columns

def update_ground_data(cursor, conn, data):
    """
    Update a record in the ground_data table with processed values.
    
    Args:
        cursor: SQLite database cursor
        conn: SQLite database connection
        data (dict): Dictionary containing values to update
        
    Raises:
        sqlite3.Error: If database operation fails
    """
    try:
        # SQL query to update all percentile columns for a specific grid_id and month
        cursor.execute('''
        UPDATE ground_data
        SET bs_pc_10 = ?, bs_pc_50 = ?, bs_pc_90 = ?, 
            pv_pc_10 = ?, pv_pc_50 = ?, pv_pc_90 = ?, 
            npv_pc_10 = ?, npv_pc_50 = ?, npv_pc_90 = ?
        WHERE grid_id = ? AND month = ?
        ''', (
            data['bs_pc_10'], data['bs_pc_50'], data['bs_pc_90'],
            data['pv_pc_10'], data['pv_pc_50'], data['pv_pc_90'],
            data['npv_pc_10'], data['npv_pc_50'], data['npv_pc_90'],
            data['grid_id'], data['time']
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error during update: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error during update: {e}")
        raise

def setup_database(db_path):
    """
    Initialize the database from CSV file and set up required columns.
    
    Args:
        db_path (str): Path to the SQLite database file
        
    Raises:
        FileNotFoundError: If CSV file is not found
        pd.errors.EmptyDataError: If CSV file is empty
        sqlite3.Error: If database operations fail
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Read and process CSV file
        try:
            print("Reading CSV file...")
            df = pd.read_csv('./Data/base_data_1986-2018.csv')
        except FileNotFoundError:
            print("Error: base_data_1986-2018.csv not found in Data directory")
            sys.exit(1)
        except pd.errors.EmptyDataError:
            print("Error: CSV file is empty")
            sys.exit(1)
        
        # Clean and transform data
        print("Processing CSV data...")
        df.drop(['time', 'index_right'], axis=1, inplace=True)
        df.month = pd.to_datetime(df.month, format='%Y-%m')
        df = df[df['month'].dt.year != 1986]
        
        # Create database table
        print("Creating database table...")
        df.to_sql('ground_data', conn, if_exists='replace', index=False)
        
        # Define and add new columns for percentile data
        columns_to_add = [
            'bs_pc_10 REAL DEFAULT NAN',  # Bare soil 10th percentile
            'bs_pc_50 REAL DEFAULT NAN',  # Bare soil 50th percentile
            'bs_pc_90 REAL DEFAULT NAN',  # Bare soil 90th percentile
            'pv_pc_10 REAL DEFAULT NAN',  # Photosynthetic vegetation 10th percentile
            'pv_pc_50 REAL DEFAULT NAN',  # Photosynthetic vegetation 50th percentile
            'pv_pc_90 REAL DEFAULT NAN',  # Photosynthetic vegetation 90th percentile
            'npv_pc_10 REAL DEFAULT NAN', # Non-photosynthetic vegetation 10th percentile
            'npv_pc_50 REAL DEFAULT NAN', # Non-photosynthetic vegetation 50th percentile
            'npv_pc_90 REAL DEFAULT NAN'  # Non-photosynthetic vegetation 90th percentile
        ]
        
        print("Adding percentile columns...")
        for column in columns_to_add:
            column_name = column.split()[0]
            if not column_exists(cursor, 'ground_data', column_name):
                cursor.execute(f'ALTER TABLE ground_data ADD COLUMN {column}')
        
        conn.commit()
        print("Database setup completed successfully")
        
    except sqlite3.Error as e:
        print(f"Database error during setup: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error during setup: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

def process_single_tile(data, catalog, year):
    """
    Process a single tile of data using the STAC catalog.
    
    Args:
        data (tuple): Contains (month, lat, lon, grid_id)
        catalog: STAC catalog client
        year (str): Year being processed
        
    Returns:
        dict: Processed data dictionary or None if processing fails
    """
    try:
        # Calculate bounding box for the tile
        bbox = [data[2] - 0.05, data[1] - 0.05, data[2] + 0.05, data[1] + 0.05]
        start_date = f"{data[0][:4]}-01-01"
        end_date = f"{data[0][:4]}-12-31"
        
        # Query STAC catalog
        query = catalog.search(
            bbox=bbox,
            collections=["ga_ls_fc_pc_cyear_3"],
            datetime=f"{start_date}/{end_date}",
        )
        
        items = list(query.items())
        if not items:
            print(f"No data found for {start_date}-{end_date}, Location: {data[1]}, {data[2]}")
            return None
        
        # Load and process data
        ds = odc.stac.load(
            items=items,
            crs="EPSG:3577",
            lat=(bbox[1], bbox[3]),
            lon=(bbox[0], bbox[2]),
            time=(start_date, end_date)
        )
        
        # Calculate means for all percentiles
        return {
            'time': data[0],
            'grid_id': data[3],
            'bs_pc_10': float(ds.bs_pc_10.mean().values),
            'bs_pc_50': float(ds.bs_pc_50.mean().values),
            'bs_pc_90': float(ds.bs_pc_90.mean().values),
            'pv_pc_10': float(ds.pv_pc_10.mean().values),
            'pv_pc_50': float(ds.pv_pc_50.mean().values),
            'pv_pc_90': float(ds.pv_pc_90.mean().values),
            'npv_pc_10': float(ds.npv_pc_10.mean().values),
            'npv_pc_50': float(ds.npv_pc_50.mean().values),
            'npv_pc_90': float(ds.npv_pc_90.mean().values)
        }
        
    except Exception as e:
        print(f"Error processing year {year}, location {data[1]}, {data[2]}: {e}")
        return None

def process_single_year(year, db_path):
    """
    Process all tiles for a given year.
    
    Args:
        year (str): Year to process
        db_path (str): Path to SQLite database
    """
    try:
        # Get data tiles that need processing
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        data_tiles = get_month_lat_long(cursor, year)
        cursor.close()
        conn.close()
        
        print(f"Processing {len(data_tiles)} tiles for year: {year}")
        
        # Initialize STAC catalog and configure AWS connection
        catalog = pystac_client.Client.open("https://explorer.dea.ga.gov.au/stac")
        odc.stac.configure_rio(
            cloud_defaults=True,
            aws={"aws_unsigned": True},
        )
        
        # Process each tile and update database
        for data in data_tiles:
            result = process_single_tile(data, catalog, year)
            if result:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                update_ground_data(cursor, conn, result)
                cursor.close()
                conn.close()
                
    except Exception as e:
        print(f"Error in process_single_year for year {year}: {e}")

def main():
    """
    Main function to orchestrate the data processing workflow.
    Handles command line arguments and initiates processing.
    """
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Process ground data with optional parallel processing')
    parser.add_argument('--first', action='store_true', help='Initialize database')
    parser.add_argument('--parallel', action='store_true', help='Enable parallel processing')
    parser.add_argument('--db-path', default='fire.db', help='Path to database file')
    args = parser.parse_args()
    
    try:
        # Initialize database if requested
        if args.first:
            print("Initializing database...")
            setup_database(args.db_path)
        
        # Get list of years to process
        conn = sqlite3.connect(args.db_path)
        cursor = conn.cursor()
        years = get_all_years(cursor)
        cursor.close()
        conn.close()
        
        # Process years either in parallel or sequentially
        if args.parallel:
            print("Running with parallel processing...")
            with Pool() as pool:
                pool.map(partial(process_single_year, db_path=args.db_path), years)
        else:
            print("Running with sequential processing...")
            for year in years:
                process_single_year(year, args.db_path)
                
    except Exception as e:
        print(f"Critical error in main: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()