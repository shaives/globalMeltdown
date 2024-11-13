import sqlite3
import pandas as pd

import pystac_client
import odc.stac

import os

# List of columns to add
columns_to_add = [
	'bs_pc_10 REAL DEFAULT NAN',
	'bs_pc_50 REAL DEFAULT NAN',
	'bs_pc_90 REAL DEFAULT NAN',
	'pv_pc_10 REAL DEFAULT NAN',
	'pv_pc_50 REAL DEFAULT NAN',
	'pv_pc_90 REAL DEFAULT NAN',
	'npv_pc_10 REAL DEFAULT NAN',
	'npv_pc_50 REAL DEFAULT NAN',
	'npv_pc_90 REAL DEFAULT NAN'
]

#Get all tables
def get_all_tables(cursor):
    sql_query = """SELECT name FROM sqlite_master  
      WHERE type='table';"""
    cursor.execute(sql_query)
    return cursor.fetchall()

# Get column names and data types for each table
def get_table_info(cursor, table_name):
    cursor.execute(f'PRAGMA table_info({table_name})')
    return cursor.fetchall()

# function to get month, lat, long and grid id from database
def get_month_lat_long(cursor):
    # define how many entries you want or all (remove LIMIT 10)
    sql_query = '''SELECT month, lat, lon, grid_id from ground_data LIMIT 10'''
    cursor.execute(sql_query)
    return cursor.fetchall()

# Function to check if a column exists in a table
def column_exists(cursor, table_name, column_name):
	cursor.execute(f"PRAGMA table_info({table_name})")
	columns = [info[1] for info in cursor.fetchall()]
	return column_name in columns

# Function to check if a column exists in a table
def column_exists(cursor, table_name, column_name):
	cursor.execute(f"PRAGMA table_info({table_name})")
	columns = [info[1] for info in cursor.fetchall()]
	return column_name in columns

# function to update each row in the data base table
def update_ground_data(cursor, conn, data):
    cursor.execute('''
    UPDATE ground_data
    SET bs_pc_10 = ?, bs_pc_50 = ?, bs_pc_90 = ?, pv_pc_10 = ?, pv_pc_50 = ?, pv_pc_90 = ?, npv_pc_10 = ?, npv_pc_50 = ?, npv_pc_90 = ?
    WHERE grid_id = ? AND month = ?
    ''', (
        data['bs_pc_10'], data['bs_pc_50'], data['bs_pc_90'],
        data['pv_pc_10'], data['pv_pc_50'], data['pv_pc_90'],
        data['npv_pc_10'], data['npv_pc_50'], data['npv_pc_90'],
        data['grid_id'], data['time']
    ))

    # Commit the changes
    conn.commit()

if os.name == 'posix':
    db_path = '/data/oa3802fa25/GlobalMeltdown/fire.db'
else:
    db_path = 'fire.db'

# Connect to the appropriate database
conn = sqlite3.connect(db_path)

# Create a cursor object
cursor = conn.cursor()

# set this to True if you run it the first time, to create the database
first = False

if first == True:
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv('./Data/base_data_1986-2018.csv')

    df.drop('time', axis=1, inplace=True)
    df.drop('index_right', axis=1, inplace=True)
    df.month = pd.to_datetime(df.month, format='%Y-%m')
    df = df[df['month'].dt.year != 1986]

    # Write the DataFrame to the SQLite database
    df.to_sql('ground_data', conn, if_exists='replace', index=False)

    # Alter the table to add new columns if they don't already exist
    for column in columns_to_add:
        column_name = column.split()[0]
        if not column_exists(cursor, 'ground_data', column_name):
            cursor.execute(f'ALTER TABLE ground_data ADD COLUMN {column}')

    # Commit the changes
    conn.commit()

# fetch data from fire.db
cursor.execute('''SELECT * from ground_data LIMIT 10''')
cursor.fetchall()

tables = [table[0] for table in get_all_tables(cursor)]

# printing all tables and datatypes of fire.db
for table in tables:
    print(f"Table: {table}")
    for column in get_table_info(cursor, table):
        print(f"Column: {column[1]}, Type: {column[2]}")
    print("\n")

# loading catalog from database
catalog = pystac_client.Client.open("https://explorer.dea.ga.gov.au/stac")

# configure the AWS connection
odc.stac.configure_rio(
    cloud_defaults=True,
    aws={"aws_unsigned": True},
)

# get information from database
data_tiles = get_month_lat_long(cursor)

# print length of data
print(len(data_tiles))

# for each dataset, get the data from AWS
for data in data_tiles:

    bbox = [data[2] - 0.05, data[1] - 0.05, data[2] + 0.05, data[1] + 0.05]

    start_date = f"{data[0][:4]}-01-01"
    end_date = f"{data[0][:4]}-12-31"

    print(f"Searching for data in {start_date} to {end_date} for lat: {data[1]} and long: {data[2]}")

    # Set product ID as the STAC "collection"
    collections = ["ga_ls_fc_pc_cyear_3"]

    try:
        # Build a query with the parameters above
        query = catalog.search(
            bbox=bbox,
            collections=collections,
            datetime=f"{start_date}/{end_date}",
        )

        if len(list(query.items())) == 0:
            print("No data found")
            continue

    except Exception as e:
        
        print(f"An error occurred: {e}")
        continue

    # Search the STAC catalog for all items matching the query
    items = list(query.items())
    print(f"Found: {len(items):d} datasets")

    # Load the data using the odc.stac module
    ds = odc.stac.load(
                items=items,
                crs="EPSG:3577",
                lat=(bbox[1], bbox[3]),
                lon=(bbox[0], bbox[2]),
                time=(start_date, end_date))
    
    data_entry = {
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

    update_ground_data(cursor, conn, data_entry)