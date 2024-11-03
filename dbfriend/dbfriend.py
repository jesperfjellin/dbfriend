#!/usr/bin/env python

import argparse
import os
import sys
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
import logging
import getpass
from collections import defaultdict
from tqdm import tqdm

def parse_arguments():
    parser = argparse.ArgumentParser(description='Load spatial data into PostGIS with compatibility checks.')

    # Positional arguments
    parser.add_argument('dbuser', help='Database user')
    parser.add_argument('filepath', help='Path to data files')

    # Optional arguments with default values
    parser.add_argument('extension', nargs='?', default=None, help='File extension to process (e.g., .shp, .geojson). If not provided, all supported spatial files will be processed.')
    parser.add_argument('--dbname', default='dbfriend', help='Database name (default: dbfriend)')
    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', default='5432', help='Database port (default: 5432)')
    parser.add_argument('--password', help='Database password (will prompt if not provided)')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing tables without prompting.')
    parser.add_argument('--rename-geom', action='store_true', help='Automatically rename geometry columns to "geom" without prompting.')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    return parser.parse_args()

def connect_db(dbname, dbuser, host, port, password):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=dbuser,
            host=host,
            port=port,
            password=password
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        sys.exit(1)

def get_existing_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def create_spatial_index(conn, table_name, geom_column='geom'):
    cursor = conn.cursor()
    index_name = f"{table_name}_{geom_column}_idx"
    try:
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS "{index_name}"
            ON "{table_name}"
            USING GIST ("{geom_column}");
        """)
        conn.commit()
        logging.info(f"Spatial index '{index_name}' created on table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error creating spatial index on '{table_name}': {e}")
        conn.rollback()
    finally:
        cursor.close()

def get_db_geometry_column(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT f_geometry_column
        FROM geometry_columns
        WHERE f_table_schema = 'public' AND f_table_name = %s;
    """, (table_name,))
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]
    else:
        # If geometry_columns is empty, check information_schema
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND udt_name = 'geometry';
        """, (table_name,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0]
        else:
            return None

def process_files(args, conn, existing_tables):
    if args.password:
        engine = create_engine(f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}')
    else:
        engine = create_engine(f'postgresql://{args.dbuser}@{args.host}:{args.port}/{args.dbname}')

    # Determine file extensions to process
    supported_extensions = ['.shp', '.geojson', '.json', '.gpkg', '.kml', '.gml']
    if args.extension:
        extensions_to_process = [args.extension.lower()]
    else:
        extensions_to_process = supported_extensions

    # Collect files to process
    file_info_list = []
    for root, dirs, files in os.walk(args.filepath):
        for file in files:
            if any(file.lower().endswith(ext) for ext in extensions_to_process):
                full_path = os.path.join(root, file)
                table_name = os.path.splitext(file)[0].lower()

                # Read the spatial file to get the geometry column name
                try:
                    gdf = gpd.read_file(full_path)
                    input_geom_col = gdf.geometry.name
                    file_info_list.append({
                        'file': file,
                        'full_path': full_path,
                        'table_name': table_name,
                        'gdf': gdf,
                        'input_geom_col': input_geom_col
                    })
                except Exception as e:
                    logging.error(f"Error reading '{file}': {e}")
                    continue

    if not file_info_list:
        logging.warning("No spatial files found to process.")
        return

    # Collect geometry column names
    geom_col_files = defaultdict(list)
    for info in file_info_list:
        geom_col_files[info['input_geom_col']].append(info['file'])

    # Handle geometry column renaming
    if len(geom_col_files) == 1:
        # All files have the same geometry column name
        geom_col = next(iter(geom_col_files))
        if geom_col != 'geom':
            file_names = geom_col_files[geom_col]
            if args.rename_geom:
                action = 'y'
            else:
                print(f"Geometry column detected as '{geom_col}' for files {file_names}. Rename to 'geom'? (y/n): ", end='')
                action = input()
            if action.lower() == 'y':
                for info in file_info_list:
                    info['gdf'] = info['gdf'].rename_geometry('geom')
                    info['renamed'] = True
            else:
                for info in file_info_list:
                    info['renamed'] = False
        else:
            for info in file_info_list:
                info['renamed'] = False
    else:
        # Files have different geometry column names
        for geom_col, files in geom_col_files.items():
            if geom_col != 'geom':
                if args.rename_geom:
                    action = 'y'
                else:
                    print(f"Geometry column detected as '{geom_col}' for files {files}. Rename to 'geom'? (y/n): ", end='')
                    action = input()
                if action.lower() == 'y':
                    for info in file_info_list:
                        if info['input_geom_col'] == geom_col:
                            info['gdf'] = info['gdf'].rename_geometry('geom')
                            info['renamed'] = True
                else:
                    for info in file_info_list:
                        if info['input_geom_col'] == geom_col:
                            info['renamed'] = False
            else:
                for info in file_info_list:
                    if info['input_geom_col'] == geom_col:
                        info['renamed'] = False

    # Process files with tqdm progress bar
    for info in tqdm(file_info_list, desc="Processing files", unit="file"):
        file = info['file']
        table_name = info['table_name']
        gdf = info['gdf']
        input_geom_col = gdf.geometry.name

        logging.info(f"Processing file '{file}'.")

        # Compatibility Check: Table Name Conflict
        if table_name in existing_tables:
            logging.warning(f"Table '{table_name}' already exists.")
            if args.overwrite:
                action = 'y'
            else:
                print(f"Table '{table_name}' exists. Overwrite? (y/n): ", end='')
                action = input()
            if action.lower() != 'y':
                logging.info(f"Skipping '{file}'.")
                continue
            else:
                logging.info(f"Overwriting table '{table_name}'.")
        else:
            existing_tables.append(table_name)  # Add to existing tables list

        # Get geometry column name in database table (if it exists)
        if table_name in existing_tables:
            db_geom_col = get_db_geometry_column(conn, table_name)
            if db_geom_col:
                logging.info(f"Geometry column in database table '{table_name}' is '{db_geom_col}'.")
                if input_geom_col != db_geom_col:
                    if args.rename_geom:
                        action = 'y'
                    else:
                        print(f"The matching table in the PostGIS database uses '{db_geom_col}' as the geometry column name. Rename input file's geometry column from '{input_geom_col}' to '{db_geom_col}'? (y/n): ", end='')
                        action = input()
                    if action.lower() == 'y':
                        gdf = gdf.rename_geometry(db_geom_col)
                        input_geom_col = db_geom_col
                        logging.info(f"Renamed geometry column to '{db_geom_col}'.")
                    else:
                        logging.info(f"Skipping '{file}' due to geometry column name mismatch.")
                        continue
            else:
                logging.warning(f"No geometry column found in existing table '{table_name}'.")

        # CRS Compatibility Check
        gdf = check_crs_compatibility(gdf, conn, table_name, input_geom_col, args)
        if gdf is None:
            continue  # Skip this file if CRS is incompatible or an error occurred

        # Write to PostGIS
        try:
            gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
            logging.info(f"Imported '{file}' to table '{table_name}'.")
        except Exception as e:
            logging.error(f"Error importing '{file}': {e}")
            continue

        # Create spatial index
        create_spatial_index(conn, table_name, geom_column=input_geom_col)

def check_crs_compatibility(gdf, conn, table_name, geom_column, args):
    cursor = conn.cursor()
    # Check if the table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        );
    """, (table_name,))
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Table does not exist, proceed without CRS check
        logging.info(f"Table '{table_name}' does not exist. Skipping CRS compatibility check.")
        cursor.close()
        return gdf  # Proceed with the current GeoDataFrame

    # Table exists, retrieve existing SRID using ST_SRID
    try:
        cursor.execute(f"""
            SELECT ST_SRID("{geom_column}") FROM "{table_name}" WHERE "{geom_column}" IS NOT NULL LIMIT 1;
        """)
        result = cursor.fetchone()
        if result:
            existing_srid = result[0]
            logging.info(f"Existing SRID for '{table_name}' is {existing_srid}.")
        else:
            logging.warning(f"No geometries found in '{table_name}' to determine SRID.")
            existing_srid = None
    except Exception as e:
        logging.error(f"Error retrieving SRID for '{table_name}': {e}")
        conn.rollback()
        cursor.close()
        return None  # Skip this file due to error

    # Get the SRID of the new data
    new_srid = gdf.crs.to_epsg()
    if new_srid is None:
        logging.warning(f"No EPSG code found for the CRS of the new data for '{table_name}'.")
        if args.overwrite:
            action = 'y'
        else:
            print(f"Proceed without CRS check for '{table_name}'? (y/n): ", end='')
            action = input()
        if action.lower() != 'y':
            logging.info(f"Skipping '{table_name}' due to unknown CRS.")
            cursor.close()
            return None  # Skip this file
    else:
        logging.info(f"CRS of new data for '{table_name}' is EPSG:{new_srid}.")

    # Compare SRIDs
    if existing_srid and new_srid != existing_srid:
        logging.warning(f"CRS mismatch for '{table_name}': Existing SRID {existing_srid}, New SRID {new_srid}")
        if args.overwrite:
            action = 'y'
        else:
            print(f"Reproject new data to SRID {existing_srid}? (y/n): ", end='')
            action = input()
        if action.lower() == 'y':
            gdf = gdf.to_crs(epsg=existing_srid)
            logging.info(f"Reprojected new data to SRID {existing_srid}.")
        else:
            logging.info(f"Skipping '{table_name}' due to CRS mismatch.")
            cursor.close()
            return None  # Skip this file
    else:
        logging.info(f"CRS is compatible for '{table_name}'.")

    cursor.close()
    return gdf  # Return the (possibly reprojected) GeoDataFrame

def main():
    args = parse_arguments()
    if not args.password:
        args.password = getpass.getpass(prompt='Database password: ')

    # Configure logging
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Invalid log level: {args.log_level}")
        sys.exit(1)

    logging.basicConfig(
        filename='dbfriend.log',
        level=numeric_level,
        format='%(asctime)s %(levelname)s:%(message)s'
    )

    conn = connect_db(args.dbname, args.dbuser, args.host, args.port, args.password)
    existing_tables = get_existing_tables(conn)
    process_files(args, conn, existing_tables)
    conn.close()
    logging.info("All tasks completed.")

if __name__ == '__main__':
    main()
