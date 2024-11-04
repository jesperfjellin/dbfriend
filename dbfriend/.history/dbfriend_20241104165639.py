#!/usr/bin/env python
import argparse
import os
import sys
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
import logging
import getpass
import hashlib
import json
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from collections import defaultdict
from pathlib import Path
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# Initialize rich Console
console = Console(width=100)

# Configure logging with rich
logging.basicConfig(
    level=logging.INFO,  # Default level; will be updated based on arguments
    format="%(message)s",
    handlers=[
        RichHandler(
            console=console,
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            show_path=False,  # Removes the file path from each line
            show_time=False,
            markup=True
        ),
        logging.FileHandler(
            filename=Path('logs/console.log'),
            mode='a', # Append to the log file
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger("rich")

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Load spatial data into PostGIS with compatibility checks.',
        add_help=False  # Disable default help to implement custom help
    )

    # Check if the user is requesting help
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'help':
        display_help()
        sys.exit(0)

    # Define positional arguments
    parser.add_argument('dbuser', help='Database user')
    parser.add_argument('dbname', help='Database name')
    parser.add_argument('filepath', help='Path to data files')
    parser.add_argument('password', nargs='?', help='Database password (will prompt if not provided)')

    # Define optional arguments (flags)
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing tables without prompting.')
    parser.add_argument('--rename-geom', action='store_true', help='Automatically rename geometry columns to "geom" without prompting.')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging verbosity.')

    # Add optional arguments for host and port with default values
    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', default='5432', help='Database port (default: 5432)')

    return parser.parse_args()

def display_help():
    help_message = """
Usage:
    dbfriend <username> <dbname> <filepath> <password> [options]

Positional Arguments:
    <username>    Database user
    <dbname>      Database name
    <filepath>    Path to data files
    <password>    Database password

Options:
    --overwrite       Overwrite existing tables without prompting.
    --rename-geom     Automatically rename geometry columns to "geom" without prompting.
    --log-level       Set the logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    --host            Database host (default: localhost).
    --port            Database port (default: 5432).

Examples:
    dbfriend jesper mydatabase path/to/your/files password --overwrite --rename-geom
    dbfriend jesper mydatabase path/to/your/files password --log-level DEBUG --host 192.168.1.100 --port 5433
"""
    console.print(help_message)

def connect_db(dbname, dbuser, host, port, password):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=dbuser,
            host=host,
            port=port,
            password=password
        )
        logger.info("Database connection established ✓")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
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
        logging.info(f"Spatial index created on table '{table_name}'.")
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

def check_table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        );
    """, (table_name,))
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists

def compute_geom_hash(geometry):
    wkb = geometry.wkb
    return hashlib.md5(wkb).hexdigest()

def compare_geometries(gdf, conn, table_name, geom_column='geom'):
    """
    Compare geometries between incoming GeoDataFrame and existing PostGIS table.
    Returns a tuple of (new_geometries, updated_geometries, identical_geometries)
    """
    cursor = conn.cursor()
    
    # First, get the columns that exist in both datasets
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    db_columns = set(row[0] for row in cursor.fetchall())
    gdf_columns = set(gdf.columns)
    common_columns = list(db_columns.intersection(gdf_columns) - {geom_column})  # Exclude geometry column
    
    # Debug logging
    logger.debug(f"Common columns for comparison: {common_columns}")
    
    # Fetch existing geometries with their attributes
    columns_sql = ', '.join(f'"{col}"' for col in common_columns)
    cursor.execute(f"""
        SELECT 
            {columns_sql},
            md5(ST_AsBinary("{geom_column}")) as geom_hash
        FROM "{table_name}"
        WHERE "{geom_column}" IS NOT NULL
    """)
    
    # Store existing records with their attributes
    existing_records = {}  # geom_hash -> attributes
    column_names = [desc[0] for desc in cursor.description[:-1]]  # Exclude geom_hash
    
    for row in cursor.fetchall():
        geom_hash = row[-1]
        attrs = {col: val for col, val in zip(column_names, row[:-1])}
        existing_records[geom_hash] = attrs
    
    # Create temporary copy of GDF for comparison
    comparison_gdf = gdf.copy()
    
    # Compute geometry hashes
    comparison_gdf['geom_hash'] = comparison_gdf[geom_column].apply(compute_geom_hash)
    
    # Compare records
    new_geometries = []
    updated_geometries = []
    identical_geometries = []
    
    for idx, row in comparison_gdf.iterrows():
        geom_hash = row['geom_hash']
        
        if geom_hash not in existing_records:
            new_geometries.append(row)
            continue
            
        # Compare attributes
        existing_attrs = existing_records[geom_hash]
        current_attrs = {col: row[col] for col in common_columns}
        
        # Debug logging
        if logger.level <= logging.DEBUG:
            logger.debug(f"Comparing attributes for geometry {geom_hash}:")
            logger.debug(f"Existing: {existing_attrs}")
            logger.debug(f"Current:  {current_attrs}")
        
        # Check if any attributes are different
        attrs_match = all(
            str(existing_attrs.get(col)) == str(current_attrs.get(col))
            for col in common_columns
        )
        
        if attrs_match:
            identical_geometries.append(row)
        else:
            updated_geometries.append(row)
    
    cursor.close()
    
    # Convert lists to GeoDataFrames
    new_gdf = GeoDataFrame(new_geometries, geometry=geom_column, crs="EPSG:4326") if new_geometries else None
    updated_gdf = GeoDataFrame(updated_geometries, geometry=geom_column, crs="EPSG:4326") if updated_geometries else None
    identical_gdf = GeoDataFrame(identical_geometries, geometry=geom_column, crs="EPSG:4326") if identical_geometries else None
    
    # Remove temporary hash column
    for gdf in [new_gdf, updated_gdf, identical_gdf]:
        if gdf is not None and 'geom_hash' in gdf.columns:
            gdf.drop('geom_hash', axis=1, inplace=True)
    
    return new_gdf, updated_gdf, identical_gdf

def process_files(args, conn, existing_tables):
    engine = create_engine(f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}')

    # Determine file extensions to process
    supported_extensions = ['.shp', '.geojson', '.json', '.gpkg', '.kml', '.gml']
    
    # Collect files to process (only in specified directory, not subdirectories)
    file_info_list = []
    
    # List files only in the specified directory
    for file in os.listdir(args.filepath):
        if any(file.lower().endswith(ext) for ext in supported_extensions):
            full_path = os.path.join(args.filepath, file)
            
            # Skip if not a file (e.g., if it's a directory)
            if not os.path.isfile(full_path):
                continue
                
            table_name = os.path.splitext(file)[0].lower()

            try:
                gdf = gpd.read_file(full_path)
                gdf.set_crs(epsg=4326, inplace=True)
                input_geom_col = gdf.geometry.name
                file_info_list.append({
                    'file': file,
                    'full_path': full_path,
                    'table_name': table_name,
                    'gdf': gdf,
                    'input_geom_col': input_geom_col
                })
            except Exception as e:
                logger.error(f"[red]Error reading '{file}': {e}[/red]")
                continue

    if not file_info_list:
        logger.warning("[red]No spatial files found to process.[/red]")
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
            formatted_files = '\n         '.join(file_names)  # Format files vertically
            if args.rename_geom:
                action = 'y'
            else:
                action = console.input(f"         Geometry column detected as '{geom_col}' for files:\n         {formatted_files}\n         Rename to 'geom'? (y/n): ")
            if action.lower() == 'y':
                for info in file_info_list:
                    info['gdf'] = info['gdf'].rename_geometry('geom')
                    info['gdf'].set_crs(epsg=4326, inplace=True)  # Ensure CRS is maintained
                    info['input_geom_col'] = 'geom'  # Update the geometry column name
                    info['renamed'] = True
                logger.info("Geometry columns renamed to 'geom'.")
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
                formatted_files = '\n         '.join(files)  # Format files vertically
                if args.rename_geom:
                    action = 'y'
                else:
                    action = console.input(f"         Geometry column detected as '{geom_col}' for files:\n         {formatted_files}\n         Rename to 'geom'? (y/n): ")
                if action.lower() == 'y':
                    for info in file_info_list:
                        if info['input_geom_col'] == geom_col:
                            info['gdf'] = info['gdf'].rename_geometry('geom')
                            info['gdf'].set_crs(epsg=4326, inplace=True)  # Ensure CRS is maintained
                            info['input_geom_col'] = 'geom'  # Update the geometry column name
                            info['renamed'] = True
                    logger.info(f"Geometry columns renamed to 'geom' for files with '{geom_col}' column.")
                else:
                    for info in file_info_list:
                        if info['input_geom_col'] == geom_col:
                            info['renamed'] = False

    # Initialize rich Progress
    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]{task.description:<30}"),  # Fixed width for description
        BarColumn(bar_width=30),  # Fixed width for progress bar
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        console=console,
        expand=False  # Prevents the progress bar from expanding to full width
    ) as progress:
        task = progress.add_task("       Processing files", total=len(file_info_list))
        
        for info in file_info_list:
            file = info['file']
            table_name = info['table_name']
            input_geom_col = info['input_geom_col']
            
            logger.info(f"Processing {file}")
            
            if table_name in existing_tables:
                logger.info(f"Table {table_name} exists, analyzing differences...")
                
                new_geoms, updated_geoms, identical_geoms = compare_geometries(
                    info['gdf'], conn, table_name, input_geom_col
                )
                
                # Create summary of differences
                num_new = len(new_geoms) if new_geoms is not None and not new_geoms.empty else 0
                num_updated = len(updated_geoms) if updated_geoms is not None and not updated_geoms.empty else 0
                num_identical = len(identical_geoms) if identical_geoms is not None and not identical_geoms.empty else 0
                
                logger.info(f"Found {num_new} new geometries, {num_updated} updated geometries, and {num_identical} identical geometries. Skipping identical geometries...")
                
                # Handle new geometries
                if num_new > 0:
                    try:
                        new_geoms.to_postgis(table_name, engine, if_exists='append', index=False)
                        logger.info(f"Successfully appended {num_new} new geometries to {table_name}")
                    except Exception as e:
                        logger.error(f"Error appending new geometries: {e}")
            else:
                # For new tables
                num_geometries = len(info['gdf'])
                logger.info(f"Found {num_geometries} new geometries to import into new table '{table_name}'")
                
                try:
                    info['gdf'].to_postgis(table_name, engine, if_exists='replace', index=False)
                    logger.info(f"Successfully imported {num_geometries} geometries to new table '{table_name}'")
                    create_spatial_index(conn, table_name, geom_column=input_geom_col)
                    existing_tables.append(table_name)
                except Exception as e:
                    logger.error(f"[red]Error importing '{file}': {e}[/red]")
            
            progress.advance(task)

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
            logger.info(f"Existing SRID for '{table_name}' is {existing_srid}")
        else:
            logger.warning(f"No geometries found in '{table_name}' to determine SRID")
            existing_srid = None
    except Exception as e:
        logger.error(f"Error retrieving SRID for '{table_name}': {e}")
        conn.rollback()
        cursor.close()
        return None  # Skip this file due to error

    # Get the SRID of the new data
    new_srid = gdf.crs.to_epsg()
    if new_srid is None:
        logger.warning(f"No EPSG code found for the CRS of the new data for '{table_name}'")
        if args.overwrite:
            action = 'y'
        else:
            action = console.input(f"Proceed without CRS check for '{table_name}'? (y/n): ")
        if action.lower() != 'y':
            logger.info(f"Skipping '{table_name}' due to unknown CRS")
            cursor.close()
            return None  # Skip this file
    else:
        logger.info(f"CRS of new data for '{table_name}' is EPSG:{new_srid}")

    # Compare SRIDs
    if existing_srid and new_srid != existing_srid:
        logger.warning(f"CRS mismatch for '{table_name}': Existing SRID {existing_srid}, New SRID {new_srid}")
        if args.overwrite:
            action = 'y'
        else:
            action = console.input(f"Reproject new data to SRID {existing_srid}? (y/n): ")
        if action.lower() == 'y':
            try:
                gdf = gdf.to_crs(epsg=existing_srid)
                logger.info(f"Reprojected new data to SRID {existing_srid}")
            except Exception as e:
                logger.error(f"[red]Error reprojecting data for '{table_name}': {e}[/red]")
                cursor.close()
                return None  # Skip this file due to reprojection error
        else:
            logger.info(f"Skipping '{table_name}' due to CRS mismatch")
            cursor.close()
            return None  # Skip this file
    else:
        logger.info(f"CRS is compatible for '{table_name}'")

    cursor.close()
    return gdf  # Return the (possibly reprojected) GeoDataFrame

def main():
    args = parse_arguments()

    # Securely handle the password
    if not args.password:
        # Check for environment variable first
        args.password = os.getenv('DB_PASSWORD')
        if not args.password:
            args.password = getpass.getpass(prompt='Database password: ')

    # Update logging level based on arguments
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        console.print(f"[red]Invalid log level: {args.log_level}[/red]")
        sys.exit(1)
    logger.setLevel(numeric_level)

    conn = connect_db(args.dbname, args.dbuser, args.host, args.port, args.password)
    existing_tables = get_existing_tables(conn)
    process_files(args, conn, existing_tables)
    conn.close()
    logger.info("All tasks completed ✓")

if __name__ == '__main__':
    main()