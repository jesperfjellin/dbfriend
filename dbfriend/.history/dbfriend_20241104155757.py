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

def compare_geometries(gdf, conn, table_name, geom_column='geom'):
    """
    Compare geometries between incoming GeoDataFrame and existing PostGIS table.
    Returns a tuple of (new_geometries, updated_geometries, identical_geometries)
    """
    # Get existing geometries from PostGIS
    cursor = conn.cursor()
    
    # First, get the columns that exist in both datasets
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    db_columns = set(row[0] for row in cursor.fetchall())
    gdf_columns = set(gdf.columns)
    common_columns = list(db_columns.intersection(gdf_columns))
    
    # Fetch existing geometries with their attributes
    columns_sql = ', '.join(f'"{col}"' for col in common_columns)
    cursor.execute(f"""
        SELECT 
            {columns_sql},
            ST_AsText("{geom_column}") as geom_wkt,
            MD5(CAST(row_to_json(t) AS TEXT)) as row_hash
        FROM (SELECT * FROM "{table_name}") t
    """)
    
    existing_records = {}
    for row in cursor.fetchall():
        # Create a hash of the geometry and attributes
        existing_records[row[-2]] = row[-1]  # WKT -> hash mapping
    
    # Compare with new geometries
    new_geometries = []
    updated_geometries = []
    identical_geometries = []
    
    for idx, row in gdf.iterrows():
        wkt = row[geom_column].wkt
        # Create a comparable hash of the new record
        row_dict = row[common_columns].to_dict()
        row_dict[geom_column] = wkt
        new_hash = hashlib.md5(json.dumps(row_dict, sort_keys=True).encode()).hexdigest()
        
        if wkt not in existing_records:
            new_geometries.append(row)
        elif existing_records[wkt] != new_hash:
            updated_geometries.append(row)
        else:
            identical_geometries.append(row)
    
    cursor.close()
    return (
        GeoDataFrame(new_geometries, geometry=geom_column, crs="EPSG:4326") if new_geometries else None,
        GeoDataFrame(updated_geometries, geometry=geom_column, crs="EPSG:4326") if updated_geometries else None,
        GeoDataFrame(identical_geometries, geometry=geom_column, crs="EPSG:4326") if identical_geometries else None
    )

def process_files(args, conn, existing_tables):
    engine = create_engine(f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}')

    # Determine file extensions to process
    supported_extensions = ['.shp', '.geojson', '.json', '.gpkg', '.kml', '.gml']
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
                    gdf.set_crs(epsg=4326, inplace=True)  # Set CRS to 4326
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
            
            # Update logging messages
            logger.info(f"Processing {file}")
            
            if table_name in existing_tables:
                logger.info(f"Table {table_name} exists, analyzing differences...")
                
                # When renaming geometry, we need to set it explicitly
                if info.get('renamed', False):
                    info['gdf'] = info['gdf'].set_geometry('geom')
                
                new_geoms, updated_geoms, identical_geoms = compare_geometries(
                    info['gdf'], conn, table_name, info['input_geom_col']
                )
                
                # Fix the any() check
                if new_geoms is None and updated_geoms is None:
                    logger.info(f"No changes needed for {table_name}")
                    continue
                
                if new_geoms is not None:
                    logger.info(f"Found {len(new_geoms)} new geometries to append")
                    try:
                        # Ensure geometry and CRS are set correctly before writing
                        new_geoms = new_geoms.set_geometry(info['input_geom_col'])
                        new_geoms.set_crs(epsg=4326, inplace=True)
                        new_geoms.to_postgis(
                            table_name, 
                            engine, 
                            if_exists='append', 
                            index=False
                        )
                        logger.info(f"Successfully appended new geometries to {table_name}")
                    except Exception as e:
                        logger.error(f"Error appending new geometries: {e}")
                
                if updated_geoms is not None:
                    logger.info(f"Found {len(updated_geoms)} geometries with updated attributes")
                    try:
                        # Create temporary table for updates
                        temp_table = f"temp_{table_name}"
                        updated_geoms.to_postgis(
                            temp_table, 
                            engine, 
                            if_exists='replace', 
                            index=False
                        )
                        
                        # Update main table from temp table
                        cursor = conn.cursor()
                        cursor.execute(f"""
                            UPDATE "{table_name}" t
                            SET {
                                ', '.join(f'"{col}" = s."{col}"' 
                                for col in updated_geoms.columns 
                                if col != info['input_geom_col'])
                            }
                            FROM "{temp_table}" s
                            WHERE ST_Equals(t."{info['input_geom_col']}", s."{info['input_geom_col']}")
                        """)
                        conn.commit()
                        
                        # Clean up temp table
                        cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                        conn.commit()
                        cursor.close()
                        
                        logger.info(f"Successfully updated existing geometries in {table_name}")
                    except Exception as e:
                        logger.error(f"Error updating geometries: {e}")
                        conn.rollback()
                
                if new_geoms is None and updated_geoms is None:
                    logger.info(f"No changes needed for {table_name}")

            # CRS Compatibility Check
            gdf = check_crs_compatibility(info['gdf'], conn, table_name, info['input_geom_col'], args)
            if gdf is None:
                progress.advance(task)
                continue  # Skip this file if CRS is incompatible or an error occurred

            # Write to PostGIS
            try:
                gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
                logger.info(f"Imported '{file}' to table '{table_name}'")
                existing_tables.append(table_name)  # Add to existing_tables after successful import
            except Exception as e:
                logger.error(f"[red]Error importing '{file}': {e}[/red]")
                progress.advance(task)
                continue

            # Create spatial index
            create_spatial_index(conn, table_name, geom_column=input_geom_col)
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
