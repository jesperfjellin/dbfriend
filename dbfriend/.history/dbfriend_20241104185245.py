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
import re
from typing import List, Set
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

def get_non_essential_columns(conn, table_name: str, schema: str = 'public', custom_patterns: List[str] = None) -> Set[str]:
    """
    Retrieve a set of non-essential columns based on naming patterns and database metadata.
    
    Args:
        conn: Database connection object.
        table_name (str): Name of the table.
        schema (str): Schema of the table (default is 'public').
        custom_patterns (List[str], optional): Additional regex patterns for exclusion.
    
    Returns:
        Set[str]: A set of column names to exclude.
    """
    if custom_patterns is None:
        custom_patterns = []
    
    cursor = conn.cursor()
    
    # Fetch all column names
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{table_name}'
    """)
    all_columns = set(row[0] for row in cursor.fetchall())
    
    # Fetch primary key columns
    cursor.execute(f"""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = '{schema}'
          AND tc.table_name = '{table_name}'
    """)
    pk_columns = set(row[0] for row in cursor.fetchall())
    
    # Fetch columns with default values indicating auto-generation (e.g., sequences)
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{table_name}'
          AND column_default IS NOT NULL
    """)
    default_columns = set(row[0] for row in cursor.fetchall())
    
    cursor.close()
    
    # Define regex patterns for exclusion
    exclusion_patterns = [
        r'^id$',          # Exact match 'id'
        r'^gid$',         # Exact match 'gid'
        r'.*_id$',        # Suffix '_id'
        r'.*_gid$',       # Suffix '_gid'
        r'^uuid$',        # Exact match 'uuid'
        r'^created_at$',  # Exact match 'created_at'
        r'^updated_at$',  # Exact match 'updated_at'
        r'^.*_at$',       # Suffix '_at'
    ]
    
    # Add custom patterns if any
    for pattern in custom_patterns:
        exclusion_patterns.append(pattern)
    
    # Compile regex patterns
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in exclusion_patterns]
    
    # Identify columns matching exclusion patterns
    pattern_excluded = set()
    for col in all_columns:
        if any(pattern.match(col) for pattern in compiled_patterns):
            pattern_excluded.add(col)
    
    # Combine pattern-based exclusions with metadata-based exclusions
    metadata_excluded = pk_columns.union(default_columns)
    
    # Final set of columns to exclude
    exclude_columns = pattern_excluded.union(metadata_excluded)
    
    return exclude_columns

def compare_geometries(gdf: GeoDataFrame, conn, table_name: str, geom_column: str = 'geom', exclude_columns: List[str] = None):
    """
    Compare geometries between incoming GeoDataFrame and existing PostGIS table.
    Returns a tuple of (new_geometries, updated_geometries, identical_geometries)
    
    Args:
        gdf (GeoDataFrame): Incoming GeoDataFrame.
        conn: Database connection object.
        table_name (str): Name of the target table.
        geom_column (str): Name of the geometry column.
        exclude_columns (List[str], optional): Columns to exclude from comparison.
    
    Returns:
        Tuple[GeoDataFrame, GeoDataFrame, GeoDataFrame]: New, Updated, Identical geometries.
    """
    if exclude_columns is None:
        exclude_columns = []
    
    cursor = conn.cursor()
    
    # Get columns from the database table
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public'
          AND table_name   = '{table_name}'
    """)
    db_columns = set(row[0] for row in cursor.fetchall())
    
    # Get columns from the GeoDataFrame
    gdf_columns = set(gdf.columns)
    
    # Determine common columns, excluding the geometry column and any excluded columns
    common_columns = list(db_columns.intersection(gdf_columns) - {geom_column} - set(exclude_columns))
    
    # Debug logging
    logger.debug(f"Common columns for comparison: {common_columns}")
    
    # Get existing records from database
    sql = f"""
    SELECT 
        MD5(ST_AsBinary({geom_column})) as geom_hash,
        {', '.join(common_columns)}
    FROM "{table_name}"
    """
    logger.debug(f"Executing SQL: {sql}")
    
    existing_records = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        logger.debug(f"Raw database results: {rows}")
        
        for row in rows:
            geom_hash = row[0]
            attrs = {col: row[i+1] for i, col in enumerate(common_columns)}
            if geom_hash not in existing_records:
                existing_records[geom_hash] = []
            existing_records[geom_hash].append(attrs)
            
    logger.debug(f"Existing records from database: {existing_records}")
    
    cursor.close()
    
    # Create temporary copy of GDF for comparison
    comparison_gdf = gdf.copy()
    
    # Compute geometry hashes
    comparison_gdf['geom_hash'] = comparison_gdf[geom_column].apply(compute_geom_hash)
    
    # Compare records
    new_geometries = []
    updated_geometries = []
    identical_geometries = []
    
    # Keep track of processed geometries to avoid duplicates
    processed_geom_hashes = set()
    
    for idx, row in comparison_gdf.iterrows():
        geom_hash = row['geom_hash']
        
        if geom_hash in processed_geom_hashes:
            logger.debug(f"Skipping duplicate geometry with hash {geom_hash}")
            continue
            
        if geom_hash not in existing_records:
            logger.debug(f"New geometry found with hash {geom_hash}")
            new_geometries.append(row)
            processed_geom_hashes.add(geom_hash)
            continue
        
        # Compare attributes with all existing records having the same geom_hash
        if common_columns:
            current_attrs = {col: row[col] for col in common_columns}
            
            # Debug current record
            logger.debug(f"\nChecking record with hash {geom_hash}:")
            logger.debug(f"Current attributes: {current_attrs}")
            logger.debug(f"Existing records for this hash: {existing_records[geom_hash]}")
            
            # Check if this exact record already exists
            found_match = False
            for existing_attrs in existing_records[geom_hash]:
                # Compare each attribute
                all_match = True
                for col in common_columns:
                    current_val = str(current_attrs.get(col, '')).strip()
                    existing_val = str(existing_attrs.get(col, '')).strip()
                    if current_val != existing_val:
                        logger.debug(f"Mismatch in column {col}: current='{current_val}' existing='{existing_val}'")
                        all_match = False
                        break
                
                if all_match:
                    logger.debug("Found exact attribute match - marking as identical")
                    identical_geometries.append(row)
                    found_match = True
                    break
            
            if not found_match:
                logger.debug("No exact match found - marking as updated")
                updated_geometries.append(row)
            
            processed_geom_hashes.add(geom_hash)
        else:
            identical_geometries.append(row)
            processed_geom_hashes.add(geom_hash)

    logger.debug(f"Processed geometry hashes: {processed_geom_hashes}")
    logger.debug(f"New geometries: {len(new_geometries)}")
    logger.debug(f"Updated geometries: {len(updated_geometries)}")
    logger.debug(f"Identical geometries: {len(identical_geometries)}")
    
    # Convert lists to GeoDataFrames
    new_gdf = GeoDataFrame(new_geometries, geometry=geom_column, crs=gdf.crs) if new_geometries else GeoDataFrame(columns=gdf.columns)
    updated_gdf = GeoDataFrame(updated_geometries, geometry=geom_column, crs=gdf.crs) if updated_geometries else GeoDataFrame(columns=gdf.columns)
    identical_gdf = GeoDataFrame(identical_geometries, geometry=geom_column, crs=gdf.crs) if identical_geometries else GeoDataFrame(columns=gdf.columns)
    
    # Remove temporary hash column
    for gdf_temp in [new_gdf, updated_gdf, identical_gdf]:
        if 'geom_hash' in gdf_temp.columns:
            gdf_temp.drop('geom_hash', axis=1, inplace=True)
    
    return new_gdf if not new_gdf.empty else None, \
           updated_gdf if not updated_gdf.empty else None, \
           identical_gdf if not identical_gdf.empty else None

def update_geometries(gdf, table_name, engine, unique_id_column):
    """Update existing geometries in PostGIS table."""
    if gdf is None or gdf.empty:
        return

    try:
        # Create temporary table for updates
        temp_table = f"temp_{table_name}"
        gdf.to_postgis(temp_table, engine, if_exists='replace', index=False)

        # Update main table from temp table
        with engine.connect() as connection:
            from sqlalchemy import text
            
            # Get all columns except the unique ID
            columns = [col for col in gdf.columns if col != unique_id_column]
            update_cols = ", ".join([f"{col} = s.{col}" for col in columns])
            
            sql = text(f"""
                UPDATE "{table_name}" t
                SET {update_cols}
                FROM "{temp_table}" s
                WHERE t.{unique_id_column} = s.{unique_id_column}
            """)
            logger.debug(f"Executing update SQL: {sql}")
            connection.execute(sql)
            connection.execute(text(f'DROP TABLE IF EXISTS "{temp_table}"'))
            connection.commit()
        
        logger.info(f"Successfully updated {len(gdf)} geometries in {table_name}")
    except Exception as e:
        logger.error(f"Error updating geometries: {e}")

def process_files(args, conn, existing_tables):
    engine = create_engine(f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}')
    
    total_new = 0
    total_updated = 0
    total_identical = 0

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
                action = input(f"Geometry column detected as '{geom_col}' for files:\n         {formatted_files}\n         Rename to 'geom'? (y/n): ")
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
                    action = input(f"Geometry column detected as '{geom_col}' for files:\n         {formatted_files}\n         Rename to 'geom'? (y/n): ")
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

    # Define columns to exclude from comparison dynamically
    exclude_cols = set()
    for info in file_info_list:
        table_name = info['table_name']
        non_essential = get_non_essential_columns(conn, table_name)
        exclude_cols.update(non_essential)
    exclude_cols = list(exclude_cols)
    
    if exclude_cols:
        logger.debug(f"Columns excluded from comparison: {exclude_cols}")
    else:
        logger.debug("No columns excluded from comparison.")
    
    # Initialize rich Progress
    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]{task.description:<30}"),
        BarColumn(bar_width=30),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        console=console,
        expand=False
    ) as progress:
        task = progress.add_task("       Processing files", total=len(file_info_list))
        
        for info in file_info_list:
            file = info['file']
            table_name = info['table_name']
            input_geom_col = info['input_geom_col']
            gdf = info['gdf']
            
            logger.info(f"Processing {file}")
            
            if table_name in existing_tables:
                logger.info(f"Table {table_name} exists, analyzing differences...")
                
                new_geoms, updated_geoms, identical_geoms = compare_geometries(
                    gdf, conn, table_name, input_geom_col, exclude_columns=exclude_cols
                )
                
                # Create summary of differences
                num_new = len(new_geoms) if new_geoms is not None else 0
                num_updated = len(updated_geoms) if updated_geoms is not None else 0
                num_identical = len(identical_geoms) if identical_geoms is not None else 0
                
                total_new += num_new
                total_updated += num_updated
                total_identical += num_identical
                
                logger.info(f"Found {num_new} new geometries, {num_updated} updated geometries, and {num_identical} identical geometries. Skipping identical geometries...")
                
                # Detailed logging for updated geometries
                if num_updated > 0:
                    logger.debug("Updated geometries details:")
                    for idx, row in updated_geoms.iterrows():
                        logger.debug(f"Updated Geometry: {row.to_dict()}")
                
                # Detailed logging for identical geometries
                if num_identical > 0:
                    logger.debug("Identical geometries details:")
                    for idx, row in identical_geoms.iterrows():
                        logger.debug(f"Identical Geometry: {row.to_dict()}")
                
                # Handle new geometries
                if num_new > 0:
                    try:
                        new_geoms.to_postgis(table_name, engine, if_exists='append', index=False)
                        logger.info(f"Successfully appended {num_new} new geometries to {table_name}")
                    except Exception as e:
                        logger.error(f"Error appending new geometries: {e}")
                else:
                    #logger.info(f"No new geometries to append to {table_name}")
                    pass
                
                # Handle updated geometries (if implemented)
                if num_updated > 0:
                    update_geometries(updated_geoms, table_name, engine, unique_id_column='osm_id')  # Adjust unique_id_column as needed
                
            else:
                # For new tables
                num_geometries = len(gdf)
                logger.info(f"Found {num_geometries} new geometries to import into new table '{table_name}'")
                
                try:
                    gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
                    logger.info(f"Successfully imported {num_geometries} geometries to new table '{table_name}'")
                    create_spatial_index(conn, table_name, geom_column=input_geom_col)
                    existing_tables.append(table_name)
                except Exception as e:
                    logger.error(f"[red]Error importing '{file}': {e}[/red]")
            
            progress.advance(task)

    # After the progress bar completes, show the summary
    logger.info("All tasks completed ✓")
    logger.info(f"Summary of tasks: {total_new:,} new geometries added, "
               f"{total_updated:,} geometries updated, "
               f"{total_identical:,} duplicate geometries skipped")

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
