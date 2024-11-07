#!/usr/bin/env python
import argparse
import os
import sys
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
            show_path=False,
            show_time=False,
            markup=True
        )
    ]
)
logger = logging.getLogger("rich")

def print_geometry_details(row, status="", coordinates_enabled=False):
    """Print coordinates and attributes for a geometry."""
    if not coordinates_enabled:  # Skip if flag not set
        # Still log basic info without coordinates
        if isinstance(row, dict):
            logger.info(row.get('name', ''))
        return
    
    # Try both 'geometry' and 'geom' column names
    geom = row.get('geometry') or row.get('geom')
    if geom is None:
        logger.warning(f"No geometry column found in row: {row}")
        return
    
    # Extract attributes excluding geometry columns
    if isinstance(row, dict):
        attrs = {k: v for k, v in row.items() if k not in ('geometry', 'geom')}
    else:
        # Assume it's a pandas/geopandas DataFrame row
        attrs = row.drop(['geometry', 'geom'] if 'geometry' in row else ['geom']).to_dict()
    
    attrs_str = ", ".join(f"{k}: {v}" for k, v in attrs.items())
    
    # Prepare output text
    output_lines = [
        f"\n{status} Geometry Details:",
        f"Attributes: {attrs_str}"
    ]
    
    if geom.geom_type == 'Point':
        output_lines.append(f"Coordinates: ({geom.x:.6f}, {geom.y:.6f})")
    else:
        if hasattr(geom, 'exterior'):
            coords = list(geom.exterior.coords)
            output_lines.append("Coordinates:")
            # Format each coordinate pair individually
            for x, y in coords:
                output_lines.append(f"({x:.6f}, {y:.6f})")
            
            if geom.interiors:
                for i, interior in enumerate(geom.interiors):
                    output_lines.append(f"Interior Ring {i+1} Coordinates:")
                    for x, y in interior.coords:
                        output_lines.append(f"({x:.6f}, {y:.6f})")
        else:
            coords = list(geom.coords)
            output_lines.append("Coordinates:")
            for x, y in coords:
                output_lines.append(f"({x:.6f}, {y:.6f})")
    
    # Output to terminal
    for line in output_lines:
        logger.info(line)
    
    # Output to file
    with open('geometry_details.txt', 'a', encoding='utf-8') as f:
        f.write('\n'.join(output_lines) + '\n')

def parse_arguments():
    help_text = """
Usage:
    dbfriend <username> <dbname> <filepath> [options]

Positional Arguments:
    <username>    Database user
    <dbname>      Database name
    <filepath>    Path to data files

Options:
    --help            Show this help message and exit
    --overwrite       Overwrite existing tables without prompting.
    --log-level       Set the logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    --host            Database host (default: localhost).
    --port            Database port (default: 5432).
    --epsg            Target EPSG code for the data. If not specified, will preserve source CRS
                      or default to 4326.
    --schema          Specify the database schema that the data will be queried from and written to.
    --coordinates     Print coordinates and attributes for each geometry.
    --table           Target table name. If specified, all data will be loaded into this table
    
Note: Password will be prompted securely or can be set via DB_PASSWORD environment variable.
"""

    # If --help is passed, print help and exit
    if '--help' in sys.argv:
        console.print(help_text)
        sys.exit(0)

    # Custom argument parser that only uses --help
    parser = argparse.ArgumentParser(add_help=False)

    # Define positional arguments
    parser.add_argument('dbuser', help='Database user')
    parser.add_argument('dbname', help='Database name')
    parser.add_argument('filepath', help='Path to data files')

    # Define optional arguments
    parser.add_argument('--help', action='store_true', 
                       help='Show this help message and exit')
    parser.add_argument('--overwrite', action='store_true', 
                       help='Overwrite existing tables without prompting')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set the logging verbosity')
    parser.add_argument('--host', default='localhost',
                       help='Database host (default: localhost)')
    parser.add_argument('--port', default='5432',
                       help='Database port (default: 5432)')
    parser.add_argument('--epsg', type=int,
                       help='Target EPSG code for the data. If not specified, will preserve source CRS or default to 4326')
    parser.add_argument('--schema', 
                       help='Specify the database schema')
    parser.add_argument('--coordinates', action='store_true', 
                       help='Print coordinates and attributes for each geometry')
    parser.add_argument('--table', 
                       help='Target table name. If specified, all data will be loaded into this table')

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
        logger.info("Database connection established ✓")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

def get_existing_tables(conn, schema='public'):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s;
    """, (schema,))
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def create_spatial_index(conn, table_name, schema='public', geom_column='geom'):
    cursor = conn.cursor()
    # Get the actual geometry column name
    actual_geom_column = get_db_geometry_column(conn, table_name, schema=schema) or geom_column
    index_name = f"{schema}_{table_name}_{actual_geom_column}_idx"
    
    try:
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS "{index_name}"
            ON "{schema}"."{table_name}"
            USING GIST ("{actual_geom_column}");
        """)
        conn.commit()
        logger.info(f"Spatial index created on table '{schema}.{table_name}'")
    except Exception as e:
        logger.error(f"Error creating spatial index on '{schema}.{table_name}': {e}")
        conn.rollback()
    finally:
        cursor.close()

def get_db_geometry_column(conn, table_name, schema='public'):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT f_geometry_column
        FROM geometry_columns
        WHERE f_table_schema = %s AND f_table_name = %s;
    """, (schema, table_name))
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
            WHERE table_schema = %s AND table_name = %s AND udt_name = 'geometry';
        """, (schema, table_name))
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0]
        else:
            return None

def check_table_exists(conn, table_name, schema='public'):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
    """, (schema, table_name))
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

def compare_geometries(gdf: GeoDataFrame, conn, table_name: str, geom_column: str = 'geom', exclude_columns: List[str] = None, args=None):
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
    cursor = conn.cursor()
    
    # Get the actual geometry column name from the database
    db_geom_column = get_db_geometry_column(conn, table_name, schema='public')
    if not db_geom_column:
        logger.error(f"No geometry column found in table '{table_name}'")
        return None, None, None
        
    # Get common columns between GDF and database table
    common_columns = [col for col in gdf.columns if col != geom_column]
    if exclude_columns:
        common_columns = [col for col in common_columns if col not in exclude_columns]
    
    # Use the database geometry column name for the SQL query
    if common_columns:
        quoted_columns = ', '.join(f'"{col}"' for col in common_columns)
        columns_sql = f"MD5(ST_AsBinary({db_geom_column})) as geom_hash, {quoted_columns}"
    else:
        columns_sql = f"MD5(ST_AsBinary({db_geom_column})) as geom_hash"
    
    sql = f"""
    SELECT {columns_sql}
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
            print_geometry_details(row, "NEW", args.coordinates)
            new_geometries.append(row)
            processed_geom_hashes.add(geom_hash)
            continue
        
        # Compare attributes with all existing records having the same geom_hash
        if common_columns:
            current_attrs = {col: str(row[col]).strip() if pd.notnull(row[col]) else None 
                           for col in common_columns}
            
            logger.debug("\nComparing record:")
            logger.debug(f"Current attributes: {current_attrs}")
            logger.debug(f"Existing records: {existing_records[geom_hash]}")
            
            # Compare with existing records
            found_match = False
            for existing_attrs in existing_records[geom_hash]:
                # Normalize existing attributes the same way
                existing_attrs = {col: str(val).strip() if val is not None else None 
                                for col, val in existing_attrs.items()}
                
                if current_attrs == existing_attrs:
                    logger.debug("Found exact match")
                    print_geometry_details(row, "DUPLICATE", args.coordinates)
                    identical_geometries.append(row)
                    found_match = True
                    break
                else:
                    # Log differences
                    differences = {
                        col: (current_attrs.get(col), existing_attrs.get(col))
                        for col in common_columns
                        if current_attrs.get(col) != existing_attrs.get(col)
                    }
                    logger.debug(f"Differences found: {differences}")
                    print_geometry_details(row, "UPDATED", args.coordinates)
            
            if not found_match:
                logger.debug("No match found - marking as updated")
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

        with engine.connect() as connection:
            from sqlalchemy import text
            
            # First, check for and add any new columns
            cursor = connection.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = '{table_name}'
            """))
            existing_columns = {row[0] for row in cursor}
            
            # Get new columns from the GeoDataFrame
            new_columns = set(gdf.columns) - existing_columns
            
            # Add any new columns to the main table
            for col in new_columns:
                # Determine column type from GeoDataFrame
                dtype = gdf[col].dtype
                if dtype == 'object':
                    sql_type = 'TEXT'
                elif dtype == 'int64':
                    sql_type = 'INTEGER'
                elif dtype == 'float64':
                    sql_type = 'DOUBLE PRECISION'
                else:
                    sql_type = 'TEXT'  # Default to TEXT for unknown types
                
                logger.info(f"Adding new column '{col}' with type {sql_type}")
                connection.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{col}" {sql_type}'))
            
            # Now proceed with the update
            columns = [col for col in gdf.columns if col != unique_id_column]
            update_cols = ", ".join([f'"{col}" = s."{col}"' for col in columns])
            
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
    # Modify the engine creation to include schema if specified
    if args.schema:
        engine = create_engine(
            f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}',
            connect_args={'options': f'-c search_path={args.schema},public'}
        )
        schema = args.schema
    else:
        engine = create_engine(f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}')
        schema = 'public'

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
                
                # Handle CRS
                source_crs = gdf.crs
                if args.epsg:
                    # User specified an EPSG
                    if source_crs and source_crs.to_epsg() != args.epsg:
                        logger.info(f"Reprojecting from EPSG:{source_crs.to_epsg()} to EPSG:{args.epsg}")
                        gdf.set_crs(source_crs, inplace=True)  # Ensure source CRS is set
                        gdf = gdf.to_crs(epsg=args.epsg)
                    else:
                        gdf.set_crs(epsg=args.epsg, inplace=True)
                elif not source_crs:
                    # No source CRS and no user EPSG specified, default to 4326
                    logger.warning(f"No CRS found in {file}, defaulting to EPSG:4326")
                    gdf.set_crs(epsg=4326, inplace=True)
                else:
                    # Keep source CRS
                    pass
                    # logger.info(f"Using source CRS: EPSG:{source_crs.to_epsg()}")
                
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
    for info in file_info_list:
        gdf = info['gdf']
        if gdf.geometry.name != 'geom':
            gdf = gdf.rename_geometry('geom')
            gdf.set_geometry('geom', inplace=True)
            gdf.set_crs(gdf.crs, inplace=True)  # Preserve CRS
            info['gdf'] = gdf
            info['input_geom_col'] = 'geom'

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
            # Use specified table name if provided, otherwise use filename
            table_name = args.table if args.table else info['table_name']
            qualified_table = f"{schema}.{table_name}"  # Define this early
            gdf = info['gdf']
            
            try:
                logger.info(f"Processing {file}")
                
                # Handle geometry column naming
                if gdf.geometry.name != 'geom':
                    logger.debug(f"Renaming geometry column from '{gdf.geometry.name}' to 'geom'")
                    gdf = gdf.rename_geometry('geom')
                    gdf.set_geometry('geom', inplace=True)
                    gdf.set_crs(gdf.crs, inplace=True)  # Preserve CRS
                
                if args.table and info != file_info_list[0]:
                    # For subsequent files when using --table, always append
                    logger.info(f"Appending {len(gdf)} geometries to existing table '{qualified_table}'")
                    gdf.to_postgis(
                        name=table_name,
                        con=engine,
                        schema=schema,
                        if_exists='append',
                        index=False
                    )
                    total_new += len(gdf)
                elif table_name in existing_tables:
                    logger.info(f"Table {qualified_table} exists, analyzing differences...")
                    
                    # Get common columns between GDF and database table
                    common_columns = [col for col in gdf.columns if col != 'geom']
                    if not common_columns:
                        # If no common columns, just compare geometries
                        columns_sql = "MD5(ST_AsBinary(geom)) as geom_hash"
                    else:
                        quoted_columns = ', '.join(f'"{col}"' for col in common_columns)
                        columns_sql = f"MD5(ST_AsBinary(geom)) as geom_hash, {quoted_columns}"
                    
                    # Check existing geometry column name in PostGIS
                    existing_geom_col = get_db_geometry_column(conn, table_name, schema=schema)
                    
                    # Only keep 'geometry' if it's the existing column name, otherwise use 'geom'
                    target_geom_col = 'geometry' if existing_geom_col == 'geometry' else 'geom'
                    
                    if gdf.geometry.name != target_geom_col:
                        logger.debug(f"Renaming geometry column from '{gdf.geometry.name}' to '{target_geom_col}'")
                        gdf = gdf.rename_geometry(target_geom_col)
                        gdf.set_geometry(target_geom_col, inplace=True)
                        gdf.set_crs(gdf.crs, inplace=True)  # Preserve CRS
                    
                    new_geoms, updated_geoms, identical_geoms = compare_geometries(
                        gdf, conn, table_name, gdf.geometry.name, exclude_columns=exclude_cols, args=args
                    )
                    
                    # Create summary of differences
                    num_new = len(new_geoms) if new_geoms is not None else 0
                    num_updated = len(updated_geoms) if updated_geoms is not None else 0
                    num_identical = len(identical_geoms) if identical_geoms is not None else 0
                    
                    total_new += num_new
                    total_updated += num_updated
                    total_identical += num_identical
                    
                    logger.info(f"Found {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries, "
                               f"{format(num_updated, ',').replace(',', ' ')}[yellow] updated[/] geometries, and "
                               f"{format(num_identical, ',').replace(',', ' ')} [red]identical[/] geometries skipped. "
                               "Skipping identical geometries...")
                    
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
                            # Use schema-qualified table name
                            new_geoms.to_postgis(
                                qualified_table, 
                                engine, 
                                if_exists='append', 
                                index=False
                            )
                            logger.info(f"Successfully appended {num_new} new geometries to {qualified_table}")
                        except Exception as e:
                            logger.error(f"Error appending new geometries: {e}")
                    else:
                        #logger.info(f"No new geometries to append to {table_name}")
                        pass
                    
                    # Handle updated geometries (if implemented)
                    if num_updated > 0:
                        update_geometries(updated_geoms, table_name, engine, unique_id_column='osm_id')  # Adjust unique_id_column as needed
                    
                else:
                    num_geometries = len(gdf)
                    logger.info(f"Found {num_geometries} new geometries to import into new table '{qualified_table}'")
                    
                    # Add coordinate printing for new tables
                    if args.coordinates:
                        for idx, row in gdf.iterrows():
                            print_geometry_details(row, "NEW", args.coordinates)
                    
                    try:
                        # Write to PostGIS with schema
                        gdf.to_postgis(
                            name=table_name,  # Use unqualified name
                            con=engine,
                            schema=schema,    # Specify schema separately
                            if_exists='replace',
                            index=False
                        )
                        
                        # Verify the table was created
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT 1 
                                FROM information_schema.tables 
                                WHERE table_schema = %s 
                                AND table_name = %s
                            );
                        """, (schema, table_name))
                        table_exists = cursor.fetchone()[0]
                        cursor.close()
                        
                        if table_exists:
                            logger.info(f"Successfully imported {num_geometries} geometries to new table '{qualified_table}'")
                            create_spatial_index(conn, table_name, schema=schema, geom_column='geom')
                            existing_tables.append(table_name)
                            total_new += num_geometries
                        else:
                            logger.error(f"Failed to create table '{qualified_table}'")
                            
                    except Exception as e:
                        logger.error(f"Error importing '{file}': {e}")
                        continue
                    
            except Exception as e:
                logger.error(f"Error processing '{file}': {e}")
                continue
            
            progress.advance(task)

    # After the progress bar completes, show the summary
    logger.info("All tasks completed ✓")
    logger.info("Summary of tasks:\n"
               f"[green]{format(total_new, ',').replace(',', ' ')}[/] new geometries added, "
               f"[yellow]{format(total_updated, ',').replace(',', ' ')}[/] updated geometries, "
               f"{format(total_identical, ',').replace(',', ' ')} [red]identical[/] geometries skipped")

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

def check_schema_exists(conn, schema_name: str) -> bool:
    """Check if the specified schema exists."""
    cursor = conn.cursor()
    
    # Debug: List all available schemas
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata;
    """)
    all_schemas = [row[0] for row in cursor.fetchall()]
    logger.debug(f"Available schemas: {all_schemas}")
    
    # Check for specific schema
    cursor.execute("""
        SELECT EXISTS(
            SELECT 1 
            FROM information_schema.schemata 
            WHERE schema_name = %s
        );
    """, (schema_name.lower(),))
    exists = cursor.fetchone()[0]
    
    logger.debug(f"Schema check for '{schema_name}': {exists}")
    logger.debug(f"Current user: {conn.info.user}")
    logger.debug(f"Current database: {conn.info.dbname}")
    
    cursor.close()
    return exists

def main():
    args = parse_arguments()

    # Securely handle the password
    password = os.getenv('DB_PASSWORD')
    if not password:
        password = getpass.getpass(prompt='Database password: ')
    
    # Update the args namespace with the password
    args.password = password

    # Update logging level based on arguments
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        console.print(f"[red]Invalid log level: {args.log_level}[/red]")
        sys.exit(1)
    logger.setLevel(numeric_level)

    # Verify schema exists if specified
    conn = connect_db(args.dbname, args.dbuser, args.host, args.port, args.password)
    if args.schema:
        if not check_schema_exists(conn, args.schema):
            logger.error(f"[red]Schema '{args.schema}' does not exist. Please create it first.[/red]")
            sys.exit(1)
        logger.info(f"Using schema '{args.schema}'")
    
    # Continue with normal operation
    existing_tables = get_existing_tables(conn, schema=args.schema or 'public')
    process_files(args, conn, existing_tables)
    conn.close()

if __name__ == '__main__':
    main()
