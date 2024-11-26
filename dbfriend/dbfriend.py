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
import datetime
from geopandas import GeoDataFrame
from collections import defaultdict
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
import subprocess

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
    --schema          Target schema name. If specified, all data will be loaded into this schema.
                      If the schema does not exist, dbfriend will not generate one for
                      safety reasons.
    --table           Target table name. If specified, all data will be loaded into this table.
                      If the table does not exist, it will be created.
    --coordinates     Print coordinates and attributes for each geometry.
    --no-backup       Do not create backups of existing tables before modifying them.
    
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
    parser.add_argument('--no-backup', action='store_true',
                       help='Do not create backups of existing tables before modifying them')

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
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s;
        """, (schema,))
        tables = [row[0] for row in cursor.fetchall()]
    return tables

def identify_affected_tables(file_info_list, args, schema='public'):
    """Identify all tables that will be modified during this run."""
    affected_tables = set()
    
    if args.table:
        # If --table is specified, only one table will be affected
        affected_tables.add(args.table)
    else:
        # Otherwise, collect all table names from file_info_list
        for info in file_info_list:
            affected_tables.add(info['table_name'])
    
    return affected_tables

def manage_old_backups(backup_dir, table_name):
    """Keep only the last 3 file backups for a given table."""
    try:
        # Create backups directory if it doesn't exist
        os.makedirs(backup_dir, exist_ok=True)
        
        # Find all backup files for this table
        backup_files = [f for f in os.listdir(backup_dir) 
                       if f.startswith(f"{table_name}_backup_") and f.endswith('.sql')]
        backup_files.sort(reverse=True)
        
        # Remove all but the last 3 backups
        if len(backup_files) > 3:
            for old_file in backup_files[3:]:
                os.remove(os.path.join(backup_dir, old_file))
                
    except Exception as e:
        logger.error(f"Error managing old backups: {e}")

def backup_tables(conn, tables, schema='public'):
    """Create file backups of all affected tables before processing."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(os.getcwd(), 'backups')
    backup_info = {}
    
    try:
        # Create backups directory if it doesn't exist
        os.makedirs(backup_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to create backup directory: {e}")
        return backup_info  # Continue without backups
    
    for table in tables:
        # Ensure table name is lowercase for consistency
        table = table.lower()
        
        if not check_table_exists(conn, table, schema):
            logger.info(f"Table '{schema}.{table}' does not exist, no backup needed.")
            continue
        
        backup_file = os.path.join(backup_dir, f"{table}_backup_{timestamp}.sql")
        
        try:
            # Create pg_dump command
            cmd = [
                'pg_dump',
                f'--host={conn.info.host}',
                f'--port={conn.info.port}',
                f'--username={conn.info.user}',
                f'--dbname={conn.info.dbname}',
                f'--table={schema}.{table}',  # Use lowercase table name
                '--format=p',
                f'--file={backup_file}'
            ]
            
            # Set PGPASSWORD environment variable for the subprocess
            env = os.environ.copy()
            env['PGPASSWORD'] = conn.info.password
            
            # Execute pg_dump
            subprocess.run(cmd, env=env, check=True, capture_output=True)
            
            backup_info[table] = backup_file
            logger.info(f"Created backup of '{schema}.{table}' to '{backup_file}'")
            
            # Manage old backups
            manage_old_backups(backup_dir, table)  # Pass lowercase table name
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to backup table '{schema}.{table}': {e.stderr.decode()}")
            # Continue processing even if backup fails
            
    return backup_info

def create_spatial_index(conn, table_name, schema='public', geom_column='geom'):
    # Get the actual geometry column name
    actual_geom_column = get_db_geometry_column(conn, table_name, schema=schema) or geom_column
    index_name = f"{schema}_{table_name}_{actual_geom_column}_idx"
    
    try:
        with conn.cursor() as cursor:
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

def get_db_geometry_column(conn, table_name, schema='public'):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s;
        """, (schema, table_name))
        result = cursor.fetchone()
        
    if result:
        return result[0]
    else:
        # If geometry_columns is empty, check information_schema
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND udt_name = 'geometry';
            """, (schema, table_name))
            result = cursor.fetchone()
            
        if result:
            return result[0]
        else:
            return None

def check_table_exists(conn, table_name, schema='public'):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema, table_name))
        exists = cursor.fetchone()[0]
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
    
    with conn.cursor() as cursor:
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
        
        # Fetch columns with default values
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

def compare_geometries(gdf: GeoDataFrame, conn, table_name: str, geom_column: str = 'geom', schema: str = 'public', exclude_columns: List[str] = None, args=None):
    
    # Get the actual geometry column name from the database
    db_geom_column = get_db_geometry_column(conn, table_name, schema=schema)
    if not db_geom_column:
        logger.error(f"No geometry column found in table '{schema}.{table_name}'")
        return None, None, None
    
    sql = f"""
    SELECT MD5(ST_AsBinary({db_geom_column})) as geom_hash
    FROM {schema}.{table_name}
    """
    
    # Get existing geometry hashes from database
    existing_hashes = set()
    with conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            existing_hashes.add(row[0])
    
    # Create temporary copy of GDF for comparison
    comparison_gdf = gdf.copy()
    comparison_gdf['geom_hash'] = comparison_gdf[geom_column].apply(compute_geom_hash)
    
    # Compare with database hashes
    new_geometries = []
    identical_geometries = []
    
    for idx, row in comparison_gdf.iterrows():
        geom_hash = row['geom_hash']
        if geom_hash in existing_hashes:
            identical_geometries.append(row)
        else:
            new_geometries.append(row)
    
    # Convert lists to GeoDataFrames
    new_gdf = GeoDataFrame(new_geometries, geometry=geom_column, crs=gdf.crs) if new_geometries else GeoDataFrame(columns=gdf.columns)
    identical_gdf = GeoDataFrame(identical_geometries, geometry=geom_column, crs=gdf.crs) if identical_geometries else GeoDataFrame(columns=gdf.columns)
    
    # Remove temporary hash column
    for gdf_temp in [new_gdf, identical_gdf]:
        if 'geom_hash' in gdf_temp.columns:
            gdf_temp.drop('geom_hash', axis=1, inplace=True)
    
    return new_gdf if not new_gdf.empty else None, None, identical_gdf if not identical_gdf.empty else None

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

def check_geometry_type_constraint(conn, table_name, schema='public'):
    """Check if table has a specific geometry type constraint."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT type 
            FROM geometry_columns 
            WHERE f_table_schema = %s 
            AND f_table_name = %s
        """, (schema, table_name))
        result = cursor.fetchone()
    
    if result and result[0].upper() != 'GEOMETRY':
        return result[0].upper()
    return None

def create_generic_geometry_table(conn, engine, table_name, srid, schema='public'):
    """Create a new table with a generic geometry column and specified SRID."""
    try:
        with conn.cursor() as cursor:
            # Drop table if it exists (use quoted identifiers)
            cursor.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
            
            # Create table with generic geometry type and SRID (use quoted identifiers)
            sql = f"""
            CREATE TABLE "{schema}"."{table_name}" (
                gid SERIAL PRIMARY KEY,
                geom geometry(Geometry, {srid})
            );
            """
            cursor.execute(sql)
            conn.commit()
        
        # Add spatial index after commit
        create_spatial_index(conn, table_name, schema=schema)
        
        logger.info(f"Created new table '{schema}.{table_name}' with generic geometry type (SRID: {srid})")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table: {e}")
        return False

def append_geometries(conn, engine, gdf, table_name, schema='public'):
    """Append geometries using raw SQL to avoid CRS issues."""
    try:
        # Create temporary table with schema (use quoted identifiers)
        temp_table = f"temp_{table_name}"
        gdf.to_postgis(
            temp_table,
            engine,
            schema=schema,
            if_exists='replace',
            index=False
        )
        
        # Copy geometries from temp to main table (use quoted identifiers)
        with conn.cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO "{schema}"."{table_name}" (geom)
                SELECT geom FROM "{schema}"."{temp_table}"
            """)
            
            # Clean up
            cursor.execute(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"')
            conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error appending geometries: {e}")
        return False

def process_files(args, conn, engine, existing_tables, schema):
    logger.debug("Entering process_files...")
    total_new = 0
    total_updated = 0
    total_identical = 0

    try:
        # Start fresh transaction
        conn.rollback()  # Ensure clean state
        
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
        
        # Normalize table name if provided
        if args.table:
            args.table = args.table.lower()

        # Identify affected tables
        affected_tables = identify_affected_tables(file_info_list, args, schema)
        
        # Create backups of all affected tables at once
        if not args.no_backup:
            backup_info = backup_tables(conn, affected_tables, schema)
            
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
            non_essential = get_non_essential_columns(conn, table_name, schema=schema)
            exclude_cols.update(non_essential)
        exclude_cols = list(exclude_cols)

        if exclude_cols:
            logger.debug(f"Columns excluded from comparison: {exclude_cols}")
        else:
            logger.debug("No columns excluded from comparison.")

        # Check for geometry type constraint if using --table and table exists
        if args.table and args.table in existing_tables:
            geom_type = check_geometry_type_constraint(conn, args.table, schema)
            if geom_type:
                logger.error(f"[red]Error: Table '{schema}.{args.table}' has a specific {geom_type} geometry type constraint.[/red]")
                logger.error("[yellow]To use this table with mixed geometry types, you need to either:[/yellow]")
                logger.error("  1. Drop the existing table and let dbfriend create it with a generic geometry type")
                logger.error("  2. Use a different table name")
                logger.error("\nExiting to allow you to make this decision.")
                sys.exit(1)

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
                table_name = args.table if args.table else info['table_name']
                qualified_table = f"{schema}.{table_name}"
                gdf = info['gdf']

                try:
                    logger.info(f"Processing {file}")

                    # Handle geometry column naming
                    if gdf.geometry.name != 'geom':
                        gdf = gdf.rename_geometry('geom')
                        gdf.set_geometry('geom', inplace=True)
                        gdf.set_crs(gdf.crs, inplace=True)

                    # Ensure valid CRS
                    if not gdf.crs:
                        logger.warning(f"No CRS found in {file}, defaulting to EPSG:4326")
                        gdf.set_crs(epsg=4326, inplace=True)

                    if args.table:
                        # When using --table, only keep the geometry column
                        gdf = gdf[['geom']]

                        if table_name not in existing_tables:
                            # Get SRID from data or args
                            srid = args.epsg if args.epsg else gdf.crs.to_epsg()
                            if not srid:
                                srid = 4326  # Default to WGS84 if no SRID found

                            # Create table with generic geometry type and SRID
                            if create_generic_geometry_table(conn, engine, table_name, srid, schema):
                                existing_tables.append(table_name)
                            else:
                                continue

                            # For new tables, all geometries are new
                            logger.info(f"Found {format(len(gdf), ',').replace(',', ' ')} [green]new[/] geometries.")

                            # Append first batch of geometries
                            logger.info(f"Appending {format(len(gdf), ',').replace(',', ' ')} geometries to '{qualified_table}'")
                            if append_geometries(conn, engine, gdf, table_name, schema):
                                total_new += len(gdf)
                        else:
                            # Compare geometries before appending
                            new_geoms, updated_geoms, identical_geoms = compare_geometries(
                                gdf, conn, table_name, 'geom', schema=schema, exclude_columns=[], args=args
                            )

                            # Create summary of differences for this dataset
                            num_new = len(new_geoms) if new_geoms is not None else 0
                            num_updated = len(updated_geoms) if updated_geoms is not None else 0
                            num_identical = len(identical_geoms) if identical_geoms is not None else 0

                            logger.info(f"Found {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries, "
                                        f"{format(num_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries, and "
                                        f"{format(num_identical, ',').replace(',', ' ')} [red]identical[/] geometries skipped.")

                            if new_geoms is not None and not new_geoms.empty:
                                logger.info(f"Appending {format(len(new_geoms), ',').replace(',', ' ')} geometries to '{qualified_table}'")
                                # Use schema parameter in to_postgis
                                new_geoms.to_postgis(
                                    name=table_name,
                                    con=engine,
                                    schema=schema,
                                    if_exists='append',
                                    index=False
                                )
                                total_new += len(new_geoms)

                            if identical_geoms is not None:
                                total_identical += len(identical_geoms)
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
                            gdf, conn, table_name, gdf.geometry.name, schema=schema, exclude_columns=exclude_cols, args=args
                        )

                        # Create summary of differences
                        num_new = len(new_geoms) if new_geoms is not None else 0
                        num_updated = len(updated_geoms) if updated_geoms is not None else 0
                        num_identical = len(identical_geoms) if identical_geoms is not None else 0

                        total_new += num_new
                        total_updated += num_updated
                        total_identical += num_identical

                        logger.info(f"Found {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries, "
                                    f"{format(num_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries, and "
                                    f"{format(num_identical, ',').replace(',', ' ')} [red]identical[/] geometries skipped. "
                                    "Skipping identical geometries...")

                        # Handle new geometries
                        if num_new > 0:
                            try:
                                # Use schema parameter in to_postgis
                                new_geoms.to_postgis(
                                    name=table_name,
                                    con=engine,
                                    schema=schema,
                                    if_exists='append',
                                    index=False
                                )
                                logger.info(f"Successfully appended {num_new} new geometries to {qualified_table}")
                            except Exception as e:
                                logger.error(f"Error appending new geometries: {e}")
                        else:
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
                            with conn.cursor() as cursor:
                                cursor.execute("""
                                    SELECT EXISTS (
                                        SELECT 1
                                        FROM information_schema.tables
                                        WHERE table_schema = %s
                                        AND table_name = %s
                                    );
                                """, (schema, table_name))
                                table_exists = cursor.fetchone()[0]

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

        # Single commit at the end
        conn.commit()
        logger.info("All changes committed successfully")
        logger.info("Summary of tasks:\n"
                    f"{format(total_new, ',').replace(',', ' ')} [green]new[/] geometries added, "
                    f"{format(total_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries, "
                    f"{format(total_identical, ',').replace(',', ' ')} [red]identical[/] geometries skipped")

    except Exception as e:
        conn.rollback()
        raise  # Re-raise the exception to be caught by main()

def check_crs_compatibility(gdf, conn, table_name, geom_column, args):
    with conn.cursor() as cursor:
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
    with conn.cursor() as cursor:
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
        
        return exists

def main():
    args = parse_arguments()
    
    # Normalize schema and table names immediately after parsing
    if args.schema:
        args.schema = args.schema.lower()
    if args.table:
        args.table = args.table.lower()
    
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

    conn = None
    try:
        logger.debug("Establishing database connection...")
        conn = psycopg2.connect(
            dbname=args.dbname,
            user=args.dbuser,
            host=args.host,
            port=args.port,
            password=args.password
        )
        
        # Start in autocommit mode for session setup
        conn.autocommit = True
        logger.info("Database connection established ✓")

        # Handle schema setup while in autocommit mode
        if args.schema:
            logger.debug(f"Checking schema '{args.schema}'...")
            if not check_schema_exists(conn, args.schema):
                logger.error(f"[red]Schema '{args.schema}' does not exist. Please create it first.[/red]")
                sys.exit(1)
            logger.info(f"Using schema '{args.schema}'")
            
            logger.debug("Setting search_path...")
            with conn.cursor() as cursor:
                cursor.execute(f"SET search_path TO {args.schema}, public;")
            schema = args.schema
        else:
            schema = 'public'

        # Create SQLAlchemy engine with specific isolation level
        logger.debug("Creating SQLAlchemy engine...")
        engine = create_engine(
            f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}',
            isolation_level='READ COMMITTED'  # Changed from AUTOCOMMIT
        )

        # Switch to transaction mode for the main operations
        conn.autocommit = False

        logger.debug("Getting existing tables...")
        existing_tables = get_existing_tables(conn, schema=schema)

        logger.debug("Starting file processing...")
        process_files(args, conn, engine, existing_tables, schema)

    except Exception as e:
        if conn and not conn.autocommit:
            conn.rollback()
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            logger.debug("Closing database connection...")
            conn.close()

if __name__ == '__main__':
    main()
