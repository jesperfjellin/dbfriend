# TODO: Refactor dbfriend.py into modular files

#!/usr/bin/env python
import argparse
import datetime
import getpass
import hashlib
import logging
import os
import re
import subprocess
import sys
import time
from typing import List
from typing import Set
import geopandas as gpd
import psycopg2
from geopandas import GeoDataFrame
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from sqlalchemy import create_engine
import fiona
import json
from pathlib import Path

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

def quote_identifier(name: str) -> str:
    """
    Safely quote a database identifier (table name, column name, etc.)
    
    Args:
        name: The identifier to quote
        
    Returns:
        Safely quoted identifier
        
    Raises:
        ValueError: If the identifier contains invalid characters
    """
    # Check if name is None or empty
    if not name or not isinstance(name, str):
        raise ValueError(f"Invalid identifier: {name}")
    
    # Basic validation for common SQL identifier rules
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
        raise ValueError(f"Invalid identifier: {name}")
    
    # Double quote the identifier and escape any existing quotes
    return '"' + name.replace('"', '""') + '"'

def build_update_statement(table_name: str, schema: str, columns: List[str], where_clause: str) -> tuple[str, list]:
    """
    Safely build an UPDATE statement with proper quoting and parameterization.
    
    Args:
        table_name: Name of the table to update
        schema: Schema name
        columns: List of column names to update
        where_clause: The WHERE clause condition
        
    Returns:
        Tuple of (SQL statement, list of parameters)
    """
    try:
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table_name)
        quoted_columns = [quote_identifier(col) for col in columns]
    except ValueError as e:
        raise ValueError(f"Invalid identifier in SQL statement: {e}")
    
    # Build SET clause with placeholders
    set_clause = ", ".join(f"{col} = %s" for col in quoted_columns)
    
    sql = f"""
        UPDATE {quoted_schema}.{quoted_table}
        SET {set_clause}
        WHERE {where_clause}
    """
    
    return sql

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
    --dry-run         Show what operations would be performed without actually executing them.
    --deploy          Monitor directory for new/changed files and automatically import them.
    
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
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what operations would be performed without actually executing them')
    parser.add_argument('--deploy', action='store_true',
                       help='Monitor directory for new/changed files and automatically import them')

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

def backup_tables(conn, tables, schema='public', dry_run=False):
    """Create file backups of all affected tables before processing."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(os.getcwd(), 'backups')
    backup_info = {}
    
    if dry_run:
        logger.info("[yellow][DRY RUN MODE][/yellow] Would create backups for the following tables:")
        for table in tables:
            if check_table_exists(conn, table.lower(), schema):
                backup_file = os.path.join(backup_dir, f"{table.lower()}_backup_{timestamp}.sql")
                logger.info(f"  - Would backup '{schema}.{table}' to '{backup_file}'")
        return backup_info

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
            # Validate identifiers
            try:
                validated_schema = quote_identifier(schema)
                validated_table = quote_identifier(table)
            except ValueError as e:
                logger.error(f"Invalid identifier in backup_tables: {e}")
                continue  # Skip this table

            # Construct the '--table' argument using validated identifiers
            if any(char.isupper() or not char.isalnum() and char != '_' for char in schema + table):
                table_arg = f'"{schema}"."{table}"'
            else:
                table_arg = f'{schema}.{table}'

            # Prepare the pg_dump command
            cmd = [
                'pg_dump',
                f'--host={conn.info.host}',
                f'--port={conn.info.port}',
                f'--username={conn.info.user}',
                f'--dbname={conn.info.dbname}',
                f'--table={table_arg}',
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

def create_spatial_index(conn, table_name, schema='public', geom_column='geom', dry_run=False):
    """Create a spatial index on the geometry column."""
    if dry_run:
        safe_index_name = re.sub(r'[^a-zA-Z0-9_]', '_', f"{schema}_{table_name}_{geom_column}_idx")
        logger.info(f"[yellow][DRY RUN][/yellow] Would create spatial index '{safe_index_name}' on table '{schema}.{table_name}'")
        return
    
    try:
        # Validate and quote identifiers
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table_name)
        quoted_geom = quote_identifier(geom_column)
        
        # Create a safe index name by removing invalid characters
        safe_index_name = re.sub(r'[^a-zA-Z0-9_]', '_', f"{schema}_{table_name}_{geom_column}_idx")
        quoted_index_name = quote_identifier(safe_index_name)
        
        with conn.cursor() as cursor:
            sql = f"""
                CREATE INDEX IF NOT EXISTS {quoted_index_name}
                ON {quoted_schema}.{quoted_table}
                USING GIST ({quoted_geom});
            """
            cursor.execute(sql)
            conn.commit()
            logger.info(f"Spatial index created on table '{schema}.{table_name}'")
    except Exception as e:
        logger.error(f"Error creating spatial index on '{schema}.{table_name}': {e}")
        conn.rollback()

def get_db_geometry_column(conn, table_name, schema='public'):
    # Validate and quote identifiers
    try:
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table_name)
    except ValueError as e:
        logger.error(f"Invalid identifier: {e}")
        return None
    
    with conn.cursor() as cursor:
        # Use parameterized query for values, quoted identifiers for names
        cursor.execute("""
            SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_schema = %s 
            AND f_table_name = %s;
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
                WHERE table_schema = %s 
                AND table_name = %s 
                AND udt_name = 'geometry';
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
    """Compute hash for geometry coordinates only (for backward compatibility)."""
    wkb = geometry.wkb
    return hashlib.md5(wkb).hexdigest()

def compute_attributes_hash(row, exclude_columns=None, geom_column='geom'):
    """
    Compute hash for all non-geometry attributes.
    
    Args:
        row: DataFrame row or dictionary
        exclude_columns: List of columns to exclude from hashing
        geom_column: Name of geometry column to exclude
        
    Returns:
        MD5 hash of concatenated attribute values
    """
    if exclude_columns is None:
        exclude_columns = []
    
    # Convert row to dict if it's a pandas Series
    if hasattr(row, 'to_dict'):
        attr_dict = row.to_dict()
    else:
        attr_dict = dict(row)
    
    # Exclude geometry and specified columns
    attrs_to_hash = {}
    for key, value in attr_dict.items():
        if (key not in exclude_columns and 
            key != geom_column and 
            key != 'geometry' and
            not key.endswith('_hash')):  # Don't include our hash columns
            # Convert to string for consistent hashing
            attrs_to_hash[key] = str(value) if value is not None else 'NULL'
    
    # Sort by key for consistent hashing regardless of dict order
    sorted_attrs = sorted(attrs_to_hash.items())
    attrs_string = '|'.join([f"{k}:{v}" for k, v in sorted_attrs])
    
    return hashlib.md5(attrs_string.encode('utf-8')).hexdigest()

def compute_composite_hash(row, geom_column='geom', exclude_columns=None):
    """
    Compute composite hash including both geometry and attributes.
    
    Args:
        row: DataFrame row containing geometry and attributes
        geom_column: Name of geometry column
        exclude_columns: List of columns to exclude from attribute hashing
        
    Returns:
        MD5 hash combining geometry and attributes
    """
    geom = row[geom_column] if geom_column in row else row.get('geometry')
    if geom is None:
        geom_hash = 'NULL'
    else:
        geom_hash = compute_geom_hash(geom)
    
    attr_hash = compute_attributes_hash(row, exclude_columns, geom_column)
    
    # Combine geometry and attribute hashes
    composite_string = f"geom:{geom_hash}|attrs:{attr_hash}"
    return hashlib.md5(composite_string.encode('utf-8')).hexdigest()

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
        
    all_columns = set()
    pk_columns = set()
    default_columns = set()
    
    try:
        with conn.cursor() as cursor:
            # Fetch all column names
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
            """, (schema, table_name))
            all_columns = set(row[0] for row in cursor.fetchall())
            
            # Fetch primary key columns
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
            """, (schema, table_name))
            pk_columns = set(row[0] for row in cursor.fetchall())
            
            # Fetch columns with default values
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_default IS NOT NULL
            """, (schema, table_name))
            default_columns = set(row[0] for row in cursor.fetchall())
            
    except Exception as e:
        logger.error(f"Error getting non-essential columns for '{schema}.{table_name}': {e}")
        return set()  # Return empty set on error
    
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
    exclusion_patterns.extend(custom_patterns)
    
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

def compare_geometries(gdf: GeoDataFrame, conn, table_name: str, geom_column: str = 'geom', schema: str = 'public', exclude_columns: List[str] = None, args=None, engine=None):
    """
    Compare geometries and attributes between source data and database.
    
    This function now performs comprehensive comparison including:
    1. Composite hash comparison (geometry + attributes) to detect truly identical features
    2. Geometry-only hash comparison to detect potential updates vs new features
    3. Attribute comparison to identify what type of change occurred
    
    Returns:
        tuple: (new_gdf, updated_gdf, identical_gdf)
            - new_gdf: Completely new features (new geometry + new attributes)
            - updated_gdf: Existing features with changes (geometry or attribute changes)  
            - identical_gdf: Features with no changes at all
    """
    # Get the actual geometry column name from the database
    db_geom_column = get_db_geometry_column(conn, table_name, schema=schema)
    if not db_geom_column:
        logger.error(f"No geometry column found in table '{schema}.{table_name}'")
        return None, None, None

    # Quote identifiers
    try:
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table_name)
        quoted_geom_col = quote_identifier(db_geom_column)
    except ValueError as e:
        logger.error(f"Invalid identifier in compare_geometries: {e}")
        return None, None, None

    if exclude_columns is None:
        exclude_columns = []

    # Get all existing data from database for comprehensive comparison
    try:
        # First, get the column names from the database
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            db_columns = {row[0]: row[1] for row in cur.fetchall()}
        
        # Build SQL to get all data including computed hashes
        non_geom_columns = [col for col in db_columns.keys() if col != db_geom_column]
        
        # Quote all column names for the SELECT
        quoted_columns = []
        for col in non_geom_columns:
            try:
                quoted_columns.append(quote_identifier(col))
            except ValueError:
                logger.warning(f"Skipping column with invalid name: {col}")
                continue
        
        columns_sql = ", ".join(quoted_columns) if quoted_columns else "1"
        
        sql = f"""
        SELECT 
            {columns_sql},
            MD5(ST_AsBinary({quoted_geom_col})) as geom_hash,
            ST_AsText({quoted_geom_col}) as geom_wkt
        FROM {quoted_schema}.{quoted_table}
        """
        
        # Execute and get all existing data
        import pandas as pd
        # Use SQLAlchemy engine if available to avoid pandas warning
        if engine is not None:
            existing_df = pd.read_sql(sql, engine)
        else:
            existing_df = pd.read_sql(sql, conn)
        
        if existing_df.empty:
            # No existing data, everything is new
            logger.debug("No existing data in table, treating all as new")
            return gdf, None, None
            
    except Exception as e:
        logger.error(f"Error reading existing data from '{schema}.{table_name}': {e}")
        return None, None, None

    # Compute hashes for existing data (attributes only, since we have geom_hash)
    existing_df['attr_hash'] = existing_df.apply(
        lambda row: compute_attributes_hash(row, exclude_columns + ['geom_hash', 'geom_wkt'], db_geom_column), 
        axis=1
    )
    existing_df['composite_hash'] = existing_df.apply(
        lambda row: hashlib.md5(f"geom:{row['geom_hash']}|attrs:{row['attr_hash']}".encode('utf-8')).hexdigest(),
        axis=1
    )

    # Create lookup sets for fast comparison
    existing_geom_hashes = set(existing_df['geom_hash'])
    existing_composite_hashes = set(existing_df['composite_hash'])
    
    # Create lookup dict for finding potential updates by geometry hash
    geom_to_attrs = {}
    for _, row in existing_df.iterrows():
        geom_hash = row['geom_hash']
        if geom_hash not in geom_to_attrs:
            geom_to_attrs[geom_hash] = []
        geom_to_attrs[geom_hash].append(row['attr_hash'])

    # Process new data
    comparison_gdf = gdf.copy()
    comparison_gdf['geom_hash'] = comparison_gdf[geom_column].apply(compute_geom_hash)
    comparison_gdf['attr_hash'] = comparison_gdf.apply(
        lambda row: compute_attributes_hash(row, exclude_columns, geom_column), 
        axis=1
    )
    comparison_gdf['composite_hash'] = comparison_gdf.apply(
        lambda row: compute_composite_hash(row, geom_column, exclude_columns),
        axis=1
    )

    # Categorize each feature
    new_features = []
    updated_features = []
    identical_features = []

    for idx, row in comparison_gdf.iterrows():
        composite_hash = row['composite_hash']
        geom_hash = row['geom_hash']
        attr_hash = row['attr_hash']
        
        if composite_hash in existing_composite_hashes:
            # Exact match (geometry + attributes) - truly identical
            identical_features.append(row)
            
        elif geom_hash in existing_geom_hashes:
            # Same geometry exists but different composite hash
            # This could be:
            # 1. Attribute change on existing geometry
            # 2. Same geometry with different attributes (multiple features)
            
            existing_attr_hashes = geom_to_attrs.get(geom_hash, [])
            if attr_hash not in existing_attr_hashes:
                # This is an update - same geometry, different attributes
                updated_features.append(row)
                logger.debug("Detected update: same geometry, different attributes")
            else:
                # This shouldn't happen if composite hash is working correctly
                logger.warning(f"Hash logic inconsistency detected for feature at index {idx}")
                identical_features.append(row)
                
        else:
            # New geometry (regardless of attributes)
            new_features.append(row)

    # Convert to GeoDataFrames and clean up
    def create_clean_gdf(features, original_gdf):
        if not features:
            return None
        gdf_result = GeoDataFrame(features, geometry=geom_column, crs=original_gdf.crs)
        # Remove our temporary hash columns
        hash_cols = ['geom_hash', 'attr_hash', 'composite_hash']
        for col in hash_cols:
            if col in gdf_result.columns:
                gdf_result.drop(col, axis=1, inplace=True)
        return gdf_result

    new_gdf = create_clean_gdf(new_features, gdf)
    updated_gdf = create_clean_gdf(updated_features, gdf) 
    identical_gdf = create_clean_gdf(identical_features, gdf)

    # Log detailed comparison results
    logger.debug(f"Comparison results for '{schema}.{table_name}':")
    logger.debug(f"  - New features: {len(new_features)}")
    logger.debug(f"  - Updated features: {len(updated_features)}")
    logger.debug(f"  - Identical features: {len(identical_features)}")

    return new_gdf, updated_gdf, identical_gdf

def update_geometries(gdf, table_name, engine, conn, schema='public', dry_run=False):
    """
    Update existing features in PostGIS table.
    
    This function now handles updates more intelligently by:
    1. Identifying features to update based on geometry hash matching
    2. Replacing the matched features completely (handles both attribute and geometry changes)
    3. Using spatial operations when no unique ID is available
    
    Args:
        gdf: GeoDataFrame with updated features
        table_name: Target table name
        engine: SQLAlchemy engine
        conn: Database connection
        schema: Database schema
        dry_run: Whether to simulate the operation
    """
    if gdf is None or gdf.empty:
        return
    
    if dry_run:
        logger.info(f"[yellow][DRY RUN][/yellow] Would update {len(gdf)} existing features in table '{schema}.{table_name}'")
        return

    try:
        # Get the actual geometry column name from the database
        db_geom_column = get_db_geometry_column(conn, table_name, schema=schema)
        if not db_geom_column:
            logger.error(f"No geometry column found in table '{schema}.{table_name}'")
            return

        # Create temporary table for updates
        temp_table = f"temp_update_{table_name}"
        gdf.to_postgis(
            name=temp_table,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )

        with engine.connect() as connection:
            from sqlalchemy import text
            
            # Quote identifiers
            quoted_schema = quote_identifier(schema)
            quoted_table = quote_identifier(table_name)
            quoted_temp = quote_identifier(temp_table)
            quoted_geom_col = quote_identifier(db_geom_column)
            
            # Get target geometry column in temp table
            target_geom_col = gdf.geometry.name
            quoted_target_geom = quote_identifier(target_geom_col)
            
            # First, identify features to update by finding matches between geometry hashes
            # This handles the case where the same geometry exists but with different attributes
            
            # Delete existing features that match the geometry hash of incoming features
            delete_sql = text(f"""
                DELETE FROM {quoted_schema}.{quoted_table} 
                WHERE MD5(ST_AsBinary({quoted_geom_col})) IN (
                    SELECT DISTINCT MD5(ST_AsBinary({quoted_target_geom}))
                    FROM {quoted_schema}.{quoted_temp}
                )
            """)
            
            result = connection.execute(delete_sql)
            deleted_count = result.rowcount
            logger.debug(f"Deleted {deleted_count} existing features that will be replaced")
            
            # Insert all the updated features
            # Get column names from temp table
            cursor = connection.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :temp_table
                ORDER BY ordinal_position
            """), {"schema": schema, "temp_table": temp_table})
            temp_columns = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get column names from main table
            cursor = connection.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table_name
                ORDER BY ordinal_position
            """), {"schema": schema, "table_name": table_name})
            main_columns = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Add any new columns to the main table
            for col, data_type in temp_columns.items():
                if col not in main_columns:
                    # Map pandas/geopandas types to SQL types
                    sql_type = {
                        'object': 'TEXT',
                        'int64': 'INTEGER', 
                        'float64': 'DOUBLE PRECISION',
                        'bool': 'BOOLEAN',
                        'datetime64[ns]': 'TIMESTAMP'
                    }.get(str(gdf[col].dtype) if col in gdf.columns else 'object', 'TEXT')
                    
                    quoted_col = quote_identifier(col)
                    logger.info(f"Adding new column '{col}' with type {sql_type}")
                    connection.execute(text(f"""
                        ALTER TABLE {quoted_schema}.{quoted_table} 
                        ADD COLUMN IF NOT EXISTS {quoted_col} {sql_type}
                    """))
            
            # Insert the updated features
            # Build column list for INSERT (only columns that exist in both tables)
            common_columns = []
            for col in temp_columns:
                if col in main_columns or col in temp_columns:  # Column exists or was just added
                    try:
                        quoted_col = quote_identifier(col)
                        common_columns.append(quoted_col)
                    except ValueError:
                        logger.warning(f"Skipping column with invalid name: {col}")
                        continue
            
            if common_columns:
                columns_str = ", ".join(common_columns)
                insert_sql = text(f"""
                    INSERT INTO {quoted_schema}.{quoted_table} ({columns_str})
                    SELECT {columns_str}
                    FROM {quoted_schema}.{quoted_temp}
                """)
                
                result = connection.execute(insert_sql)
                inserted_count = result.rowcount
                logger.debug(f"Inserted {inserted_count} updated features")
            
            # Clean up temporary table
            connection.execute(text(f'DROP TABLE IF EXISTS {quoted_schema}.{quoted_temp}'))
            connection.commit()
        
        logger.info(f"Successfully updated {len(gdf)} features in {table_name}")
        
    except Exception as e:
        logger.error(f"Error updating features: {e}")
        # Clean up temp table on error
        try:
            with engine.connect() as connection:
                quoted_schema = quote_identifier(schema)
                quoted_temp = quote_identifier(f"temp_update_{table_name}")
                connection.execute(text(f'DROP TABLE IF EXISTS {quoted_schema}.{quoted_temp}'))
                connection.commit()
        except:
            pass

def check_geometry_type_constraint(conn, table_name, schema='public'):
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

def analyze_geometry_type_compatibility(gdf, conn, table_name, schema='public'):
    """
    Analyze compatibility between source data geometry types and table constraints.
    
    Args:
        gdf: GeoDataFrame with source data
        conn: Database connection
        table_name: Target table name
        schema: Database schema
        
    Returns:
        tuple: (is_compatible, table_constraint, source_types, compatibility_message)
    """
    # Get table constraint
    table_constraint = check_geometry_type_constraint(conn, table_name, schema)
    
    # Analyze source geometry types
    source_geom_types = set()
    for geom in gdf.geometry:
        if geom is not None:
            source_geom_types.add(geom.geom_type.upper())
    
    # If no constraint or generic constraint, always compatible
    if not table_constraint or table_constraint == 'GEOMETRY':
        return True, table_constraint, source_geom_types, "No geometry type constraint or generic constraint"
    
    # Check compatibility
    if len(source_geom_types) == 1:
        source_type = next(iter(source_geom_types))
        if source_type == table_constraint:
            return True, table_constraint, source_geom_types, f"Source {source_type} matches table constraint {table_constraint}"
        else:
            return False, table_constraint, source_geom_types, f"Source {source_type} incompatible with table constraint {table_constraint}"
    else:
        # Multiple geometry types in source - incompatible with specific constraint
        return False, table_constraint, source_geom_types, f"Source has mixed types {source_geom_types} incompatible with table constraint {table_constraint}"

def create_generic_geometry_table(conn, engine, table_name, srid, schema='public', dry_run=False):
    """Create a new table with a generic geometry column and specified SRID."""
    if dry_run:
        logger.info(f"[yellow][DRY RUN][/yellow] Would create new table '{schema}.{table_name}' with generic geometry type (SRID: {srid})")
        logger.info(f"[yellow][DRY RUN][/yellow] Would create spatial index on table '{schema}.{table_name}'")
        return True
    
    try:
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table_name)
        
        with conn.cursor() as cursor:
            # Drop table if it exists
            cursor.execute(f"""
                DROP TABLE IF EXISTS {quoted_schema}.{quoted_table}
            """)
            
            # Create table with generic geometry type and SRID
            cursor.execute(f"""
                CREATE TABLE {quoted_schema}.{quoted_table} (
                    gid SERIAL PRIMARY KEY,
                    geom geometry(Geometry, %s)
                )
            """, (srid,))
            
            conn.commit()
        
        # Add spatial index after commit
        create_spatial_index(conn, table_name, schema=schema, dry_run=False)
        
        logger.info(f"Created new table '{schema}.{table_name}' with generic geometry type (SRID: {srid})")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table: {e}")
        return False

def append_geometries(conn, engine, gdf, table_name, schema='public', dry_run=False):
    """Append geometries using raw SQL to avoid CRS issues."""
    if dry_run:
        logger.info(f"[yellow][DRY RUN][/yellow] Would append {len(gdf)} geometries to table '{schema}.{table_name}'")
        return True
    
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
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table_name)
        quoted_temp_table = quote_identifier(temp_table)
        with conn.cursor() as cursor:
            cursor.execute(f"""
            INSERT INTO {quoted_schema}.{quoted_table} (geom)
                SELECT geom FROM {quoted_schema}.{quoted_temp_table}
            """)
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_schema}.{quoted_temp_table}")
            conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error appending geometries: {e}")
        return False

def process_files(args, conn, engine, existing_tables, schema):
    """
    Process spatial files and import them into the database.
    
    Args:
        args: Command line arguments
        conn: Database connection
        engine: SQLAlchemy engine
        existing_tables: List of existing tables
        schema: Database schema
    """
    logger.debug("Entering process_files...")
    
    if args.dry_run:
        logger.info("[yellow][DRY RUN MODE][/yellow] - No changes will be made to the database")
    
    total_new = 0
    total_updated = 0
    total_identical = 0

    try:
        # Start fresh transaction
        conn.rollback()  # Ensure clean state
        
        file_info_list = []

        # List files only in the specified directory
        for file in os.listdir(args.filepath):
            full_path = os.path.join(args.filepath, file)
            if not os.path.isfile(full_path):
                continue
                
            # Skip the deploy state file specifically
            if file == '.dbfriend_deploy.json':
                continue

            table_name = os.path.splitext(file)[0].lower()
            try:
                # Try to read the file with geopandas - if it works, it's spatial
                gdf = gpd.read_file(full_path)
                source_crs = gdf.crs
                
                # Handle CRS
                if args.epsg:
                    if source_crs and source_crs.to_epsg() != args.epsg:
                        logger.info(f"[yellow]Reprojecting[/] from EPSG:{source_crs.to_epsg()} to EPSG:{args.epsg}")
                        gdf.set_crs(source_crs, inplace=True)
                        gdf = gdf.to_crs(epsg=args.epsg)
                    else:
                        gdf.set_crs(epsg=args.epsg, inplace=True)
                elif not source_crs:
                    logger.warning(f"No CRS found in {file}, defaulting to [yellow]EPSG:4326[/]")
                    gdf.set_crs(epsg=4326, inplace=True)

                file_info_list.append({
                    'file': file,
                    'full_path': full_path,
                    'table_name': table_name,
                    'gdf': gdf
                })
            except Exception:
                # Silently skip files that can't be read as spatial data
                logger.debug(f"Skipping '{file}' - not a readable spatial file")
                continue

        if not file_info_list:
            logger.warning("[red]No spatial files found to process.[/red]")
            return

        # Normalize table name if provided
        if args.table:
            args.table = args.table.lower()

        # Identify affected tables and create backups
        affected_tables = identify_affected_tables(file_info_list, args, schema)
        if not args.no_backup:
            backup_tables(conn, affected_tables, schema, dry_run=args.dry_run)

        # Define columns to exclude from comparison
        exclude_cols = set()
        for info in file_info_list:
            table_name = info['table_name']
            non_essential = get_non_essential_columns(conn, table_name, schema=schema)
            exclude_cols.update(non_essential)
        exclude_cols = list(exclude_cols)

        # Check geometry type constraint for --table option with intelligent compatibility analysis
        if args.table and args.table in existing_tables:
            # Analyze all files that would go into this table to check compatibility
            all_compatible = True
            compatibility_issues = []
            
            for info in file_info_list:
                gdf = info['gdf']
                is_compatible, table_constraint, source_types, message = analyze_geometry_type_compatibility(
                    gdf, conn, args.table, schema
                )
                
                if not is_compatible:
                    all_compatible = False
                    compatibility_issues.append(f"File '{info['file']}': {message}")
                else:
                    logger.debug(f"File '{info['file']}': {message}")
            
            # Only error/warn if there are actual compatibility issues
            if not all_compatible:
                if args.dry_run:
                    logger.warning("[yellow][DRY RUN WARNING][/yellow] Geometry type compatibility issues detected:")
                    for issue in compatibility_issues:
                        logger.warning(f"[yellow][DRY RUN WARNING][/yellow]   - {issue}")
                    logger.warning("[yellow][DRY RUN WARNING][/yellow] In a live run, this would require --overwrite or table recreation.")
                    logger.info("[yellow][DRY RUN][/yellow] Continuing with analysis to show what would happen...")
                else:
                    logger.error("[red]Geometry type compatibility issues detected:[/red]")
                    for issue in compatibility_issues:
                        logger.error(f"[red]  - {issue}[/red]")
                    logger.error("[yellow]To resolve this, you can either:[/yellow]")
                    logger.error("  1. Use --overwrite to recreate the table with compatible geometry types")
                    logger.error("  2. Drop the existing table and let dbfriend create it with a generic geometry type")
                    logger.error("  3. Use a different table name")
                    sys.exit(1)
            else:
                logger.info(f"[green]✓[/green] All source files are compatible with table '{schema}.{args.table}' geometry type constraint")

        # Initialize progress bar
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
                    logger.info(f"Processing [cyan]{file}[/]")

                    # Get existing geometry column name and handle renaming
                    existing_geom_col = get_db_geometry_column(conn, table_name, schema=schema)
                    target_geom_col = 'geometry' if existing_geom_col == 'geometry' else 'geom'
                    
                    if gdf.geometry.name != target_geom_col:
                        logger.debug(f"Renaming geometry column from '{gdf.geometry.name}' to '{target_geom_col}'")
                        gdf = gdf.rename_geometry(target_geom_col)
                        gdf.set_geometry(target_geom_col, inplace=True)
                        gdf.set_crs(gdf.crs, inplace=True)

                    if args.table:
                        # Handle --table option
                        gdf = gdf[[target_geom_col]]  # Keep only geometry column
                        
                        if table_name not in existing_tables:
                            srid = args.epsg if args.epsg else (gdf.crs.to_epsg() or 4326)
                            if create_generic_geometry_table(conn, engine, table_name, srid, schema, dry_run=args.dry_run):
                                existing_tables.append(table_name)
                            else:
                                continue

                            if append_geometries(conn, engine, gdf, table_name, schema, dry_run=args.dry_run):
                                total_new += len(gdf)
                                logger.info(f"{'[yellow][DRY RUN MODE][/yellow] Would append' if args.dry_run else 'Appended'} {format(len(gdf), ',').replace(',', ' ')} [green]new[/] geometries to '{qualified_table}'")
                        else:
                            new_geoms, updated_geoms, identical_geoms = compare_geometries(
                                gdf, conn, table_name, target_geom_col, schema=schema,
                                exclude_columns=[], args=args, engine=engine
                            )

                            num_new = len(new_geoms) if new_geoms is not None else 0
                            num_updated = len(updated_geoms) if updated_geoms is not None else 0
                            num_identical = len(identical_geoms) if identical_geoms is not None else 0

                            logger.info(f"Found {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries, "
                                      f"{format(num_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries, and "
                                      f"{format(num_identical, ',').replace(',', ' ')} [red]identical[/] geometries")

                            if new_geoms is not None and not new_geoms.empty:
                                if not args.dry_run:
                                    new_geoms.to_postgis(
                                        name=table_name,
                                        con=engine,
                                        schema=schema,
                                        if_exists='append',
                                        index=False
                                    )
                                total_new += num_new
                                logger.info(f"{'[yellow][DRY RUN MODE][/yellow] Would append' if args.dry_run else 'Successfully appended'} {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries")

                            if updated_geoms is not None and not updated_geoms.empty:
                                update_geometries(updated_geoms, table_name, engine, conn, schema=schema, dry_run=args.dry_run)
                                total_updated += num_updated
                                logger.info(f"{'[yellow][DRY RUN MODE][/yellow] Would update' if args.dry_run else 'Successfully updated'} {format(num_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries")

                            if identical_geoms is not None:
                                total_identical += num_identical
                    
                    elif table_name in existing_tables:
                        # Handle existing table without --table option
                        logger.info(f"Analyzing differences for existing table '[cyan]{qualified_table}[/]'")
                        
                        new_geoms, updated_geoms, identical_geoms = compare_geometries(
                            gdf, conn, table_name, target_geom_col, schema=schema,
                            exclude_columns=exclude_cols, args=args, engine=engine
                        )

                        num_new = len(new_geoms) if new_geoms is not None else 0
                        num_updated = len(updated_geoms) if updated_geoms is not None else 0
                        num_identical = len(identical_geoms) if identical_geoms is not None else 0

                        logger.info(f"Found {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries, "
                                  f"{format(num_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries, and "
                                  f"{format(num_identical, ',').replace(',', ' ')} [red]identical[/] geometries")

                        if num_new > 0:
                            try:
                                if not args.dry_run:
                                    new_geoms.to_postgis(
                                        name=table_name,
                                        con=engine,
                                        schema=schema,
                                        if_exists='append',
                                        index=False
                                    )
                                total_new += num_new
                                logger.info(f"{'[DRY RUN] Would append' if args.dry_run else 'Successfully appended'} {format(num_new, ',').replace(',', ' ')} [green]new[/] geometries")
                            except Exception as e:
                                if not args.dry_run:
                                    logger.error(f"[red]Error appending new geometries: {e}[/red]")

                        if num_updated > 0:
                            update_geometries(updated_geoms, table_name, engine, conn, schema=schema, dry_run=args.dry_run)
                            total_updated += num_updated
                            logger.info(f"{'[DRY RUN] Would update' if args.dry_run else 'Successfully updated'} {format(num_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries")

                        total_identical += num_identical

                    else:
                        # Handle new table creation
                        logger.info(f"Creating new table '[cyan]{qualified_table}[/]'")
                        
                        if args.coordinates:
                            for _, row in gdf.iterrows():
                                print_geometry_details(row, "NEW", args.coordinates)

                        try:
                            if args.dry_run:
                                logger.info(f"[yellow][DRY RUN MODE][/yellow] Would create new table '[cyan]{qualified_table}[/]'")
                                logger.info(f"[yellow][DRY RUN MODE][/yellow] Would import {format(len(gdf), ',').replace(',', ' ')} [green]new[/] geometries to '[cyan]{qualified_table}[/]'")
                                logger.info(f"[yellow][DRY RUN MODE][/yellow] Would create spatial index on table '{qualified_table}'")
                                total_new += len(gdf)
                            else:
                                with conn.cursor() as cursor:
                                    # Safely create new table using parameterized query
                                    quoted_schema = quote_identifier(schema)
                                    quoted_table = quote_identifier(table_name)
                                    quoted_geom = quote_identifier(target_geom_col)
                                    
                                    gdf.to_postgis(
                                        name=table_name,
                                        con=engine,
                                        schema=schema,
                                        if_exists='replace',
                                        index=False
                                    )

                                    # Verify table creation
                                    cursor.execute("""
                                        SELECT EXISTS (
                                            SELECT 1
                                            FROM information_schema.tables
                                            WHERE table_schema = %s
                                            AND table_name = %s
                                        );
                                    """, (schema, table_name))
                                    
                                    if cursor.fetchone()[0]:
                                        create_spatial_index(conn, table_name, schema=schema, 
                                                          geom_column=target_geom_col)
                                        existing_tables.append(table_name)
                                        total_new += len(gdf)
                                        logger.info(f"Successfully imported {format(len(gdf), ',').replace(',', ' ')} [green]new[/] geometries to '[cyan]{qualified_table}[/]'")
                                    else:
                                        logger.error(f"[red]Failed to create table '{qualified_table}'[/red]")

                        except Exception as e:
                            logger.error(f"[red]Error importing '{file}': {e}[/red]")
                            continue

                except Exception as e:
                    logger.error(f"[red]Error processing '{file}': {e}[/red]")
                    continue

                progress.advance(task)

        # Commit all changes (unless in dry-run mode)
        if not args.dry_run:
            conn.commit()
            logger.info("[green]All changes committed successfully[/green]")
        else:
            logger.info("[yellow][DRY RUN MODE][/yellow] No changes were made to the database")
        
        # Print final summary with rich formatting
        action_word = "would be" if args.dry_run else "were"
        logger.info(f"\n[bold]Summary of operations{' [yellow](DRY RUN MODE)[/yellow]' if args.dry_run else ''}:[/bold]\n"
                   f"• {format(total_new, ',').replace(',', ' ')} [green]new[/] geometries {action_word} added\n"
                   f"• {format(total_updated, ',').replace(',', ' ')} [yellow]updated[/] geometries {action_word} modified\n"
                   f"• {format(total_identical, ',').replace(',', ' ')} [red]identical[/] geometries {action_word} skipped")

    except Exception as e:
        if not args.dry_run:
            conn.rollback()
        logger.error(f"[red]An error occurred: {e}[/red]")
        raise

    return total_new, total_updated, total_identical

def check_crs_compatibility(gdf, conn, table_name, geom_column, args, schema='public'):
    """
    Check CRS compatibility between new data and existing table.
    
    Args:
        gdf: GeoDataFrame with new data
        conn: Database connection
        table_name: Name of the target table
        geom_column: Name of the geometry column
        args: Command line arguments
        schema: Database schema (default: 'public')
        
    Returns:
        GeoDataFrame or None: Returns the (possibly reprojected) GeoDataFrame or None if skipped
    """
    try:
        with conn.cursor() as cursor:
            # Check if the table exists using parameterized query
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table_name))
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                logger.debug(f"Table '{schema}.{table_name}' does not exist, proceeding without CRS check")
                return gdf

            # Get existing SRID using quoted identifiers for table/column names
            quoted_schema = quote_identifier(schema)
            quoted_table = quote_identifier(table_name)
            quoted_geom = quote_identifier(geom_column)
            
            cursor.execute(f"""
                SELECT ST_SRID({quoted_geom}) 
                FROM {quoted_schema}.{quoted_table} 
                WHERE {quoted_geom} IS NOT NULL 
                LIMIT %s;
            """, (1,))
            result = cursor.fetchone()
            
            if result:
                existing_srid = result[0]
                logger.info(f"Existing SRID for '{schema}.{table_name}' is {existing_srid}")
            else:
                logger.warning(f"No geometries found in '{schema}.{table_name}' to determine SRID")
                return gdf

        # Rest of the function remains the same as it doesn't involve SQL
        new_srid = gdf.crs.to_epsg() if gdf.crs else None
        
        if new_srid is None:
            logger.warning(f"No EPSG code found for the CRS of the new data for '{schema}.{table_name}'")
            if args.overwrite:
                action = 'y'
            else:
                action = console.input(f"Proceed without CRS check for '{schema}.{table_name}'? (y/n): ")
            
            if action.lower() != 'y':
                logger.info(f"Skipping '{schema}.{table_name}' due to unknown CRS")
                return None
            return gdf
        
        logger.info(f"CRS of new data for '{schema}.{table_name}' is EPSG:{new_srid}")

        if existing_srid != new_srid:
            logger.warning(f"CRS mismatch for '{schema}.{table_name}': "
                         f"Existing SRID {existing_srid}, New SRID {new_srid}")
            
            if args.overwrite:
                action = 'y'
            else:
                action = console.input(f"Reproject new data to SRID {existing_srid}? (y/n): ")
            
            if action.lower() == 'y':
                try:
                    gdf = gdf.to_crs(epsg=existing_srid)
                    logger.info(f"Reprojected new data to SRID {existing_srid}")
                except Exception as e:
                    logger.error(f"Error reprojecting data for '{schema}.{table_name}': {e}")
                    return None
            else:
                logger.info(f"Skipping '{schema}.{table_name}' due to CRS mismatch")
                return None
        else:
            logger.info(f"CRS is compatible for '{schema}.{table_name}'")

        return gdf

    except Exception as e:
        logger.error(f"Error checking CRS compatibility: {e}")
        return None

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
        
        # Set search path using quoted identifiers
        if exists:
            try:
                quoted_schema = quote_identifier(schema_name)
                cursor.execute(f"SET search_path TO {quoted_schema}, public;")
            except ValueError as e:
                logger.error(f"Invalid schema name: {e}")
                return False
        
        return exists

def is_spatial_file(filepath):
    """Check if file is processable by geopandas/dbfriend"""
    try:
        # Quick test - just try to open without fully reading
        with fiona.open(filepath) as src:
            return True
    except:
        return False

def scan_directory_for_spatial_files(directory_path):
    """Scan directory for spatial files and return their metadata."""
    files_info = {}
    
    try:
        for file_path in Path(directory_path).iterdir():
            if not file_path.is_file():
                continue
                
            # Skip the deploy state file specifically
            if file_path.name == '.dbfriend_deploy.json':
                continue
                
            # Try to verify it's actually a spatial file by opening with fiona
            try:
                with fiona.open(str(file_path)) as src:
                    # If we can open it, it's spatial
                    stat = file_path.stat()
                    files_info[file_path.name] = {
                        'size': stat.st_size,
                        'mtime': stat.st_mtime,
                        'full_path': str(file_path)
                    }
            except:
                # Silently skip files that can't be opened as spatial
                continue
                
    except Exception as e:
        logger.error(f"Error scanning directory {directory_path}: {e}")
    
    return files_info

def load_deploy_state(state_file_path):
    """Load the deploy state from JSON file."""
    try:
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r') as f:
                return json.load(f)
        else:
            return {
                'deploy_started': datetime.datetime.now().isoformat(),
                'last_scan': None,
                'files': {}
            }
    except Exception as e:
        logger.error(f"Error loading deploy state: {e}")
        return {'deploy_started': datetime.datetime.now().isoformat(), 'last_scan': None, 'files': {}}

def save_deploy_state(state_file_path, state_data):
    """Save the deploy state to JSON file."""
    try:
        state_data['last_scan'] = datetime.datetime.now().isoformat()
        with open(state_file_path, 'w') as f:
            json.dump(state_data, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving deploy state: {e}")

def needs_processing(filename, current_size, current_mtime, known_files):
    """Check if file needs processing based on metadata."""
    if filename not in known_files.get('files', {}):
        return True  # New file
    
    file_info = known_files['files'][filename]
    if (file_info.get('size') != current_size or 
        file_info.get('mtime') != current_mtime):
        return True  # File changed
    
    return False  # No changes

def process_and_update_state(file_info, state_file_path, args, conn, engine, existing_tables, schema):
    """Process a single file and update the state."""
    filename = file_info['filename']
    filepath = file_info['full_path']
    
    logger.info(f"[bold]Processing detected file: [cyan]{filename}[/cyan][/bold]")
    
    try:
        # Create a temporary args object for this single file
        temp_args = argparse.Namespace(**vars(args))
        temp_args.filepath = os.path.dirname(filepath)
        
        # Process just this one file
        # We'll reuse the existing process_files logic but filter to one file
        table_name = os.path.splitext(filename)[0].lower()
        
        try:
            gdf = gpd.read_file(filepath)
            source_crs = gdf.crs
            
            # Handle CRS (same logic as process_files)
            if args.epsg:
                if source_crs and source_crs.to_epsg() != args.epsg:
                    logger.info(f"[yellow]Reprojecting[/] from EPSG:{source_crs.to_epsg()} to EPSG:{args.epsg}")
                    gdf.set_crs(source_crs, inplace=True)
                    gdf = gdf.to_crs(epsg=args.epsg)
                else:
                    gdf.set_crs(epsg=args.epsg, inplace=True)
            elif not source_crs:
                logger.warning(f"No CRS found in {filename}, defaulting to [yellow]EPSG:4326[/]")
                gdf.set_crs(epsg=4326, inplace=True)

            # Process the file (simplified version of process_files logic)
            target_table = args.table if args.table else table_name
            qualified_table = f"{schema}.{target_table}"
            
            # Handle geometry column naming
            existing_geom_col = get_db_geometry_column(conn, target_table, schema=schema)
            target_geom_col = 'geometry' if existing_geom_col == 'geometry' else 'geom'
            
            if gdf.geometry.name != target_geom_col:
                gdf = gdf.rename_geometry(target_geom_col)
                gdf.set_geometry(target_geom_col, inplace=True)
                gdf.set_crs(gdf.crs, inplace=True)

            if target_table in existing_tables:
                # Existing table - append new geometries
                new_geoms, _, identical_geoms = compare_geometries(
                    gdf, conn, target_table, target_geom_col, schema=schema, 
                    exclude_columns=[], args=args, engine=engine
                )
                
                num_new = len(new_geoms) if new_geoms is not None else 0
                num_identical = len(identical_geoms) if identical_geoms is not None else 0
                
                if num_new > 0:
                    new_geoms.to_postgis(
                        name=target_table,
                        con=engine,
                        schema=schema,
                        if_exists='append',
                        index=False
                    )
                    logger.info(f"[green]✓[/green] Added {num_new} new geometries to '{qualified_table}'")
                
                if num_identical > 0:
                    logger.info(f"[yellow]→[/yellow] Skipped {num_identical} identical geometries")
            else:
                # New table
                gdf.to_postgis(
                    name=target_table,
                    con=engine,
                    schema=schema,
                    if_exists='replace',
                    index=False
                )
                create_spatial_index(conn, target_table, schema=schema, geom_column=target_geom_col)
                existing_tables.append(target_table)
                logger.info(f"[green]✓[/green] Created new table '{qualified_table}' with {len(gdf)} geometries")
            
            conn.commit()
            status = 'success'
            error_msg = None
            
        except Exception as e:
            conn.rollback()
            status = 'failed'
            error_msg = str(e)
            logger.error(f"[red]✗[/red] Failed to process {filename}: {e}")
    
    except Exception as e:
        status = 'failed'
        error_msg = str(e)
        logger.error(f"[red]✗[/red] Error processing {filename}: {e}")
    
    # Update state
    state_data = load_deploy_state(state_file_path)
    state_data['files'][filename] = {
        'size': file_info['size'],
        'mtime': file_info['mtime'],
        'last_processed': datetime.datetime.now().isoformat(),
        'status': status,
        'error': error_msg
    }
    save_deploy_state(state_file_path, state_data)

def deploy_mode(args, conn, engine, existing_tables, schema):
    """Main deploy mode function."""
    deploy_file = os.path.join(args.filepath, '.dbfriend_deploy.json')
    
    logger.info("[bold cyan]🚀 Deploy mode started[/bold cyan]")
    logger.info(f"[cyan]Monitoring:[/cyan] {args.filepath}")
    logger.info(f"[cyan]State file:[/cyan] {deploy_file}")
    logger.info(f"[cyan]Schema:[/cyan] {schema}")
    if args.table:
        logger.info(f"[cyan]Target table:[/cyan] {args.table}")
    logger.info("[yellow]Press Ctrl+C to stop[/yellow]\n")
    
    # Initial scan to establish baseline
    current_files = scan_directory_for_spatial_files(args.filepath)
    logger.info(f"Found {len(current_files)} spatial files to monitor")
    
    try:
        while True:
            # Add timestamp log for each scan interval
            current_timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
            logger.info(f"[dim]{current_timestamp} - dbfriend in deploy mode - starting interval scan...[/dim]")
            
            current_files = scan_directory_for_spatial_files(args.filepath)
            known_files = load_deploy_state(deploy_file)
            
            processed_any = False
            for filename, file_meta in current_files.items():
                if needs_processing(filename, file_meta['size'], file_meta['mtime'], known_files):
                    file_info = {
                        'filename': filename,
                        'full_path': file_meta['full_path'],
                        'size': file_meta['size'],
                        'mtime': file_meta['mtime']
                    }
                    process_and_update_state(file_info, deploy_file, args, conn, engine, existing_tables, schema)
                    processed_any = True
            
            if not processed_any:
                logger.info("[dim]No changes detected in monitored files[/dim]")
            
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("\n[yellow]📋 Deploy mode stopped by user[/yellow]")
        
        # Show final status
        state_data = load_deploy_state(deploy_file)
        total_files = len(state_data.get('files', {}))
        successful = len([f for f in state_data.get('files', {}).values() if f.get('status') == 'success'])
        failed = len([f for f in state_data.get('files', {}).values() if f.get('status') == 'failed'])
        
        logger.info(f"[bold]Final Status:[/bold] {total_files} files processed")
        if successful > 0:
            logger.info(f"[green]✓ {successful} successful[/green]")
        if failed > 0:
            logger.info(f"[red]✗ {failed} failed[/red]")

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
                cursor.execute("SET search_path TO %s, public;", (args.schema,))
            schema = args.schema
        else:
            schema = 'public'

        # Create SQLAlchemy engine with specific isolation level
        logger.debug("Creating SQLAlchemy engine...")
        engine = create_engine(
            f'postgresql://{args.dbuser}:{args.password}@{args.host}:{args.port}/{args.dbname}',
            isolation_level='READ COMMITTED'
        )

        # Switch to transaction mode for the main operations (unless dry-run or deploy)
        if not args.dry_run and not args.deploy:
            conn.autocommit = False
        elif args.dry_run:
            # Keep autocommit on for dry-run mode since we won't be making changes
            logger.info("[yellow][DRY RUN MODE][/yellow] Database will remain in read-only mode")

        logger.debug("Getting existing tables...")
        existing_tables = get_existing_tables(conn, schema=schema)

        if args.deploy:
            # Switch to deploy mode
            conn.autocommit = False  # Deploy mode needs transactions
            deploy_mode(args, conn, engine, existing_tables, schema)
        else:
            # Regular processing mode
            logger.debug("Starting file processing...")
            process_files(args, conn, engine, existing_tables, schema)

    except Exception as e:
        if conn and not conn.autocommit and not args.dry_run:
            conn.rollback()
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            logger.debug("Closing database connection...")
            conn.close()

if __name__ == '__main__':
    main()
