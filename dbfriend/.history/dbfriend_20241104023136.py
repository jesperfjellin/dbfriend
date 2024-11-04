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
from pathlib import Path
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# Initialize rich Console
console = Console()

# Configure logging with rich
logging.basicConfig(
    level=logging.INFO,  # Default level; will be updated based on arguments
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(rich_tracebacks=True, tracebacks_show_locals=True)
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
    dbfriend help
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
        logger.info("[bold green]Database connection established.")
        return conn
    except Exception as e:
        logger.error(f"[bold red]Database connection failed: {e}")
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
        logger.info(f"[bold cyan]Created spatial index '[white]{index_name}[/white]' on table '[white]{table_name}[/white]'")
    except Exception as e:
        logger.error(f"[bold red]Failed to create spatial index on '[white]{table_name}[/white]': {e}")
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
        logger.warning("[yellow]No spatial files found to process.[/yellow]")
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
                action = console.input(f"[yellow]Geometry column detected as '{geom_col}' for files {file_names}. Rename to 'geom'? (y/n): [/yellow]")
            if action.lower() == 'y':
                for info in file_info_list:
                    info['gdf'] = info['gdf'].rename_geometry('geom')
                    info['renamed'] = True
                logger.info("[green]Geometry columns renamed to 'geom'.[/green]")
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
                    action = console.input(f"[yellow]Geometry column detected as '{geom_col}' for files {files}. Rename to 'geom'? (y/n): [/yellow]")
                if action.lower() == 'y':
                    for info in file_info_list:
                        if info['input_geom_col'] == geom_col:
                            info['gdf'] = info['gdf'].rename_geometry('geom')
                            info['renamed'] = True
                    logger.info(f"[green]Geometry columns renamed to 'geom' for files with '{geom_col}' column.[/green]")
                else:
                    for info in file_info_list:
                        if info['input_geom_col'] == geom_col:
                            info['renamed'] = False

    # Initialize rich Progress
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        console=console,
        transient=True  # Ensures progress bar is removed after completion
    ) as progress:
        task = progress.add_task("[green]Processing files[/green]", total=len(file_info_list))
        for info in file_info_list:
            file = info['file']
            table_name = info['table_name']
            gdf = info['gdf']
            input_geom_col = gdf.geometry.name

            logger.info(f"[blue]Processing file '{file}'.[/blue]")

            # Compatibility Check: Table Name Conflict
            if table_name in existing_tables:
                logger.warning(f"[yellow]Table '{table_name}' already exists.[/yellow]")
                if args.overwrite:
                    action = 'y'
                else:
                    action = console.input(f"[yellow]Table '{table_name}' exists. Overwrite? (y/n): [/yellow]")
                if action.lower() != 'y':
                    logger.info(f"[green]Skipping '{file}'.[/green]")
                    progress.advance(task)
                    continue
                else:
                    logger.info(f"[green]Overwriting table '{table_name}'.[/green]")
            else:
                # Don't add to existing_tables yet; will add after checking
                pass

            # Check if the table exists in the database
            table_exists = check_table_exists(conn, table_name)
            if table_exists:
                db_geom_col = get_db_geometry_column(conn, table_name)
                if db_geom_col:
                    logger.info(f"[blue]Geometry column in database table '{table_name}' is '{db_geom_col}'.[/blue]")
                    if input_geom_col != db_geom_col:
                        if args.rename_geom:
                            action = 'y'
                        else:
                            action = console.input(f"[yellow]The matching table in the PostGIS database uses '{db_geom_col}' as the geometry column name. Rename input file's geometry column from '{input_geom_col}' to '{db_geom_col}'? (y/n): [/yellow]")
                        if action.lower() == 'y':
                            gdf = gdf.rename_geometry(db_geom_col)
                            input_geom_col = db_geom_col
                            logger.info(f"[green]Renamed geometry column to '{db_geom_col}'.[/green]")
                        else:
                            logger.info(f"[green]Skipping '{file}' due to geometry column name mismatch.[/green]")
                            progress.advance(task)
                            continue
                else:
                    logger.info(f"[blue]Table '{table_name}' exists but no geometry column found. Skipping CRS compatibility check.[/blue]")
            else:
                logger.info(f"[blue]Table '{table_name}' does not exist. Skipping geometry and CRS compatibility checks.[/blue]")

            # CRS Compatibility Check
            gdf = check_crs_compatibility(gdf, conn, table_name, input_geom_col, args)
            if gdf is None:
                progress.advance(task)
                continue  # Skip this file if CRS is incompatible or an error occurred

            # Write to PostGIS
            try:
                gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
                logger.info(f"[green]Imported '{file}' to table '{table_name}'.[/green]")
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
        logger.info(f"[blue]Table '{table_name}' does not exist. Skipping CRS compatibility check.[/blue]")
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
            logger.info(f"[blue]Existing SRID for '{table_name}' is {existing_srid}.[/blue]")
        else:
            logger.warning(f"[yellow]No geometries found in '{table_name}' to determine SRID.[/yellow]")
            existing_srid = None
    except Exception as e:
        logger.error(f"[red]Error retrieving SRID for '{table_name}': {e}[/red]")
        conn.rollback()
        cursor.close()
        return None  # Skip this file due to error

    # Get the SRID of the new data
    new_srid = gdf.crs.to_epsg()
    if new_srid is None:
        logger.warning(f"[yellow]No EPSG code found for the CRS of the new data for '{table_name}'.[/yellow]")
        if args.overwrite:
            action = 'y'
        else:
            action = console.input(f"[yellow]Proceed without CRS check for '{table_name}'? (y/n): [/yellow]")
        if action.lower() != 'y':
            logger.info(f"[green]Skipping '{table_name}' due to unknown CRS.[/green]")
            cursor.close()
            return None  # Skip this file
    else:
        logger.info(f"[blue]CRS of new data for '{table_name}' is EPSG:{new_srid}.[/blue]")

    # Compare SRIDs
    if existing_srid and new_srid != existing_srid:
        logger.warning(f"[yellow]CRS mismatch for '{table_name}': Existing SRID {existing_srid}, New SRID {new_srid}[/yellow]")
        if args.overwrite:
            action = 'y'
        else:
            action = console.input(f"[yellow]Reproject new data to SRID {existing_srid}? (y/n): [/yellow]")
        if action.lower() == 'y':
            try:
                gdf = gdf.to_crs(epsg=existing_srid)
                logger.info(f"[green]Reprojected new data to SRID {existing_srid}.[/green]")
            except Exception as e:
                logger.error(f"[red]Error reprojecting data for '{table_name}': {e}[/red]")
                cursor.close()
                return None  # Skip this file due to reprojection error
        else:
            logger.info(f"[green]Skipping '{table_name}' due to CRS mismatch.[/green]")
            cursor.close()
            return None  # Skip this file
    else:
        logger.info(f"[blue]CRS is compatible for '{table_name}'.[/blue]")

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
    logger.info("[green]All tasks completed.[/green]")

if __name__ == '__main__':
    main()
