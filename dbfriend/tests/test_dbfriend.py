import pytest
from pandas.testing import assert_frame_equal
from unittest.mock import MagicMock, patch, mock_open, call
from dbfriend.dbfriend import (
    compute_geom_hash,
    get_non_essential_columns,
    parse_arguments,
    check_schema_exists,
    get_db_geometry_column,
    check_table_exists,
    print_geometry_details,
    connect_db,
    check_crs_compatibility,
    get_existing_tables
)
import hashlib
from geopandas import GeoDataFrame, GeoSeries
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import Point
import psycopg2
import sys

def normalize_sql(sql):
    # Remove newlines and extra spaces
    sql = ' '.join(sql.split())
    # Remove spaces before semicolons and parentheses
    sql = sql.replace(' ;', ';').replace(' )', ')')
    # Remove spaces after parentheses
    sql = sql.replace('( ', '(')
    return sql

# 1. Testing compute_geom_hash
def test_compute_geom_hash():
    # Create a mock geometry with known WKB
    point = Point(1.0, 2.0)
    expected_hash = hashlib.md5(point.wkb).hexdigest()
    
    # Call the function
    result = compute_geom_hash(point)
    
    # Assert the hash matches the expected value
    assert result == expected_hash

# 2. Testing get_non_essential_columns
def test_get_non_essential_columns(mocker):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Define what cursor.fetchall() should return for each execute call
    # First execute: all columns
    mock_cursor.fetchall.side_effect = [
        [('id',), ('name',), ('created_at',), ('geom',)],
        [('id',)],
        [('id',), ('created_at',)]
    ]
    
    # Call the function
    exclude_columns = get_non_essential_columns(mock_conn, 'test_table')

    # Define expected excluded columns based on patterns and metadata
    expected_exclude = {'id', 'created_at'}

    # Assert the excluded columns match
    assert exclude_columns == expected_exclude

    # Assert the correct SQL queries were executed
    assert mock_cursor.execute.call_count == 3

# 3. Testing parse_arguments
def test_parse_arguments_help(mocker):
    # Simulate passing --help
    mocker.patch('sys.argv', ['dbfriend', '--help'])

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        parse_arguments()
    
    # Assert that the system exited with code 0
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0

def test_parse_arguments_defaults(mocker):
    # Simulate passing only required positional arguments
    mocker.patch('sys.argv', ['dbfriend', 'user', 'dbname', '/path/to/files'])

    args = parse_arguments()

    # Assert default values
    assert args.dbuser == 'user'
    assert args.dbname == 'dbname'
    assert args.filepath == '/path/to/files'
    assert not args.overwrite
    assert args.log_level == 'INFO'
    assert args.host == 'localhost'
    assert args.port == '5432'
    assert args.epsg is None
    assert args.schema is None
    assert not args.coordinates

def test_parse_arguments_with_options(mocker):
    # Simulate passing various options
    mocker.patch('sys.argv', [
        'dbfriend', 'user', 'dbname', '/path/to/files',
        '--overwrite', '--log-level', 'DEBUG', '--host', '127.0.0.1',
        '--port', '5433', '--epsg', '3857', '--schema', 'public',
        '--coordinates'
    ])

    args = parse_arguments()

    # Assert values are set correctly
    assert args.overwrite
    assert args.log_level == 'DEBUG'
    assert args.host == '127.0.0.1'
    assert args.port == '5433'
    assert args.epsg == 3857
    assert args.schema == 'public'
    assert args.coordinates

# 4. Testing check_schema_exists
def test_check_schema_exists_exists(mocker):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Mock fetchall() to return a list of existing schemas
    mock_cursor.fetchall.return_value = [('public',), ('schema2',)]

    # Mock fetchone() to simulate that the schema exists
    mock_cursor.fetchone.return_value = (True,)

    # Call the function
    exists = check_schema_exists(mock_conn, 'public')

    # Assert
    assert exists is True

    # Normalize SQL queries
    actual_calls = [
        normalize_sql(call_args[0][0]) for call_args in mock_cursor.execute.call_args_list
    ]
    expected_calls = [
        normalize_sql("""
            SELECT schema_name
            FROM information_schema.schemata;
        """),
        normalize_sql("""
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = %s
            );
        """)
    ]

    # Assert that the normalized SQL queries were executed
    assert actual_calls == expected_calls

def test_check_schema_exists_not_exists(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('public',), ('schema2',)]
    mock_cursor.fetchone.return_value = (False,)

    exists = check_schema_exists(mock_conn, 'nonexistent_schema')
    assert exists is False

    actual_calls = [normalize_sql(call.args[0]) for call in mock_cursor.execute.mock_calls]
    expected_calls = [
        normalize_sql("SELECT schema_name FROM information_schema.schemata;"),
        normalize_sql("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);")
    ]
    assert actual_calls == expected_calls

# 5. Testing get_db_geometry_column
def test_get_db_geometry_column_exists(mocker):
    # Mock setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = ('geom',)

    # Call the function
    geom_col = get_db_geometry_column(mock_conn, 'test_table', 'public')

    # Assert
    assert geom_col == 'geom'
    expected_sql = """
    SELECT f_geometry_column
    FROM geometry_columns
    WHERE f_table_schema = %s AND f_table_name = %s;
    """

    # Normalize SQL strings
    import re
    actual_sql = re.sub(r'\s+', ' ', mock_cursor.execute.call_args[0][0]).strip()
    expected_sql = re.sub(r'\s+', ' ', expected_sql).strip()

    assert actual_sql == expected_sql
    mock_cursor.execute.assert_called_with(mock_cursor.execute.call_args[0][0], ('public', 'test_table'))

def test_get_db_geometry_column_fallback(mocker):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Simulate geometry_columns has no entry
    mock_cursor.fetchone.side_effect = [None, ('geometry',)]

    # Call the function
    geom_col = get_db_geometry_column(mock_conn, 'test_table', 'public')

    # Assert
    assert geom_col == 'geometry'
    assert mock_cursor.execute.call_count == 2
    mock_cursor.close.assert_called()

def test_get_db_geometry_column_none(mocker):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Simulate no geometry column found
    mock_cursor.fetchone.side_effect = [None, None]

    # Call the function
    geom_col = get_db_geometry_column(mock_conn, 'test_table', 'public')

    # Assert
    assert geom_col is None
    assert mock_cursor.execute.call_count == 2
    mock_cursor.close.assert_called()

# 6. Testing check_table_exists
def test_check_table_exists_true(mocker):
    # Mock setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (True,)

    # Call the function
    exists = check_table_exists(mock_conn, 'existing_table', 'public')

    # Assert
    assert exists is True

    # Normalize SQL strings
    import re
    actual_sql = re.sub(r'\s+', ' ', mock_cursor.execute.call_args[0][0]).strip()
    expected_sql = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    );
    """
    expected_sql = re.sub(r'\s+', ' ', expected_sql).strip()

    assert actual_sql == expected_sql
    mock_cursor.execute.assert_called_with(mock_cursor.execute.call_args[0][0], ('public', 'existing_table'))

def test_check_table_exists_false(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (False,)

    exists = check_table_exists(mock_conn, 'nonexistent_table', 'public')
    assert exists is False

    # Get actual and expected SQL with exact same formatting
    actual_sql = normalize_sql(mock_cursor.execute.call_args[0][0])
    expected_sql = normalize_sql("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
    """)
    assert actual_sql == expected_sql
    assert mock_cursor.execute.call_args[0][1] == ('public', 'nonexistent_table')

# 7. Testing print_geometry_details
def test_print_geometry_details_no_coordinates(mocker):
    row = {
        'geometry': Point(1.0, 2.0),
        'name': 'Test Point'
    }
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')
    print_geometry_details(row, coordinates_enabled=False)
    mock_logger.info.assert_called_once_with('Test Point')

def test_print_geometry_details_with_coordinates(mocker):
    row = {
        'geom': Point(1.0, 2.0),
        'name': 'Test Point'
    }
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')
    
    with patch('builtins.open', mock_open()) as mock_file:
        print_geometry_details(row, status="TEST", coordinates_enabled=True)
    
    # Assert logger calls
    expected_calls = [
        call("\nTEST Geometry Details:"),
        call("Attributes: name: Test Point"),
        call("Coordinates: (1.000000, 2.000000)")
    ]
    
    assert mock_logger.info.call_count == 3
    mock_logger.info.assert_has_calls(expected_calls, any_order=False)
    
    # Assert file writing
    mock_file().write.assert_called_once_with(
        "\nTEST Geometry Details:\nAttributes: name: Test Point\nCoordinates: (1.000000, 2.000000)\n"
    )

# 8. Testing connect_db
def test_connect_db_success(mocker):
    # Mock psycopg2.connect to return a mock connection
    mock_conn = MagicMock()
    mocker.patch('psycopg2.connect', return_value=mock_conn)

    # Mock logger
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')

    # Call the function
    conn = connect_db('dbname', 'user', 'localhost', '5432', 'password')

    # Assert psycopg2.connect was called with correct parameters
    psycopg2.connect.assert_called_once_with(
        dbname='dbname',
        user='user',
        host='localhost',
        port='5432',
        password='password'
    )

    # Assert logger.info was called
    mock_logger.info.assert_called_with("Database connection established ✓")

    # Assert the returned connection is the mock
    assert conn == mock_conn

def test_connect_db_failure(mocker):
    # Mock psycopg2.connect to raise an exception
    mocker.patch('psycopg2.connect', side_effect=Exception("Connection failed"))

    # Mock logger and sys.exit
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')
    mock_sys_exit = mocker.patch('sys.exit')

    # Call the function
    connect_db('dbname', 'user', 'localhost', '5432', 'password')

    # Assert psycopg2.connect was called
    psycopg2.connect.assert_called_once()

    # Assert logger.error was called with the exception message
    mock_logger.error.assert_called_with("Database connection failed: Connection failed")

    # Assert sys.exit was called with code 1
    mock_sys_exit.assert_called_once_with(1)

# 9. Testing check_crs_compatibility
def test_check_crs_compatibility_table_not_exists(mocker):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Simulate table does not exist
    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.return_value = (False,)

    # Create a mock GeoDataFrame
    gdf = GeoDataFrame({'geometry': GeoSeries([Point(1, 2)])}, crs='EPSG:4326')

    # Call the function
    result = check_crs_compatibility(gdf, mock_conn, 'nonexistent_table', 'geom', MagicMock())


    # Assert the GeoDataFrame is returned as is
    assert_frame_equal(result, gdf)

    # Assert cursor was closed
    mock_cursor.close.assert_called_once()

def test_check_crs_compatibility_compatible(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (4326,)

    gdf = GeoDataFrame({'geometry': GeoSeries([Point(1, 2)])}, crs='EPSG:4326')
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')

    result = check_crs_compatibility(gdf, mock_conn, 'existing_table', 'geom', MagicMock())
    assert_geodataframe_equal(result, gdf)

def test_check_crs_compatibility_incompatible_overwrite(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (4326,)

    gdf = GeoDataFrame({'geometry': GeoSeries([Point(1, 2)])}, crs='EPSG:3857')
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')
    mock_console = mocker.patch('dbfriend.dbfriend.console')
    mock_console.input.return_value = 'y'

    with patch.object(gdf, 'to_crs', return_value=gdf) as mock_to_crs:
        args = MagicMock()
        args.overwrite = True
        result = check_crs_compatibility(gdf, mock_conn, 'existing_table', 'geom', args)
        mock_to_crs.assert_called_once_with(epsg=4326)

    mock_logger.info.assert_any_call("Reprojected new data to SRID 4326")
    assert_geodataframe_equal(result, gdf)

def test_check_crs_compatibility_incompatible_skip(mocker):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Simulate table exists with SRID 4326
    mock_cursor.fetchone.return_value = (4326,)

    # Create a mock GeoDataFrame with different CRS
    gdf = GeoDataFrame({'geometry': GeoSeries([Point(1, 2)])}, crs='EPSG:3857')

    # Mock logger and console.input
    mock_logger = mocker.patch('dbfriend.dbfriend.logger')
    mock_console = mocker.patch('dbfriend.dbfriend.console')
    mock_console.input.return_value = 'n'

    # Call the function
    args = MagicMock()
    args.overwrite = False

    result = check_crs_compatibility(gdf, mock_conn, 'existing_table', 'geom', args)

    # Assert logger.info was called about skipping
    mock_logger.info.assert_called_with("Skipping 'existing_table' due to CRS mismatch")

    # Assert the function returns None
    assert result is None

    # Assert cursor was closed
    mock_cursor.close.assert_called_once()

# 10. Testing get_existing_tables
def test_get_existing_tables(mocker):
    # Mock setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('table1',), ('table2',)]

    # Call the function
    tables = get_existing_tables(mock_conn, 'public')

    # Assert
    assert tables == ['table1', 'table2']

    # Normalize SQL strings
    import re
    actual_sql = re.sub(r'\s+', ' ', mock_cursor.execute.call_args[0][0]).strip()
    expected_sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s;
    """
    expected_sql = re.sub(r'\s+', ' ', expected_sql).strip()

    assert actual_sql == expected_sql
    mock_cursor.execute.assert_called_with(mock_cursor.execute.call_args[0][0], ('public',))
