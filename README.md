

<div align="center">
  <img src="./images/Utqr-wts.png" alt="Project Logo" width="250"/>
</div>

<div align="center">


  
</div>

---

**dbfriend** is a command-line tool designed to simplify the loading and synchronization of spatial data into PostGIS databases. It focuses on data integrity and safety, ensuring that your database operations are reliable and efficient. By handling complex tasks intelligently, dbfriend helps GIS professionals and database administrators streamline their workflows.

### 🚀 Features:


- **Transactional Operations for Data Integrity**  
  All database operations are executed within transactions. This means that either all changes are committed successfully, or none are applied. This approach protects your database from partial updates and maintains consistency.

- **Automated Table Backups**  
  Before modifying any existing tables, dbfriend automatically creates backups, keeping up to three historical versions per table. This allows for easy restoration if needed and provides an extra layer of data safety.

- **Supports Multiple Vector Formats**  
  Load data from various spatial file formats, including GeoJSON, Shapefile, GeoPackage, KML, and GML, offering flexibility in handling different data sources.

- **Intelligent Geometry Comparison**  
  Prevent duplicates and ensure data consistency by comparing geometries using hashes to detect new, updated, and identical features efficiently.

- **Attribute-Aware Updates**  
  Update existing geometries based on attribute changes, so your database always reflects the most current data.

- **Automatic Geometry Handling**  
  Automatically detects and renames geometry columns to a standard format, simplifying data processing and integration.

- **CRS Compatibility Checks and Automatic Reprojection**  
  Verifies Coordinate Reference System (CRS) compatibility and automatically reprojects data as needed, ensuring spatial data aligns correctly within your database.

- **Spatial Index Creation for Optimized Queries**  
  Automatically creates spatial indexes on imported data, improving query performance and data retrieval speeds.



## Demonstration

<img src="https://github.com/user-attachments/assets/a6d8ddb8-a610-4561-a567-518d48e993c5" width="800px">

## Installation

```bash
$ pip install git+https://github.com/jesperfjellin/dbfriend.git
```

## Arguments and flags

dbfriend accepts a series of positional arguments and flags. Running dbfriend --help will print out an overview:

```
Usage:
    dbfriend <username> <dbname> <filepath>

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
                      If the schema does not exist, dbfriend will not generate one for safety reasons.
    --table           Target table name. If specified, all data will be loaded into this table. 
                      If the table does not exist, it will be created.
    --coordinates     Print coordinates and attributes for each geometry.
    --no-backup       Do not create backups of existing tables before modifying them.

Note: Password will be prompted securely or can be set via DB_PASSWORD environment variable.
```




