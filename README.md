

<div align="center">
  <img src="./images/Utqr-wts.png" alt="Project Logo" width="400"/>
</div>

<div align="center">


  
</div>

---

**dbfriend** is a command-line tool for loading and synchronizing spatial data with PostGIS databases. It intelligently handles data updates by comparing geometries and attributes, identifying new, updated, and identical features. 

Key features:
- Supports multiple vector formats (GeoJSON, Shapefile, GeoPackage, KML, GML)
- Smart geometry comparison to prevent duplicates
- Attribute-aware updates for existing geometries
- Automatic geometry column detection and renaming
- CRS compatibility checks and automatic reprojection
- Batch processing with progress tracking
- Spatial index creation for optimized queries



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
    --rename-geom     Automatically rename geometry columns to "geom" without prompting.
    --log-level       Set the logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    --host            Database host (default: localhost).
    --port            Database port (default: 5432).
    --epsg            Target EPSG code for the data. If not specified, will preserve source CRS
                      or default to 4326.

Note: Password will be prompted securely or can be set via DB_PASSWORD environment variable.
```




