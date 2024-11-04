# dbfriend

**dbfriend** is a command-line tool for loading and synchronizing spatial data with PostGIS databases. It intelligently handles data updates by comparing geometries and attributes, identifying new, updated, and identical features. 

Key features:
- Supports multiple vector formats (GeoJSON, Shapefile, GeoPackage, KML, GML)
- Smart geometry comparison to prevent duplicates
- Attribute-aware updates for existing geometries
- Automatic geometry column detection and renaming
- CRS compatibility checks and automatic reprojection
- Batch processing with progress tracking
- Spatial index creation for optimized queries



# Demonstration

<img src="https://github.com/user-attachments/assets/a6d8ddb8-a610-4561-a567-518d48e993c5" width="800px">

