# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.1.0] - 2024-11-26
### Security
- Add quote_identifier() function to safely quote table/schema names
- Implement parameterized queries throughout code
- Validate identifiers before using them in SQL statements
- Use proper quoting for special characters and uppercase names
- Add schema validation before executing SQL commands

## [1.0.1] - 2024-11-05
### Added
- New feature to add spatial data with compatibility checks.
- Command-line argument --schema for querying specific schemas.

## [1.0.0] - 2024-11-04
### Added
- Initial release of `dbfriend`.
- Command-line tool for loading spatial data into PostGIS databases.
