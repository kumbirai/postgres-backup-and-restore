# PostgreSQL Backup and Restore Tool

A Python-based command-line tool for backing up and restoring PostgreSQL databases with support for schema-specific operations and compression.

## Features

- Create compressed database backups
- Restore from backup files
- Support for schema-specific backup and restore
- Configurable through environment variables
- Rich logging and error handling
- Backup integrity verification

## Prerequisites

- Python 3.7+
- PostgreSQL client tools (pg_dump, pg_restore)
- PostgreSQL server

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd postgres-backup-and-restore
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root with the following variables:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
BACKUP_DIR=/path/to/backup/directory  # Optional, defaults to ./backups
COMPRESSION_FORMAT=gzip
```

## Usage

### Creating a Backup

To create a full database backup:
```bash
python -m src.main backup
```

To backup specific schemas:
```bash
python -m src.main backup --schemas public --schemas custom_schema
```

### Restoring from Backup

To restore the entire database:
```bash
python -m src.main restore /path/to/backup_file.sql.gz
```

To restore specific schemas:
```bash
python -m src.main restore /path/to/backup_file.sql.gz --schemas public --schemas custom_schema
```

## Project Structure

```
postgres-backup-and-restore/
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── db_operations.py
│   └── main.py
├── backups/           # Default backup directory
├── requirements.txt
├── README.md
└── .env
```

## Backup File Location

By default, backup files are stored in the `backups/` directory in the project root. You can change this by setting the `BACKUP_DIR` environment variable in your `.env` file.

## Error Handling

The tool includes comprehensive error handling and logging:
- Backup/restore operation failures
- File integrity verification
- Database connection issues
- Invalid schema names

## License

MIT License 