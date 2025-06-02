import subprocess
import gzip
import os
import platform
import shutil
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, List, Tuple
from rich.console import Console
from rich.logging import RichHandler

from .config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger("postgres-backup")
console = Console()

class DatabaseOperations:
    def __init__(self):
        self.config = Config
        self.config.ensure_backup_dir()
        self.is_windows = platform.system() == "Windows"
        self.pg_bin_dir = self._get_pg_bin_dir()

    def _get_pg_bin_dir(self) -> str:
        """Get PostgreSQL binary directory based on OS."""
        if self.is_windows:
            # Common pgAdmin installation paths
            pgadmin_paths = [
                r"C:\Program Files\pgAdmin 4\bin",
                r"C:\Program Files\PostgreSQL\17\bin",
                r"C:\Program Files\PostgreSQL\16\bin",
                r"C:\Program Files\PostgreSQL\15\bin",
                r"C:\Program Files\PostgreSQL\14\bin",
            ]
            
            for path in pgadmin_paths:
                if os.path.exists(path):
                    return path
            
            # If not found in common paths, try to find in PATH
            pg_dump_path = shutil.which("pg_dump")
            if pg_dump_path:
                return os.path.dirname(pg_dump_path)
            
            raise RuntimeError(
                "PostgreSQL binaries not found. Please ensure pgAdmin or PostgreSQL is installed "
                "and the bin directory is in your PATH."
            )
        else:
            # On Linux/Unix systems, PostgreSQL binaries are typically in PATH
            return ""

    def _get_command_path(self, command: str) -> str:
        """Get full path to PostgreSQL command."""
        if self.is_windows:
            return os.path.join(self.pg_bin_dir, f"{command}.exe")
        return command

    def _get_pg_versions(self) -> Tuple[str, str]:
        """Get PostgreSQL server and pg_dump versions."""
        try:
            # Get server version
            server_cmd = [
                self._get_command_path("psql"),
                "-h", self.config.DB_HOST,
                "-p", self.config.DB_PORT,
                "-U", self.config.DB_USER,
                "-d", self.config.DB_NAME,
                "-t",
                "-c", "SELECT version();"
            ]
            env = os.environ.copy()
            env["PGPASSWORD"] = self.config.DB_PASSWORD
            server_result = subprocess.run(server_cmd, env=env, capture_output=True, text=True)
            server_version = server_result.stdout.strip().split()[1]

            # Get pg_dump version
            dump_cmd = [self._get_command_path("pg_dump"), "--version"]
            dump_result = subprocess.run(dump_cmd, capture_output=True, text=True)
            dump_version = dump_result.stdout.strip().split()[2]

            return server_version, dump_version
        except Exception as e:
            logger.error(f"Failed to get version information: {str(e)}")
            return "", ""

    def _check_version_compatibility(self) -> bool:
        """Check if pg_dump version is compatible with server version."""
        server_version, dump_version = self._get_pg_versions()
        if not server_version or not dump_version:
            return False

        # Extract major version numbers
        server_major = int(server_version.split('.')[0])
        dump_major = int(dump_version.split('.')[0])

        if dump_major < server_major:
            logger.error(
                f"Version mismatch: PostgreSQL server version ({server_version}) is newer than "
                f"pg_dump version ({dump_version}). Please update pg_dump to version {server_major} or newer."
            )
            return False
        return True

    def _get_backup_filename(self, prefix: str = "backup") -> str:
        """Generate backup filename with timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{prefix}_{timestamp}.sql.gz"

    def _verify_backup_file(self, filepath: Path) -> bool:
        """Verify backup file integrity."""
        if not filepath.exists():
            logger.error(f"Backup file not found: {filepath}")
            return False
        
        if filepath.stat().st_size == 0:
            logger.error("Backup file is empty")
            return False
        
        try:
            with gzip.open(filepath, 'rb') as f:
                # Try to read the first few bytes to verify it's a valid gzip file
                f.read(1)
            return True
        except Exception as e:
            logger.error(f"Backup file verification failed: {str(e)}")
            return False

    def backup(self, schemas: Optional[List[str]] = None, tables: Optional[List[str]] = None) -> bool:
        """Create a database backup."""
        if not self._check_version_compatibility():
            return False

        backup_file = Path(self.config.BACKUP_DIR) / self._get_backup_filename()
        
        # Build pg_dump command
        cmd = [
            self._get_command_path("pg_dump"),
            "-h", self.config.DB_HOST,
            "-p", self.config.DB_PORT,
            "-U", self.config.DB_USER,
            "-d", self.config.DB_NAME,
            "-F", "p",  # Plain SQL format
            "-f", str(backup_file)
        ]

        # Add schema filter if specified
        if schemas:
            for schema in schemas:
                cmd.extend(["-n", schema])

        # Add table filter if specified
        if tables:
            for table in tables:
                cmd.extend(["-t", table])

        try:
            # Set PGPASSWORD environment variable
            env = os.environ.copy()
            env["PGPASSWORD"] = self.config.DB_PASSWORD
            
            logger.info(f"Starting backup to {backup_file}")
            process = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )
            
            if process.returncode != 0:
                logger.error(f"Backup failed: {process.stderr}")
                return False
            
            # Compress the backup file
            try:
                with open(backup_file, 'rb') as f_in:
                    with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                        f_out.writelines(f_in)
                # Remove the uncompressed file
                backup_file.unlink()
                # Rename the compressed file
                Path(f"{backup_file}.gz").rename(backup_file)
            except Exception as e:
                logger.error(f"Compression failed: {str(e)}")
                return False
            
            if not self._verify_backup_file(backup_file):
                return False
            
            logger.info("Backup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            return False

    def restore(self, backup_file: str, schemas: Optional[List[str]] = None, tables: Optional[List[str]] = None) -> bool:
        """Restore database from backup."""
        if not self._check_version_compatibility():
            return False

        backup_path = Path(backup_file)
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False

        # Build psql command for plain SQL format
        cmd = [
            self._get_command_path("psql"),
            "-h", self.config.DB_HOST,
            "-p", self.config.DB_PORT,
            "-U", self.config.DB_USER,
            "-d", self.config.DB_NAME,
            "-v", "ON_ERROR_STOP=1"  # Stop on error
        ]

        # Add schema filter if specified
        if schemas:
            for schema in schemas:
                cmd.extend(["-n", schema])

        # Add table filter if specified
        if tables:
            for table in tables:
                cmd.extend(["-t", table])

        try:
            # Set PGPASSWORD environment variable
            env = os.environ.copy()
            env["PGPASSWORD"] = self.config.DB_PASSWORD
            
            logger.info(f"Starting restore from {backup_file}")
            
            # Decompress and pipe to psql
            with gzip.open(backup_path, 'rb') as f_in:
                process = subprocess.run(
                    cmd,
                    env=env,
                    input=f_in.read(),
                    capture_output=True
                )
            
            if process.returncode != 0:
                logger.error(f"Restore failed: {process.stderr.decode()}")
                return False
            
            logger.info("Restore completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {str(e)}")
            return False

    def get_all_tables(self) -> List[str]:
        """Get all tables in the database.
        
        Returns:
            List[str]: List of tables in format 'schema.table'
        """
        try:
            cmd = [
                self._get_command_path("psql"),
                "-h", self.config.DB_HOST,
                "-p", self.config.DB_PORT,
                "-U", self.config.DB_USER,
                "-d", self.config.DB_NAME,
                "-t",
                "-c", """
                    SELECT schemaname || '.' || tablename
                    FROM pg_tables
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY schemaname, tablename;
                """
            ]
            
            env = os.environ.copy()
            env["PGPASSWORD"] = self.config.DB_PASSWORD
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Failed to get tables: {result.stderr}")
                return []
            
            # Split output into lines and remove empty lines
            tables = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}")
            return []

    def export_to_csv(self, tables: Optional[List[str]] = None, output_dir: Optional[str] = None) -> bool:
        """Export specified tables to CSV files.
        
        Args:
            tables: List of tables to export in format 'schema.table'. If None, exports all tables.
            output_dir: Optional directory to save CSV files. Defaults to backup directory.
        
        Returns:
            bool: True if export was successful, False otherwise
        """
        if not output_dir:
            output_dir = self.config.BACKUP_DIR
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # If no tables specified, get all tables
        if not tables:
            tables = self.get_all_tables()
            if not tables:
                logger.error("No tables found to export")
                return False
            logger.info(f"Found {len(tables)} tables to export")
        
        success = True
        for table in tables:
            try:
                # Split schema and table name
                schema, table_name = table.split('.')
                output_file = output_path / f"{schema}.{table_name}.csv"
                
                # Build COPY command
                cmd = [
                    self._get_command_path("psql"),
                    "-h", self.config.DB_HOST,
                    "-p", self.config.DB_PORT,
                    "-U", self.config.DB_USER,
                    "-d", self.config.DB_NAME,
                    "-c", f"\\COPY {schema}.{table_name} TO '{output_file}' WITH CSV HEADER"
                ]
                
                # Set PGPASSWORD environment variable
                env = os.environ.copy()
                env["PGPASSWORD"] = self.config.DB_PASSWORD
                
                logger.info(f"Exporting {table} to {output_file}")
                process = subprocess.run(cmd, env=env, capture_output=True, text=True)
                
                if process.returncode != 0:
                    logger.error(f"Failed to export {table}: {process.stderr}")
                    success = False
                    continue
                
                logger.info(f"Successfully exported {table} to {output_file}")
                
            except Exception as e:
                logger.error(f"Error exporting {table}: {str(e)}")
                success = False
        
        return success

    def import_from_csv(self, csv_files: Optional[List[str]] = None, input_dir: Optional[str] = None, truncate: bool = False) -> bool:
        """Import data from CSV files into corresponding tables.
        
        Args:
            csv_files: List of CSV file paths to import. If None, imports all CSV files from input_dir.
            input_dir: Directory containing CSV files to import. Required if csv_files is None.
            truncate: If True, truncate tables before importing
        
        Returns:
            bool: True if import was successful, False otherwise
        """
        if not csv_files and not input_dir:
            logger.error("Either csv_files or input_dir must be specified")
            return False
            
        if not csv_files:
            input_path = Path(input_dir)
            if not input_path.exists():
                logger.error(f"Input directory not found: {input_dir}")
                return False
            csv_files = [str(f) for f in input_path.glob("*.csv")]
            if not csv_files:
                logger.error(f"No CSV files found in {input_dir}")
                return False
            logger.info(f"Found {len(csv_files)} CSV files to import")
        
        success = True
        for csv_file in csv_files:
            try:
                file_path = Path(csv_file)
                if not file_path.exists():
                    logger.error(f"CSV file not found: {csv_file}")
                    success = False
                    continue
                
                # Extract schema and table name from filename (format: schema.table.csv)
                schema, table_name = file_path.stem.split('.')
                
                # Build COPY command
                cmd = [
                    self._get_command_path("psql"),
                    "-h", self.config.DB_HOST,
                    "-p", self.config.DB_PORT,
                    "-U", self.config.DB_USER,
                    "-d", self.config.DB_NAME
                ]
                
                # Set PGPASSWORD environment variable
                env = os.environ.copy()
                env["PGPASSWORD"] = self.config.DB_PASSWORD
                
                # Truncate table if requested
                if truncate:
                    truncate_cmd = cmd.copy()
                    truncate_cmd.extend(["-c", f"TRUNCATE TABLE {schema}.{table_name}"])
                    logger.info(f"Truncating table {schema}.{table_name}")
                    truncate_process = subprocess.run(truncate_cmd, env=env, capture_output=True, text=True)
                    if truncate_process.returncode != 0:
                        logger.error(f"Failed to truncate {schema}.{table_name}: {truncate_process.stderr}")
                        success = False
                        continue
                
                # Import data
                import_cmd = cmd.copy()
                import_cmd.extend(["-c", f"\\COPY {schema}.{table_name} FROM '{file_path}' WITH CSV HEADER"])
                
                logger.info(f"Importing data from {csv_file} to {schema}.{table_name}")
                import_process = subprocess.run(import_cmd, env=env, capture_output=True, text=True)
                
                if import_process.returncode != 0:
                    logger.error(f"Failed to import {csv_file}: {import_process.stderr}")
                    success = False
                    continue
                
                logger.info(f"Successfully imported data from {csv_file} to {schema}.{table_name}")
                
            except Exception as e:
                logger.error(f"Error importing {csv_file}: {str(e)}")
                success = False
        
        return success