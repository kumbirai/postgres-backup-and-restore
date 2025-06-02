#!/usr/bin/env python3
import click
from typing import List
from rich.console import Console
from rich.panel import Panel

from .db_operations import DatabaseOperations

console = Console()

@click.group()
def cli():
    """PostgreSQL Database Backup and Restore Tool"""
    pass

@cli.command()
@click.option('--schemas', '-s', multiple=True, help='Specific schemas to backup')
@click.option('--tables', '-t', multiple=True, help='Specific tables to backup (format: schema.table)')
def backup(schemas, tables):
    """Create a database backup"""
    db_ops = DatabaseOperations()
    schemas_list = list(schemas) if schemas else None
    tables_list = list(tables) if tables else None
    
    console.print(Panel.fit(
        f"Starting backup operation\n"
        f"Database: {db_ops.config.DB_NAME}\n"
        f"Host: {db_ops.config.DB_HOST}\n"
        f"Schemas: {', '.join(schemas_list) if schemas_list else 'All'}\n"
        f"Tables: {', '.join(tables_list) if tables_list else 'All'}",
        title="Backup Information"
    ))
    
    if db_ops.backup(schemas=schemas_list, tables=tables_list):
        console.print("[green]Backup completed successfully![/green]")
    else:
        console.print("[red]Backup failed![/red]")
        raise click.Abort()

@cli.command()
@click.argument('backup_file', type=click.Path(exists=True))
@click.option('--schemas', '-s', multiple=True, help='Specific schemas to restore')
@click.option('--tables', '-t', multiple=True, help='Specific tables to restore (format: schema.table)')
def restore(backup_file, schemas, tables):
    """Restore database from backup"""
    db_ops = DatabaseOperations()
    schemas_list = list(schemas) if schemas else None
    tables_list = list(tables) if tables else None
    
    console.print(Panel.fit(
        f"Starting restore operation\n"
        f"Database: {db_ops.config.DB_NAME}\n"
        f"Host: {db_ops.config.DB_HOST}\n"
        f"Backup file: {backup_file}\n"
        f"Schemas: {', '.join(schemas_list) if schemas_list else 'All'}\n"
        f"Tables: {', '.join(tables_list) if tables_list else 'All'}",
        title="Restore Information"
    ))
    
    if db_ops.restore(backup_file, schemas=schemas_list, tables=tables_list):
        console.print("[green]Restore completed successfully![/green]")
    else:
        console.print("[red]Restore failed![/red]")
        raise click.Abort()

@cli.command()
@click.option('--tables', '-t', multiple=True, help='Specific tables to export (format: schema.table)')
@click.option('--output-dir', '-o', type=click.Path(), help='Directory to save CSV files')
def export_csv(tables, output_dir):
    """Export tables to CSV files
    
    If no tables are specified, exports all tables in the database.
    """
    db_ops = DatabaseOperations()
    tables_list = list(tables) if tables else None
    
    console.print(Panel.fit(
        f"Starting CSV export operation\n"
        f"Database: {db_ops.config.DB_NAME}\n"
        f"Host: {db_ops.config.DB_HOST}\n"
        f"Tables: {', '.join(tables_list) if tables_list else 'All'}\n"
        f"Output directory: {output_dir or db_ops.config.BACKUP_DIR}",
        title="CSV Export Information"
    ))
    
    if db_ops.export_to_csv(tables_list, output_dir):
        console.print("[green]CSV export completed successfully![/green]")
    else:
        console.print("[red]CSV export failed![/red]")
        raise click.Abort()

@cli.command()
@click.option('--csv-files', '-f', multiple=True, type=click.Path(exists=True), help='Specific CSV files to import')
@click.option('--input-dir', '-i', type=click.Path(exists=True), help='Directory containing CSV files to import')
@click.option('--truncate', '-t', is_flag=True, help='Truncate tables before importing')
def import_csv(csv_files, input_dir, truncate):
    """Import data from CSV files into corresponding tables
    
    Either specify individual CSV files or a directory containing CSV files.
    If a directory is specified, all CSV files in that directory will be imported.
    """
    if not csv_files and not input_dir:
        console.print("[red]Error: Either --csv-files or --input-dir must be specified[/red]")
        raise click.Abort()
        
    db_ops = DatabaseOperations()
    files_list = list(csv_files) if csv_files else None
    
    console.print(Panel.fit(
        f"Starting CSV import operation\n"
        f"Database: {db_ops.config.DB_NAME}\n"
        f"Host: {db_ops.config.DB_HOST}\n"
        f"Files: {', '.join(files_list) if files_list else f'All files in {input_dir}'}\n"
        f"Truncate tables: {'Yes' if truncate else 'No'}",
        title="CSV Import Information"
    ))
    
    if db_ops.import_from_csv(files_list, input_dir, truncate):
        console.print("[green]CSV import completed successfully![/green]")
    else:
        console.print("[red]CSV import failed![/red]")
        raise click.Abort()

if __name__ == '__main__':
    cli() 