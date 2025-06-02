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

if __name__ == '__main__':
    cli() 