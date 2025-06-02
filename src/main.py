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
def backup(schemas):
    """Create a database backup"""
    db_ops = DatabaseOperations()
    schemas_list = list(schemas) if schemas else None
    
    console.print(Panel.fit(
        f"Starting backup operation\n"
        f"Database: {db_ops.config.DB_NAME}\n"
        f"Host: {db_ops.config.DB_HOST}\n"
        f"Schemas: {', '.join(schemas_list) if schemas_list else 'All'}",
        title="Backup Information"
    ))
    
    if db_ops.backup(schemas=schemas_list):
        console.print("[green]Backup completed successfully![/green]")
    else:
        console.print("[red]Backup failed![/red]")
        raise click.Abort()

@cli.command()
@click.argument('backup_file', type=click.Path(exists=True))
@click.option('--schemas', '-s', multiple=True, help='Specific schemas to restore')
def restore(backup_file, schemas):
    """Restore database from backup"""
    db_ops = DatabaseOperations()
    schemas_list = list(schemas) if schemas else None
    
    console.print(Panel.fit(
        f"Starting restore operation\n"
        f"Database: {db_ops.config.DB_NAME}\n"
        f"Host: {db_ops.config.DB_HOST}\n"
        f"Backup file: {backup_file}\n"
        f"Schemas: {', '.join(schemas_list) if schemas_list else 'All'}",
        title="Restore Information"
    ))
    
    if db_ops.restore(backup_file, schemas=schemas_list):
        console.print("[green]Restore completed successfully![/green]")
    else:
        console.print("[red]Restore failed![/red]")
        raise click.Abort()

if __name__ == '__main__':
    cli() 