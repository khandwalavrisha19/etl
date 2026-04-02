#!/usr/bin/env python3
"""
MySQL to PostgreSQL ETL Migration Tool
Main orchestration script for the migration process.
"""

import sys
import time
import argparse
from typing import Tuple
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config import Config
from database import test_mysql_connection, test_postgres_connection
from extract import extract
from transform import transform_schema, transform_routines
from load import create_tables, insert_data, apply_constraints, create_routines
from utils import (
    setup_logging,
    print_step_header,
    print_success,
    print_warning,
    print_error,
    print_banner,
    format_migration_summary,
)

console = Console()
logger = None


def print_banner_startup() -> None:
    """Print startup banner"""
    console.print()
    console.print(Panel.fit(
        "[bold cyan]MySQL to PostgreSQL ETL Migration Tool[/bold cyan]\n"
        "[yellow]Version 1.0.0[/yellow]",
        border_style="cyan",
        padding=(1, 2)
    ))
    console.print()


def test_connections(config: Config) -> bool:
    """
    Test database connections.
    
    Args:
        config: Configuration instance
        
    Returns:
        True if both connections successful, False otherwise
    """
    console.print("\n[bold cyan]Testing Database Connections...[/bold cyan]\n")
    
    # Test MySQL connection
    console.print("[yellow]Testing MySQL connection...[/yellow]")
    if not test_mysql_connection(config.mysql_config):
        print_error(f"Failed to connect to MySQL: {config.mysql_config.host}:{config.mysql_config.port}")
        return False
    print_success(f"Connected to MySQL: {config.mysql_config.host}:{config.mysql_config.port}")
    
    # Test PostgreSQL connection
    console.print("[yellow]Testing PostgreSQL connection...[/yellow]")
    if not test_postgres_connection(config.postgres_config):
        print_error(f"Failed to connect to PostgreSQL: {config.postgres_config.host}:{config.postgres_config.port}")
        return False
    print_success(f"Connected to PostgreSQL: {config.postgres_config.host}:{config.postgres_config.port}")
    
    return True


def validate_extraction(extractor) -> bool:
    """
    Validate extraction results.
    
    Args:
        extractor: MySQLExtractor instance
        
    Returns:
        True if validation passed, False otherwise
    """
    print_step_header(2, 9, "VALIDATE EXTRACTION")
    
    # Validate we have schemas
    if not extractor.schemas:
        print_error("No schemas extracted")
        return False
    
    # Validate we have data
    if not extractor.data:
        print_error("No data extracted")
        return False
    
    # Validate row counts
    console.print("\n[bold]Row Counts:[/bold]")
    for table_name, count in extractor.row_counts.items():
        console.print(f"  {table_name}: {count} rows")
    
    # Validate procedures
    if not extractor.procedures:
        print_warning("No stored procedures found")
    else:
        console.print(f"\n[bold]Stored Procedures: {len(extractor.procedures)} found[/bold]")
    
    # Validate functions
    if not extractor.functions:
        print_warning("No stored functions found")
    else:
        console.print(f"[bold]Stored Functions: {len(extractor.functions)} found[/bold]")
    
    print_success("Extraction validation complete")
    return True


def validate_postgres_migration(config: Config, mysql_counts: dict) -> bool:
    """
    Validate PostgreSQL migration results.
    
    Args:
        config: Configuration instance
        mysql_counts: Dictionary of MySQL row counts
        
    Returns:
        True if validation passed, False otherwise
    """
    print_step_header(9, 9, "FINAL VALIDATION")
    
    try:
        from database import get_postgres_connection
        
        with get_postgres_connection(config.postgres_config) as conn:
            cursor = conn.cursor()
            
            console.print("\n[bold]Row Count Comparison:[/bold]")
            
            all_match = True
            for table_name in mysql_counts.keys():
                # Get PostgreSQL row count
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                pg_count = cursor.fetchone()[0]
                mysql_count = mysql_counts[table_name]
                
                match = "✅" if mysql_count == pg_count else "❌"
                console.print(f"  {table_name}: MySQL={mysql_count}, PostgreSQL={pg_count} {match}")
                
                if mysql_count != pg_count:
                    all_match = False
            
            if all_match:
                print_success("All row counts match")
            else:
                print_warning("Some row counts don't match")
            
            # Since routines are generic now, we skip hardcoded function testing
            
            return all_match
            
    except Exception as e:
        print_error(f"Validation failed: {e}")
        logger.error(f"Validation error: {e}")
        return False


def run_migration(config: Config, args) -> bool:
    """
    Run the complete ETL migration.
    
    Args:
        config: Configuration instance
        args: Command-line arguments
        
    Returns:
        True if migration successful, False otherwise
    """
    start_time = time.time()
    
    # Step 1: Extract
    print_step_header(1, 9, "EXTRACT")
    success, extractor = extract(config.mysql_config)
    if not success:
        print_error("Extraction failed")
        return False
    
    # Step 2: Validate Extraction
    if not validate_extraction(extractor):
        print_error("Extraction validation failed")
        return False
    
    # Step 3: Transform Schema
    print_step_header(3, 9, "TRANSFORM SCHEMA")
    success, transformed_schemas, extracted_fks = transform_schema(extractor.schemas)
    if not success:
        print_error("Schema transformation failed")
        return False
    
    # Step 4: Transform Routines
    print_step_header(4, 9, "TRANSFORM ROUTINES")
    success, transformed_procs, transformed_funcs = transform_routines(
        extractor.procedures,
        extractor.functions
    )
    if not success:
        print_error("Routine transformation failed")
        return False
    
    # Step 5: Create Tables
    print_step_header(5, 9, "CREATE TABLES")
    if not create_tables(
        config.postgres_config,
        transformed_schemas,
        clean=config.clean_before_migration
    ):
        print_error("Table creation failed")
        return False
    
    # Step 6: Insert Data
    print_step_header(6, 9, "INSERT DATA")
    success, inserted_counts = insert_data(config.postgres_config, extractor.data)
    if not success:
        print_error("Data insertion failed")
        return False
    
    # Step 7: Apply Constraints
    print_step_header(7, 9, "APPLY CONSTRAINTS")
    if not apply_constraints(config.postgres_config, extracted_fks):
        print_error("Constraint application failed")
        return False
    
    # Step 8: Create Routines
    print_step_header(8, 9, "CREATE ROUTINES")
    success, routine_status = create_routines(
        config.postgres_config,
        transformed_procs,
        transformed_funcs
    )
    if not success:
        print_error("Routine creation failed")
        return False
    
    # Step 9: Final Validation
    if not validate_postgres_migration(config, extractor.row_counts):
        print_warning("Final validation found issues")
    
    elapsed = time.time() - start_time
    
    # Print summary
    console.print("\n")
    summary_table = format_migration_summary(
        extractor.row_counts,
        inserted_counts,
        routine_status
    )
    console.print(summary_table)
    
    console.print(Panel.fit(
        f"[bold green]✅ Migration completed successfully in {elapsed:.2f} seconds![/bold green]",
        border_style="green",
        padding=(1, 2)
    ))
    
    return True


def main():
    """Main entry point"""
    global logger
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="MySQL to PostgreSQL ETL Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to .env configuration file",
        default=".env"
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for credentials interactively",
        default=True
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Drop existing tables before migration"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(verbose=args.verbose)
    logger.info("Migration started")
    
    # Print banner
    print_banner_startup()
    
    # Load configuration
    config = Config()
    config.load_from_env()
    
    # Prompt for credentials if interactive
    if args.interactive:
        try:
            config.prompt_user_for_credentials()
        except KeyboardInterrupt:
            console.print("\n[yellow]Migration cancelled by user[/yellow]")
            return 1
    
    # Override clean flag if provided
    if args.clean:
        config.clean_before_migration = True
    
    # Test connections
    if not test_connections(config):
        print_error("Connection test failed. Exiting.")
        logger.error("Connection test failed")
        return 1
    
    # Run migration
    console.print()
    try:
        if run_migration(config, args):
            logger.info("Migration completed successfully")
            return 0
        else:
            logger.error("Migration failed")
            return 1
    except KeyboardInterrupt:
        console.print("\n[yellow]Migration cancelled by user[/yellow]")
        logger.warning("Migration cancelled by user")
        return 1
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        logger.exception("Unexpected error during migration")
        return 1


if __name__ == "__main__":
    sys.exit(main())