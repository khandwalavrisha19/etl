"""
Configuration module for MySQL to PostgreSQL migration.
Handles environment variables and user input for database connections.
"""

import os
import getpass
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from rich.console import Console

console = Console()

# Load environment variables from .env file
load_dotenv()


@dataclass
class DatabaseConfig:
    """Configuration for database connections"""
    host: str
    port: int
    user: str
    password: str
    database: str

    def __repr__(self) -> str:
        """Safe representation without exposing password"""
        return (
            f"DatabaseConfig(host={self.host}, port={self.port}, "
            f"user={self.user}, database={self.database})"
        )


class Config:
    """
    Configuration management for the migration tool.
    Handles loading from environment variables or user input.
    """

    def __init__(self):
        """Initialize configuration from environment variables"""
        self.mysql_config = None
        self.postgres_config = None
        self.clean_before_migration = False
        self.verbose_logging = False

    def load_from_env(self) -> None:
        """Load configuration from .env file or environment variables"""
        # MySQL Configuration
        self.mysql_config = DatabaseConfig(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=os.getenv("MYSQL_DATABASE", "etl_migration_db"),
        )

        # PostgreSQL Configuration
        self.postgres_config = DatabaseConfig(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", 5432)),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", ""),
            database=os.getenv("PG_DATABASE", "etl_migration_db"),
        )

        # Migration Options
        self.clean_before_migration = os.getenv("CLEAN_BEFORE_MIGRATION", "false").lower() == "true"
        self.verbose_logging = os.getenv("VERBOSE_LOGGING", "false").lower() == "true"

    def prompt_user_for_credentials(self) -> None:
        """
        Prompt user for database credentials interactively.
        Allows overriding environment variables.
        """
        console.print("\n[bold cyan]MySQL Database Configuration[/bold cyan]")
        console.print("(Press Enter to use default values)\n")

        mysql_db = console.input(
            f"[yellow]Database name[/yellow] [dim](default: {self.mysql_config.database})[/dim]: "
        ).strip() or self.mysql_config.database

        mysql_user = console.input(
            f"[yellow]Username[/yellow] [dim](default: {self.mysql_config.user})[/dim]: "
        ).strip() or self.mysql_config.user

        mysql_pass = getpass.getpass(
            "Password(hidden): "
        )

        mysql_host = console.input(
            f"[yellow]Host[/yellow] [dim](default: {self.mysql_config.host})[/dim]: "
        ).strip() or self.mysql_config.host

        mysql_port = console.input(
            f"[yellow]Port[/yellow] [dim](default: {self.mysql_config.port})[/dim]: "
        ).strip()
        mysql_port = int(mysql_port) if mysql_port else self.mysql_config.port

        self.mysql_config = DatabaseConfig(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_pass,
            database=mysql_db,
        )

        console.print("\n[bold cyan]PostgreSQL Database Configuration[/bold cyan]")
        console.print("(Press Enter to use default values)\n")

        pg_user = console.input(
            f"[yellow]Username[/yellow] [dim](default: {self.postgres_config.user})[/dim]: "
        ).strip() or self.postgres_config.user

        pg_pass = getpass.getpass(
            "Password(hidden): "
        )

        pg_host = console.input(
            f"[yellow]Host[/yellow] [dim](default: {self.postgres_config.host})[/dim]: "
        ).strip() or self.postgres_config.host

        pg_port = console.input(
            f"[yellow]Port[/yellow] [dim](default: {self.postgres_config.port})[/dim]: "
        ).strip()
        pg_port = int(pg_port) if pg_port else self.postgres_config.port

        self.postgres_config = DatabaseConfig(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_pass,
            database=mysql_db,  # Use same database name as MySQL
        )

        console.print("\n[bold cyan]Migration Options[/bold cyan]\n")
        clean_input = console.input(
            "[yellow]Clean existing tables before migration?[/yellow] (y/n, default: n): "
        ).strip().lower()
        self.clean_before_migration = clean_input == 'y'

        verbose_input = 'y'
        self.verbose_logging = verbose_input == 'y'