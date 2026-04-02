"""
Utility module for logging and helper functions.
Provides logging setup and common helper functions.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Setup logging configuration with both file and console output.
    
    Args:
        verbose: Whether to enable verbose (DEBUG) logging
        
    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("migration")
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers = []
    
    # File handler
    log_file = log_dir / f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def print_step_header(step_num: int, total_steps: int, title: str) -> None:
    """
    Print a formatted step header.
    
    Args:
        step_num: Current step number
        total_steps: Total number of steps
        title: Step title
    """
    header = f"[{step_num}/{total_steps}] {title}"
    console.print(Panel.fit(header, border_style="cyan", padding=(0, 1)))


def print_success(message: str) -> None:
    """Print a success message in green"""
    console.print(f"[green]✅ {message}[/green]")


def print_warning(message: str) -> None:
    """Print a warning message in yellow"""
    console.print(f"[yellow]⚠️  {message}[/yellow]")


def print_error(message: str) -> None:
    """Print an error message in red"""
    console.print(f"[red]❌ {message}[/red]")


def print_info(message: str) -> None:
    """Print an info message in blue"""
    console.print(f"[blue]ℹ️  {message}[/blue]")


def print_summary_table(
    title: str,
    data: dict,
    columns: list = None
) -> None:
    """
    Print a formatted summary table.
    
    Args:
        title: Table title
        data: Dictionary of data to display
        columns: List of column names (auto-detect if None)
    """
    table = Table(title=title, show_header=True, header_style="bold cyan")
    
    if columns:
        for col in columns:
            table.add_column(col, style="magenta")
    else:
        for key in data.keys():
            table.add_column(key, style="magenta")
    
    # Add rows
    if isinstance(data, dict) and all(isinstance(v, (list, tuple)) for v in data.values()):
        # Data is in format {col1: [values], col2: [values]}
        num_rows = len(next(iter(data.values())))
        for i in range(num_rows):
            row = [str(data[col][i]) for col in data.keys()]
            table.add_row(*row)
    else:
        # Data is a list of dicts or tuples
        for item in data if isinstance(data, list) else [data]:
            if isinstance(item, dict):
                table.add_row(*[str(v) for v in item.values()])
            else:
                table.add_row(*[str(v) for v in item])
    
    console.print(table)


def format_migration_summary(
    mysql_counts: dict,
    postgres_counts: dict,
    routines_status: dict
) -> Table:
    """
    Create a formatted migration summary table.
    
    Args:
        mysql_counts: Dictionary of table names and row counts in MySQL
        postgres_counts: Dictionary of table names and row counts in PostgreSQL
        routines_status: Dictionary of routine names and their creation status
        
    Returns:
        Formatted Rich Table
    """
    table = Table(
        title="Migration Summary",
        show_header=True,
        header_style="bold cyan",
        border_style="green"
    )
    
    table.add_column("Table/Routine", style="cyan")
    table.add_column("MySQL", style="yellow")
    table.add_column("PostgreSQL", style="green")
    table.add_column("Status", style="magenta")
    
    # Add table rows
    for table_name in mysql_counts.keys():
        mysql_count = mysql_counts.get(table_name, 0)
        postgres_count = postgres_counts.get(table_name, 0)
        status = "✅" if mysql_count == postgres_count else "❌"
        table.add_row(table_name, str(mysql_count), str(postgres_count), status)
    
    # Add routine rows
    table.add_row("[bold]ROUTINES[/bold]", "", "", "")
    for routine_name, status in routines_status.items():
        status_str = "✅" if status else "❌"
        table.add_row(f"  {routine_name}", "1", "1", status_str)
    
    return table


def print_banner(title: str) -> None:
    """Print a banner with title"""
    console.print(Panel.fit(
        f"[bold cyan]{title}[/bold cyan]",
        border_style="cyan",
        padding=(1, 2)
    ))


def truncate_string(value: str, max_length: int = 100) -> str:
    """
    Truncate a string to a maximum length.
    
    Args:
        value: String to truncate
        max_length: Maximum length
        
    Returns:
        Truncated string
    """
    if len(value) > max_length:
        return value[:max_length - 3] + "..."
    return value


def safe_dict_get(d: dict, key: str, default=None):
    """
    Safely get a value from a dictionary.
    
    Args:
        d: Dictionary to search
        key: Key to look for
        default: Default value if key not found
        
    Returns:
        Value if found, default otherwise
    """
    return d.get(key, default)