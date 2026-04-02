"""
Database connection utilities.
Provides context managers for MySQL and PostgreSQL connections.
"""

import pymysql
import psycopg2
from contextlib import contextmanager
from config import DatabaseConfig
from utils import setup_logging

logger = setup_logging()


@contextmanager
def get_mysql_connection(config: DatabaseConfig):
    """
    Context manager for MySQL database connections.
    
    Args:
        config: DatabaseConfig instance for MySQL
        
    Yields:
        MySQL database connection
    """
    conn = None
    try:
        conn = pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        logger.debug(f"Connected to MySQL: {config.host}:{config.port}/{config.database}")
        yield conn
    except pymysql.Error as e:
        logger.error(f"MySQL connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("MySQL connection closed")


@contextmanager
def get_postgres_connection(config: DatabaseConfig):
    """
    Context manager for PostgreSQL database connections.
    
    Args:
        config: DatabaseConfig instance for PostgreSQL
        
    Yields:
        PostgreSQL database connection
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
        logger.debug(f"Connected to PostgreSQL: {config.host}:{config.port}/{config.database}")
        yield conn
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("PostgreSQL connection closed")


def test_mysql_connection(config: DatabaseConfig) -> bool:
    """
    Test MySQL connection.
    
    Args:
        config: DatabaseConfig instance for MySQL
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_mysql_connection(config) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return True
    except Exception as e:
        logger.error(f"MySQL connection test failed: {e}")
        return False


def test_postgres_connection(config: DatabaseConfig) -> bool:
    """
    Test PostgreSQL connection.
    
    Args:
        config: DatabaseConfig instance for PostgreSQL
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_postgres_connection(config) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return True
    except psycopg2.OperationalError as e:
        if "does not exist" in str(e):
            logger.info(f"Database {config.database} not found, attempting to create it...")
            try:
                # Temporarily change database to postgres default to execute the CREATE command
                conn = psycopg2.connect(
                    host=config.host,
                    port=config.port,
                    user=config.user,
                    password=config.password,
                    database='postgres'
                )
                conn.autocommit = True
                cursor = conn.cursor()
                safe_db_name = config.database.replace('"', '').replace("'", "")
                cursor.execute(f'CREATE DATABASE "{safe_db_name}"')
                conn.close()
                logger.info(f"Successfully created database: {config.database}")
                return True
            except Exception as create_err:
                logger.error(f"Failed to create PostgreSQL database dynamically: {create_err}")
                return False
        
        logger.error(f"PostgreSQL connection test failed: {e}")
        return False
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {e}")
        return False