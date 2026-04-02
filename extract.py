"""
Extraction module for MySQL.
Extracts schema definitions, data, and stored routines from MySQL.
"""

from typing import Dict, Tuple
import pandas as pd
from database import get_mysql_connection
from config import DatabaseConfig
from utils import print_success, print_warning, print_error
import logging

logger = logging.getLogger(__name__)


class MySQLExtractor:
    """Extracts data and schema from MySQL database"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.schemas: Dict[str, str] = {}
        self.data: Dict[str, list] = {}
        self.procedures: Dict[str, str] = {}
        self.functions: Dict[str, str] = {}
        self.row_counts: Dict[str, int] = {}

    def extract_schemas(self) -> bool:
        """Extract CREATE TABLE statements from MySQL."""
        try:
            with get_mysql_connection(self.config) as conn:
                # Force tuple cursor for SHOW statements
                cursor = conn.cursor()

                # Get list of tables
                cursor.execute("SHOW TABLES")
                tables_raw = cursor.fetchall()
                
                # Handle both tuple and dict cursor safely
                if tables_raw and isinstance(tables_raw[0], dict):
                    tables = [row['Tables_in_' + self.config.database] for row in tables_raw]
                else:
                    tables = [row[0] for row in tables_raw]

                if not tables:
                    print_warning(f"No tables found in database '{self.config.database}'")
                    logger.warning(f"No tables found in database '{self.config.database}'")
                    return False

                print_success(f"Found {len(tables)} tables: {', '.join(tables)}")

                # Extract CREATE TABLE for each table
                for table_name in tables:
                    cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                    result = cursor.fetchone()
                    if result:
                        # Handle both tuple and dict result
                        create_stmt = result[1] if isinstance(result, (list, tuple)) else result.get('Create Table')
                        self.schemas[table_name] = create_stmt
                        logger.info(f"Extracted schema for table: {table_name}")

                print_success(f"Successfully extracted {len(self.schemas)} table schemas")
                return True

        except Exception as e:
            print_error(f"Failed to extract schemas: {e}")
            logger.error(f"Schema extraction error: {e}", exc_info=True)
            return False

    def extract_data(self) -> bool:
        """Extract all data from tables using native cursor (returns DictRows)."""
        try:
            with get_mysql_connection(self.config) as conn:
                cursor = conn.cursor()
                for table_name in self.schemas.keys():
                    cursor.execute(f"SELECT * FROM `{table_name}`")
                    rows = cursor.fetchall()
                    self.data[table_name] = rows
                    self.row_counts[table_name] = len(rows)
                    logger.info(f"Extracted {len(rows)} rows from table: {table_name}")

            print_success(f"Extracted data from {len(self.data)} tables "
                         f"({sum(self.row_counts.values())} total rows)")
            return True

        except Exception as e:
            print_error(f"Failed to extract data: {e}")
            logger.error(f"Data extraction error: {e}", exc_info=True)
            return False

    def extract_stored_procedures(self) -> bool:
        """Extract stored procedures."""
        try:
            with get_mysql_connection(self.config) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT ROUTINE_NAME
                    FROM INFORMATION_SCHEMA.ROUTINES
                    WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'PROCEDURE'
                """, (self.config.database,))
                
                procedures = [row['ROUTINE_NAME'] for row in cursor.fetchall()]

                for name in procedures:
                    cursor.execute(f"SHOW CREATE PROCEDURE `{name}`")
                    row = cursor.fetchone()
                    # The structure of SHOW CREATE PROCEDURE includes a 'Create Procedure' column
                    definition = row.get('Create Procedure', '')
                    if definition:
                        self.procedures[name] = definition
                        logger.info(f"Extracted procedure: {name}")

                print_success(f"Extracted {len(self.procedures)} stored procedures")
                return True

        except Exception as e:
            print_error(f"Failed to extract procedures: {e}")
            logger.error(f"Procedure extraction error: {e}", exc_info=True)
            return False

    def extract_stored_functions(self) -> bool:
        """Extract stored functions."""
        try:
            with get_mysql_connection(self.config) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT ROUTINE_NAME
                    FROM INFORMATION_SCHEMA.ROUTINES
                    WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'FUNCTION'
                """, (self.config.database,))
                
                functions = [row['ROUTINE_NAME'] for row in cursor.fetchall()]

                for name in functions:
                    cursor.execute(f"SHOW CREATE FUNCTION `{name}`")
                    row = cursor.fetchone()
                    # The structure of SHOW CREATE FUNCTION includes a 'Create Function' column
                    definition = row.get('Create Function', '')
                    if definition:
                        self.functions[name] = definition
                        logger.info(f"Extracted function: {name}")

                print_success(f"Extracted {len(self.functions)} stored functions")
                return True

        except Exception as e:
            print_error(f"Failed to extract functions: {e}")
            logger.error(f"Function extraction error: {e}", exc_info=True)
            return False

    def validate_extraction(self) -> bool:
        """Simple validation."""
        total_rows = sum(self.row_counts.values())
        print_success(f"Extraction validation passed - Total {total_rows} rows across {len(self.data)} tables")
        return True


def extract(config: DatabaseConfig) -> Tuple[bool, MySQLExtractor]:
    """Main extraction function."""
    extractor = MySQLExtractor(config)


    if not extractor.extract_schemas():
        return False, extractor

    if not extractor.extract_data():
        return False, extractor

    extractor.extract_stored_procedures()
    extractor.extract_stored_functions()

    if not extractor.validate_extraction():
        return False, extractor

    return True, extractor