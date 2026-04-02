"""
Load module for PostgreSQL.
Creates tables, inserts data, applies constraints, and creates routines.
"""

from typing import Dict, List, Any, Tuple
from database import get_postgres_connection
from config import DatabaseConfig
from utils import setup_logging, print_success, print_warning, print_error
import psycopg2

logger = setup_logging()


class PostgreSQLLoader:
    """Loads data and creates objects in PostgreSQL database"""

    def __init__(self, config: DatabaseConfig):
        """
        Initialize PostgreSQL loader.
        
        Args:
            config: DatabaseConfig instance for PostgreSQL
        """
        self.config = config
        self.created_tables = []
        self.inserted_rows = {}

    def clean_database(self, table_names: List[str]) -> bool:
        """
        Drop dynamically specified existing tables and objects.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_postgres_connection(self.config) as conn:
                cursor = conn.cursor()
                
                # Drop tables cascading safely to ignore order
                for table in table_names:
                    try:
                        cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                        logger.info(f"Dropped table: {table}")
                    except Exception as e:
                        logger.warning(f"Could not drop table {table}: {e}")
                
                conn.commit()
                print_success("Cleaned existing tables")
                return True
                
        except Exception as e:
            print_error(f"Failed to clean database: {e}")
            logger.error(f"Database cleanup error: {e}")
            return False

    def create_tables(self, schemas: Dict[str, str]) -> bool:
        """
        Create tables in PostgreSQL.
        
        Args:
            schemas: Dictionary of transformed PostgreSQL CREATE TABLE statements
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_postgres_connection(self.config) as conn:
                cursor = conn.cursor()
                
                # Create tables dynamically based on schemas keys
                for table_name in schemas.keys():
                    if table_name not in schemas:
                        continue
                    
                    try:
                        cursor.execute(schemas[table_name])
                        conn.commit()
                        self.created_tables.append(table_name)
                        logger.info(f"Created table: {table_name}")
                    except psycopg2.Error as e:
                        conn.rollback()
                        print_warning(f"Error creating table {table_name}: {e}")
                        logger.warning(f"Table creation error for {table_name}: {e}")
                
                conn.commit()
                print_success(f"Created {len(self.created_tables)} tables")
                return True
                
        except Exception as e:
            print_error(f"Failed to create tables: {e}")
            logger.error(f"Table creation error: {e}")
            return False

    def insert_data(self, data: Dict[str, List[Dict]]) -> bool:
        """
        Insert data into PostgreSQL tables.
        
        Args:
            data: Dictionary of table names and row data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_postgres_connection(self.config) as conn:
                cursor = conn.cursor()
                
                # Insert data in any order because Foreign Keys are delayed
                for table_name in data.keys():
                    
                    rows = data[table_name]
                    if not rows:
                        continue
                    
                    
                    # Fetch column types from PostgreSQL to handle boolean conversion
                    cursor.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                    """, (table_name,))
                    col_types = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # Get column names from first row
                    columns = list(rows[0].keys())
                    col_names = ', '.join([f'"{col}"' for col in columns])
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
                    
                    try:
                        for row in rows:
                            values = []
                            for col in columns:
                                val = row.get(col)
                                # auto-cast integers to booleans if the target pg column is boolean
                                if col_types.get(col) == 'boolean' and val is not None:
                                    val = bool(val)
                                values.append(val)
                            cursor.execute(insert_sql, values)
                        
                        conn.commit()
                        self.inserted_rows[table_name] = len(rows)
                        logger.info(f"Inserted {len(rows)} rows into {table_name}")
                        
                    except psycopg2.Error as e:
                        conn.rollback()
                        print_warning(f"Error inserting into {table_name}: {e}")
                        logger.warning(f"Insert error for {table_name}: {e}")
                
                conn.commit()
                print_success(f"Inserted data into {len(self.inserted_rows)} tables")
                return True
                
        except Exception as e:
            print_error(f"Failed to insert data: {e}")
            logger.error(f"Data insertion error: {e}")
            return False

    def apply_constraints(self, extracted_fks: Dict[str, list]) -> bool:
        """
        Apply dynamically captured FOREIGN KEY and constraint rules.
        
        Args:
            extracted_fks: Dictionary of table names to lists of FOREIGN KEY constraints
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_postgres_connection(self.config) as conn:
                cursor = conn.cursor()
                
                constraints = []
                # Reconstruct FOREIGN KEYS dynamically from the parsed MySQL schemas
                for table_name, fks in extracted_fks.items():
                    for fk in fks:
                        constraints.append(f'ALTER TABLE "{table_name}" ADD {fk}')
                    
                for constraint_sql in constraints:
                    try:
                        cursor.execute(constraint_sql)
                        conn.commit()
                        logger.info(f"Applied constraint: {constraint_sql[:60]}...")
                    except psycopg2.Error as e:
                        conn.rollback()
                        if 'already exists' in str(e) or 'duplicate' in str(e).lower():
                            logger.debug(f"Constraint already exists: {constraint_sql[:60]}...")
                        else:
                            logger.warning(f"Constraint error: {e}")
                
                conn.commit()
                print_success("Applied all constraints")
                return True
                
        except Exception as e:
            print_error(f"Failed to apply constraints: {e}")
            logger.error(f"Constraint application error: {e}")
            return False

    def create_routines(
        self,
        procedures: Dict[str, str],
        functions: Dict[str, str]
    ) -> Tuple[bool, Dict[str, bool]]:
        """
        Create stored functions and procedures in PostgreSQL.
        
        Args:
            procedures: Dictionary of procedure names and definitions
            functions: Dictionary of function names and definitions
            
        Returns:
            Tuple of (success: bool, routine_status: dict)
        """
        routine_status = {}
        
        try:
            with get_postgres_connection(self.config) as conn:
                cursor = conn.cursor()
                
                # Create parsed and transformed routines from MySQL dynamically
                for routine_name, routine_def in {**procedures, **functions}.items():
                

                    try:
                        cursor.execute(routine_def)
                        conn.commit()
                        routine_status[routine_name] = True
                        logger.info(f"Created routine: {routine_name}")
                    except psycopg2.Error as e:
                        conn.rollback()
                        routine_status[routine_name] = False
                        logger.warning(f"Error creating routine {routine_name}: {e}")
                
                created_count = sum(1 for v in routine_status.values() if v)
                print_success(f"Created {created_count}/{len(routine_status)} routines")
                return True, routine_status
                
        except Exception as e:
            print_error(f"Failed to create routines: {e}")
            logger.error(f"Routine creation error: {e}")
            return False, routine_status




def create_tables(
    config: DatabaseConfig,
    schemas: Dict[str, str],
    clean: bool = False
) -> bool:
    """
    Create tables in PostgreSQL.
    
    Args:
        config: DatabaseConfig instance for PostgreSQL
        schemas: Dictionary of transformed PostgreSQL CREATE TABLE statements
        clean: Whether to drop existing tables first
        
    Returns:
        True if successful, False otherwise
    """
    loader = PostgreSQLLoader(config)
    
    if clean:
        if not loader.clean_database(list(schemas.keys())):
            return False
    
    return loader.create_tables(schemas)


def insert_data(
    config: DatabaseConfig,
    data: Dict[str, List[Dict]]
) -> Tuple[bool, Dict[str, int]]:
    """
    Insert data into PostgreSQL tables.
    
    Args:
        config: DatabaseConfig instance for PostgreSQL
        data: Dictionary of table names and row data
        
    Returns:
        Tuple of (success: bool, inserted_rows: dict)
    """
    loader = PostgreSQLLoader(config)
    success = loader.insert_data(data)
    return success, loader.inserted_rows


def apply_constraints(config: DatabaseConfig, extracted_fks: Dict[str, list]) -> bool:
    """
    Apply constraints to PostgreSQL tables.
    
    Args:
        config: DatabaseConfig instance for PostgreSQL
        extracted_fks: Dictionary of table names to lists of FOREIGN KEY constraints
        
    Returns:
        True if successful, False otherwise
    """
    loader = PostgreSQLLoader(config)
    return loader.apply_constraints(extracted_fks)


def create_routines(
    config: DatabaseConfig,
    procedures: Dict[str, str],
    functions: Dict[str, str]
) -> Tuple[bool, Dict[str, bool]]:
    """
    Create routines in PostgreSQL.
    
    Args:
        config: DatabaseConfig instance for PostgreSQL
        procedures: Dictionary of procedure names and definitions
        functions: Dictionary of function names and definitions
        
    Returns:
        Tuple of (success: bool, routine_status: dict)
    """
    loader = PostgreSQLLoader(config)
    return loader.create_routines(procedures, functions)