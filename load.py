"""
Load module for PostgreSQL.
Creates tables, inserts data, applies constraints, and creates routines.
"""

import json
import psycopg2
import logging
from typing import Dict, List, Any, Tuple

from database import get_postgres_connection
from config import DatabaseConfig
from utils import print_success, print_warning, print_error

logger = logging.getLogger("migration")


def create_tables(
    config: DatabaseConfig,
    schemas: Dict[str, str],
    clean: bool = False
) -> bool:
    """Create tables in PostgreSQL from transformed schemas."""
    try:
        with get_postgres_connection(config) as conn:
            cursor = conn.cursor()

            if clean:
                for table in reversed(list(schemas.keys())):
                    try:
                        cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                        conn.commit()
                        logger.info(f"Dropped table: {table}")
                    except Exception as e:
                        conn.rollback()
                        logger.warning(f"Could not drop table {table}: {e}")
                print_success("Cleaned existing tables")

            created = 0
            for table_name, create_sql in schemas.items():
                try:
                    logger.debug(f"Creating table {table_name}:\n{create_sql}")
                    cursor.execute(create_sql)
                    conn.commit()
                    created += 1
                    logger.info(f"Created table: {table_name}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Failed to create table {table_name}: {e}")

        print_success(f"Created {created} tables")
        return created > 0

    except Exception as e:
        logger.error(f"Table creation error: {e}")
        return False


def insert_data(
    config: DatabaseConfig,
    data: Dict[str, List[Dict[str, Any]]]
) -> Tuple[bool, Dict[str, int]]:
    """Insert data into PostgreSQL tables. Returns (success, row_counts)."""
    inserted_counts = {}

    try:
        with get_postgres_connection(config) as conn:
            cursor = conn.cursor()

            for table_name, rows in data.items():
                if not rows:
                    logger.debug(f"No rows to insert for {table_name}")
                    inserted_counts[table_name] = 0
                    continue
                try:
                    columns = list(rows[0].keys())
                    col_str = ', '.join([f'"{c}"' for c in columns])
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders})'

                    for row in rows:
                        values = []
                        for v in row.values():
                            if isinstance(v, (dict, list)):
                                values.append(json.dumps(v))
                            else:
                                values.append(v)
                        logger.debug(f"Inserting into {table_name}: {values}")
                        cursor.execute(insert_sql, values)

                    conn.commit()
                    inserted_counts[table_name] = len(rows)
                    logger.info(f"Inserted {len(rows)} rows into {table_name}")

                except Exception as e:
                    conn.rollback()
                    inserted_counts[table_name] = 0
                    logger.error(f"Failed to insert data into {table_name}: {e}")

        print_success(f"Inserted data into {len(data)} tables")
        return True, inserted_counts

    except Exception as e:
        logger.error(f"Data insertion error: {e}")
        return False, inserted_counts


def apply_constraints(
    config: DatabaseConfig,
    constraints: List[str]
) -> bool:
    """Apply foreign key and other constraints after data load."""
    if not constraints:
        logger.debug("No constraints to apply")
        print_success("Applied all constraints")
        return True

    try:
        with get_postgres_connection(config) as conn:
            cursor = conn.cursor()
            for constraint_sql in constraints:
                try:
                    logger.debug(f"Applying constraint:\n{constraint_sql}")
                    cursor.execute(constraint_sql)
                    conn.commit()
                    logger.info(f"Applied constraint: {constraint_sql[:80]}...")
                except Exception as e:
                    conn.rollback()
                    logger.warning(f"Could not apply constraint: {e}\nSQL: {constraint_sql}")

        print_success("Applied all constraints")
        return True

    except Exception as e:
        logger.error(f"Constraint application error: {e}")
        return False


def create_routines(
    config: DatabaseConfig,
    procedures: Dict[str, str],
    functions: Dict[str, str]
) -> Tuple[bool, Dict[str, bool]]:
    """Create stored procedures and functions in PostgreSQL."""
    routine_status = {}
    all_routines = {**procedures, **functions}
    success_count = 0
    total = len(all_routines)

    try:
        with get_postgres_connection(config) as conn:
            cursor = conn.cursor()
            for routine_name, routine_sql in all_routines.items():
                try:
                    logger.debug(f"Creating routine {routine_name}:\n{routine_sql}")
                    cursor.execute(routine_sql)
                    conn.commit()
                    routine_status[routine_name] = True
                    success_count += 1
                    logger.info(f"Created routine: {routine_name}")
                except Exception as e:
                    conn.rollback()
                    routine_status[routine_name] = False
                    logger.warning(f"Error creating routine {routine_name}: {e}")

    except Exception as e:
        logger.error(f"Routine creation error: {e}")

    print_success(f"Created {success_count}/{total} routines")
    return success_count == total, routine_status
