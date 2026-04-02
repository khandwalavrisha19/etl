"""
Transformation module for MySQL to PostgreSQL conversion.
Improved version with aggressive cleaning.
"""

import re
from typing import Dict, Tuple
from utils import print_success, print_warning, print_error
import logging

logger = logging.getLogger(__name__)


class SchemaTransformer:
    """Transforms MySQL schemas to PostgreSQL syntax"""

    def __init__(self):
        self.transformed_schemas: Dict[str, str] = {}
        self.extracted_fks: Dict[str, list] = {}

    def clean_create_table(self, create_stmt: str, table_name: str) -> str:
        """Aggressively clean MySQL CREATE TABLE for PostgreSQL."""
        
        # Start with the raw statement
        sql = create_stmt.strip()
        
        # 1. Replace backticks with double quotes
        sql = sql.replace('`', '"')
        
        # Extracted FK list for this table
        if table_name not in self.extracted_fks:
            self.extracted_fks[table_name] = []
            
        def fk_replacer(match):
            fk_constraint = match.group(1).strip()
            self.extracted_fks[table_name].append(fk_constraint)
            return ""

        # Extract FOREIGN KEY constraints and remove them from CREATE TABLE
        sql = re.sub(
            r',\s*((?:CONSTRAINT\s+(?:"[^"]*"|\w+)\s+)?FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+(?:"[^"]*"|\w+)\s*\([^)]+\)(?:\s+ON\s+(?:DELETE|UPDATE)\s+(?:CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION))*)',
            fk_replacer,
            sql,
            flags=re.IGNORECASE
        )
        
        # 2. Remove MySQL-specific table options at the very end
        # We can just drop everything after the final closing parenthesis of the CREATE TABLE statement
        last_paren_idx = sql.rfind(')')
        if last_paren_idx != -1:
            sql = sql[:last_paren_idx + 1]
        
        # 3. Convert AUTO_INCREMENT to SERIAL
        # Match pattern: "col_name" INT NOT NULL AUTO_INCREMENT
        sql = re.sub(
            r'("?\w+"?)\s+(?:INT|BIGINT|SMALLINT|TINYINT)(?:\(\d+\))?(?:\s+NOT\s+NULL)?\s+AUTO_INCREMENT', 
            lambda m: f'{m.group(1)} BIGSERIAL' if 'BIGINT' in m.group(0).upper() else f'{m.group(1)} SERIAL', 
            sql, flags=re.IGNORECASE
        )
        
        # Fallback: remove any remaining AUTO_INCREMENT keywords
        sql = re.sub(r'\bAUTO_INCREMENT\b', '', sql, flags=re.IGNORECASE)
        
        # 4. Convert data types
        sql = re.sub(r'\bDATETIME\b', 'TIMESTAMP', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bCURDATE\(\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bON\s+UPDATE\s+CURRENT_TIMESTAMP(?:\(\))?\b', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'TINYINT\(1\)', 'BOOLEAN', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bDECIMAL\b', 'NUMERIC', sql, flags=re.IGNORECASE)
        sql = re.sub(r"ENUM\s*\([^)]+\)", "TEXT", sql, flags=re.IGNORECASE)
        
        # 5. Fix UNIQUE KEY syntax
        sql = re.sub(r'UNIQUE\s+KEY\s+"?(\w+)"?\s*\(([^)]+)\)', r'UNIQUE (\2)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'UNIQUE KEY', 'UNIQUE', sql, flags=re.IGNORECASE)
        
        # 6. Remove standalone KEY (indexes) - they cause issues in CREATE TABLE
        sql = re.sub(r',\s*KEY\s+"?\w+"?\s*\([^)]+\)', '', sql, flags=re.IGNORECASE)
        
        # 7. Fix CHECK constraints (remove extra parentheses and boolean checks)
        sql = re.sub(r',\s*CONSTRAINT\s+"?[^"]+"?\s+CHECK\s*\(\s*\(*\s*"?[^"()]+"?\s+in\s*\(\s*0\s*,\s*1\s*\)\s*\)*\)', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'CONSTRAINT\s+"?[^"]+"?\s+CHECK\s*\(\s*\(*\s*"?[^"()]+"?\s+in\s*\(\s*0\s*,\s*1\s*\)\s*\)*\)', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r',\s*CHECK\s*\(\s*\(*\s*"?[^"()]+"?\s+in\s*\(\s*0\s*,\s*1\s*\)\s*\)*\)', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'CHECK\s*\(\s*\(\s*(.+?)\s*\)\s*\)', r'CHECK (\1)', sql, flags=re.IGNORECASE)
        
        # 8. Clean up extra commas before closing parenthesis
        sql = re.sub(r',\s*\)', ')', sql)
        
        # 9. Final cleanup
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        # Make sure it ends properly
        if not sql.endswith(')'):
            sql += ')'
        
        logger.info(f"Cleaned schema for table: {table_name}")
        return sql

    def transform_table_schema(self, table_name: str, mysql_create: str) -> str:
        """Transform one table."""
        try:
            pg_sql = self.clean_create_table(mysql_create, table_name)
            self.transformed_schemas[table_name] = pg_sql
            return pg_sql
        except Exception as e:
            logger.error(f"Failed transforming {table_name}: {e}")
            return mysql_create

    def transform_all_schemas(self, schemas: Dict[str, str]) -> Dict[str, str]:
        for table_name, create_stmt in schemas.items():
            self.transform_table_schema(table_name, create_stmt)
        
        print_success(f"Transformed {len(self.transformed_schemas)} table schemas")
        return self.transformed_schemas


class RoutineTransformer:
    """Simple routine transformer"""
    
    def __init__(self):
        self.transformed_procedures = {}
        self.transformed_functions = {}

    def transform_routine(self, pg_def: str, is_function: bool = False) -> str:
        if not pg_def:
            return ""
            
        # Strip DEFINER
        pg_def = re.sub(r"CREATE\s+DEFINER=`[^`]+`@`[^`]+`\s+", "CREATE OR REPLACE ", pg_def, flags=re.IGNORECASE)
        
        # Replace all remaining backticks with double quotes (Postgres standard)
        pg_def = pg_def.replace('`', '"')
        
        # Now remove double quotes from the procedure/function declaration itself if needed
        pg_def = re.sub(r"CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+\"([^\"]+)\"", r"CREATE OR REPLACE PROCEDURE \1", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"CREATE\s+OR\s+REPLACE\s+FUNCTION\s+\"([^\"]+)\"", r"CREATE OR REPLACE FUNCTION \1", pg_def, flags=re.IGNORECASE)
        
        # Type cleanup
        pg_def = re.sub(r"INT\(\d+\)", "INTEGER", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"INT\b", "INTEGER", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"VARCHAR\(\d+\)", "VARCHAR", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"DECIMAL\(\d+,\d+\)", "NUMERIC", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"ENUM\([^)]+\)", "VARCHAR", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"\bDATETIME\b", "TIMESTAMP", pg_def, flags=re.IGNORECASE)
        
        # Strip MySQL-specific function properties
        pg_def = re.sub(r"\bDETERMINISTIC\b", "", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"\bREADS\s+SQL\s+DATA\b", "", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"\bCONTAINS\s+SQL\b", "", pg_def, flags=re.IGNORECASE)
        
        # Function mappings
        pg_def = re.sub(r"\bCURDATE\(\)", "CURRENT_DATE", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"DATE_SUB\(([^,]+),\s*INTERVAL\s+([a-zA-Z0-9_]+)\s+DAY\)", r"(\1 - (\2 * INTERVAL '1 day'))", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"DATE_ADD\(([^,]+),\s*INTERVAL\s+([a-zA-Z0-9_]+)\s+DAY\)", r"(\1 + (\2 * INTERVAL '1 day'))", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"\bLAST_INSERT_ID\(\)", "lastval()", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r"\bSET\s+([a-zA-Z0-9_]+)\s*=\s*lastval\(\)\s*;", r"\1 := lastval();", pg_def, flags=re.IGNORECASE)
        
        # MySQL UPDATE ... JOIN -> PostgreSQL UPDATE ... FROM
        pg_def = re.sub(
            r"UPDATE\s+([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)\s+JOIN\s+([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)\s+ON\s+(.+?)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?(;)",
            r"UPDATE \1 \2 SET \6 FROM \3 \4 WHERE \5 AND (\7)\8",
            pg_def,
            flags=re.IGNORECASE | re.DOTALL
        )
        pg_def = pg_def.replace(" AND ()", "")
        
        # Remove IN/OUT markers from parameters
        pg_def = re.sub(r"\(\s*IN\s+", "(", pg_def, flags=re.IGNORECASE)
        pg_def = re.sub(r",\s*IN\s+", ", ", pg_def, flags=re.IGNORECASE)
        
        # Hoist DECLARE variables out of BEGIN block FIRST
        declares = re.findall(r"^\s*DECLARE\s+([^;]+);", pg_def, flags=re.IGNORECASE | re.MULTILINE)
        pg_def = re.sub(r"^\s*DECLARE\s+[^;]+;\n?", "", pg_def, flags=re.IGNORECASE | re.MULTILINE)
        
        formatted_decls = []
        for decl in declares:
            decl = re.sub(r"\bDEFAULT\b", ":=", decl, flags=re.IGNORECASE)
            formatted_decls.append(f"    {decl.strip()};")

        # Wrapper injection
        # For functions with RETURNS block
        if is_function:
            pg_def = re.sub(r"\)\s*RETURNS\s+([A-Za-z_]+)\s+BEGIN\b", r") RETURNS \1 AS $$\n__PG_DECLARE__\nBEGIN", pg_def, flags=re.IGNORECASE)
            pg_def = re.sub(r"RETURNS\s+([A-Za-z_]+)\s+AS\s+\$\$", r"RETURNS \1 LANGUAGE plpgsql AS $$", pg_def, flags=re.IGNORECASE)
        else:
            pg_def = re.sub(r"\)\s*BEGIN\b", r") LANGUAGE plpgsql AS $$\n__PG_DECLARE__\nBEGIN", pg_def, flags=re.IGNORECASE)
            
        if formatted_decls:
            decl_block = "DECLARE\n" + "\n".join(formatted_decls)
            pg_def = pg_def.replace("__PG_DECLARE__", decl_block)
        else:
            pg_def = pg_def.replace("__PG_DECLARE__\n", "")
        pg_def = re.sub(r"\bIFNULL\(", "COALESCE(", pg_def, flags=re.IGNORECASE)
        # Exceptions & close out
        pg_def = re.sub(r"SIGNAL\s+SQLSTATE\s+'45000'\s+SET\s+MESSAGE_TEXT\s*=\s*'([^']+)'", 
                       r"RAISE EXCEPTION '\1'", pg_def, flags=re.IGNORECASE | re.DOTALL)
                       
        pg_def = re.sub(r"\bEND\s*$", "END;\n$$;", pg_def, flags=re.IGNORECASE)
        
        return pg_def

    def transform_procedure(self, name: str, definition: str) -> str:
        pg_def = self.transform_routine(definition, is_function=False)
        self.transformed_procedures[name] = pg_def
        return pg_def

    def transform_function(self, name: str, definition: str) -> str:
        pg_def = self.transform_routine(definition, is_function=True)
        self.transformed_functions[name] = pg_def
        return pg_def

    def transform_all_procedures(self, procedures: Dict[str, str]) -> Dict[str, str]:
        for name, defn in procedures.items():
            self.transform_procedure(name, defn)
        print_success(f"Transformed {len(self.transformed_procedures)} stored procedures")
        return self.transformed_procedures

    def transform_all_functions(self, functions: Dict[str, str]) -> Dict[str, str]:
        for name, defn in functions.items():
            self.transform_function(name, defn)
        print_success(f"Transformed {len(self.transformed_functions)} stored functions")
        return self.transformed_functions


def transform_schema(schemas: Dict[str, str]) -> Tuple[bool, Dict[str, str], Dict[str, list]]:
    try:
        transformer = SchemaTransformer()
        transformed = transformer.transform_all_schemas(schemas)
        return True, transformed, transformer.extracted_fks
    except Exception as e:
        print_error(f"Schema transformation failed: {e}")
        logger.error(f"Schema transformation error: {e}")
        return False, {}


def transform_routines(procedures: Dict[str, str], functions: Dict[str, str]) -> Tuple[bool, Dict[str, str], Dict[str, str]]:
    try:
        transformer = RoutineTransformer()
        procs = transformer.transform_all_procedures(procedures)
        funcs = transformer.transform_all_functions(functions)
        return True, procs, funcs
    except Exception as e:
        print_error(f"Routine transformation failed: {e}")
        logger.error(f"Routine transformation error: {e}")
        return False, {}, {}