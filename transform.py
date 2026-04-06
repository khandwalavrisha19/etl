"""
Transformation module for MySQL to PostgreSQL conversion.
Pure regex approach — no external SQL parsers.
Covers schemas, stored procedures, and functions exhaustively.
"""

import re
from typing import Dict, Tuple, List
from utils import print_success, print_warning, print_error
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Shared type-mapping helpers (used by both Schema and Routine transformers)
# ─────────────────────────────────────────────────────────────────────────────

def _apply_type_mappings(sql: str) -> str:
    """Convert MySQL data types to PostgreSQL equivalents."""

    # ── Numeric ──────────────────────────────────────────────────────────────
    sql = re.sub(r'\bTINYINT\s*\(\s*1\s*\)', 'BOOLEAN', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bTINYINT(?:\s*\(\s*\d+\s*\))?\b', 'SMALLINT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSMALLINT(?:\s*\(\s*\d+\s*\))?\b', 'SMALLINT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bMEDIUMINT(?:\s*\(\s*\d+\s*\))?\b', 'INTEGER', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bBIGINT(?:\s*\(\s*\d+\s*\))?\b', 'BIGINT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\b(?:INT|INTEGER)(?:\s*\(\s*\d+\s*\))?\b', 'INTEGER', sql, flags=re.IGNORECASE)
    
    sql = re.sub(r'\bDECIMAL\b', 'NUMERIC', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bFLOAT\b', 'REAL', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bDOUBLE\b', 'DOUBLE PRECISION', sql, flags=re.IGNORECASE)

    sql = re.sub(r'\bUNSIGNED\b|\bZEROFILL\b', '', sql, flags=re.IGNORECASE)

    # ── String ────────────────────────────────────────────────────────────────
    sql = re.sub(r'\b(?:TINY|MEDIUM|LONG)?TEXT\b', 'TEXT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\b(?:VAR)?CHAR(?:\s*\(\s*\d+\s*\))?\b', 'VARCHAR', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bENUM\s*\([^)]+\)|\bSET\s*\([^)]+\)', 'TEXT', sql, flags=re.IGNORECASE)

    sql = re.sub(r'\bCHARACTER\s+SET\s+\S+|\bCHARSET\s+\S+|\bCOLLATE\s+\S+', '', sql, flags=re.IGNORECASE)

    # ── Date / Time ───────────────────────────────────────────────────────────
    sql = re.sub(r'\bDATETIME(?:\s*\(\s*\d+\s*\))?\b', 'TIMESTAMP', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bTIMESTAMP(?:\s*\(\s*\d+\s*\))?\b', 'TIMESTAMP', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bTIME(?:\s*\(\s*\d+\s*\))?\b', 'TIME', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bYEAR(?:\s*\(\s*4\s*\))?\b(?!\s*\()', 'INTEGER', sql, flags=re.IGNORECASE)

    # ── Binary & Others ───────────────────────────────────────────────────────
    sql = re.sub(r'\b(?:TINY|MEDIUM|LONG)?BLOB\b', 'BYTEA', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bBIT\s*\(\s*1\s*\)', 'BOOLEAN', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bBIT\b', 'BIT VARYING', sql, flags=re.IGNORECASE)
    
    # ── JSON ──────────────────────────────────────────────────────────────────
    sql = re.sub(r'\bJSON\b', 'JSONB', sql, flags=re.IGNORECASE)

    return sql


def _apply_function_mappings(sql: str) -> str:
    """Replace MySQL built-in functions with PostgreSQL equivalents."""

    # ── Date functions ────────────────────────────────────────────────────────
    sql = re.sub(r'\bCURDATE\(\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCURTIME\(\)', 'CURRENT_TIME', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSYSDATE\(\)', 'NOW()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCURRENT_TIMESTAMP\(\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bUTC_TIMESTAMP\(\)', "NOW() AT TIME ZONE 'UTC'", sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bUTC_DATE\(\)', "(CURRENT_DATE AT TIME ZONE 'UTC')", sql, flags=re.IGNORECASE)

    # DATE_FORMAT(expr, fmt) → TO_CHAR(expr, fmt)
    sql = re.sub(
        r'\bDATE_FORMAT\s*\(([^,]+),\s*([^)]+)\)',
        lambda m: f"TO_CHAR({m.group(1).strip()}, {m.group(2).strip()})",
        sql, flags=re.IGNORECASE
    )

    # DATE_ADD / DATE_SUB with various units
    def _interval_replace(match, sign: str) -> str:
        expr = match.group(1).strip()
        qty  = match.group(2).strip()
        unit = match.group(3).strip().upper()
        unit_map = {
            'MICROSECOND': 'microseconds', 'SECOND': 'seconds', 'MINUTE': 'minutes',
            'HOUR': 'hours', 'DAY': 'days', 'WEEK': 'weeks',
            'MONTH': 'months', 'YEAR': 'years',
        }
        if unit == 'QUARTER':
            return f"({expr} {sign} ({qty} * INTERVAL '3 months'))"
        pg_unit = unit_map.get(unit, unit.lower())
        return f"({expr} {sign} ({qty} * INTERVAL '1 {pg_unit}'))"

    sql = re.sub(
        r'\bDATE_ADD\s*\(\s*([^,]+),\s*INTERVAL\s+([^\s)]+)\s+(\w+)\s*\)',
        lambda m: _interval_replace(m, '+'), sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bDATE_SUB\s*\(\s*([^,]+),\s*INTERVAL\s+([^\s)]+)\s+(\w+)\s*\)',
        lambda m: _interval_replace(m, '-'), sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bADDDATE\s*\(\s*([^,]+),\s*INTERVAL\s+([^\s)]+)\s+(\w+)\s*\)',
        lambda m: _interval_replace(m, '+'), sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bSUBDATE\s*\(\s*([^,]+),\s*INTERVAL\s+([^\s)]+)\s+(\w+)\s*\)',
        lambda m: _interval_replace(m, '-'), sql, flags=re.IGNORECASE
    )

    # DATEDIFF(d1, d2) → (d1::DATE - d2::DATE)
    sql = re.sub(
        r'\bDATEDIFF\s*\(\s*([^,]+),\s*([^)]+)\)',
        r'(\1::DATE - \2::DATE)',
        sql, flags=re.IGNORECASE
    )

    # YEAR(expr) → EXTRACT(YEAR FROM expr)
    sql = re.sub(r'\bYEAR\s*\(([^)]+)\)', r'EXTRACT(YEAR FROM \1)', sql, flags=re.IGNORECASE)

    # TIMESTAMPDIFF(unit, d1, d2)
    def _tsdiff(match):
        unit = match.group(1).strip().upper()
        d1, d2 = match.group(2).strip(), match.group(3).strip()
        unit_map = {
            'SECOND': f"EXTRACT(EPOCH FROM ({d2}::TIMESTAMP - {d1}::TIMESTAMP))::INTEGER",
            'MINUTE': f"(EXTRACT(EPOCH FROM ({d2}::TIMESTAMP - {d1}::TIMESTAMP))/60)::INTEGER",
            'HOUR':   f"(EXTRACT(EPOCH FROM ({d2}::TIMESTAMP - {d1}::TIMESTAMP))/3600)::INTEGER",
            'DAY':    f"({d2}::DATE - {d1}::DATE)",
            'MONTH':  (f"((EXTRACT(YEAR FROM {d2}::DATE) - EXTRACT(YEAR FROM {d1}::DATE)) * 12 "
                       f"+ (EXTRACT(MONTH FROM {d2}::DATE) - EXTRACT(MONTH FROM {d1}::DATE)))::INTEGER"),
            'YEAR':   f"(EXTRACT(YEAR FROM {d2}::DATE) - EXTRACT(YEAR FROM {d1}::DATE))::INTEGER",
        }
        return unit_map.get(unit, f"/* TIMESTAMPDIFF({unit},{d1},{d2}) -- needs manual review */")

    sql = re.sub(
        r'\bTIMESTAMPDIFF\s*\(\s*(\w+)\s*,\s*([^,]+),\s*([^)]+)\)',
        _tsdiff, sql, flags=re.IGNORECASE
    )

    # STR_TO_DATE → TO_TIMESTAMP
    sql = re.sub(r'\bSTR_TO_DATE\s*\(', 'TO_TIMESTAMP(', sql, flags=re.IGNORECASE)

    # EXTRACT shorthand helpers
    sql = re.sub(r'\bDAYOFMONTH\s*\(', 'EXTRACT(DAY FROM ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bDAYOFWEEK\s*\(', 'EXTRACT(DOW FROM ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bDAYOFYEAR\s*\(', 'EXTRACT(DOY FROM ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bWEEKOFYEAR\s*\(', 'EXTRACT(WEEK FROM ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bYEARWEEK\s*\(([^)]+)\)', r"TO_CHAR(\1, 'IYYYIW')", sql, flags=re.IGNORECASE)

    # ── String functions ──────────────────────────────────────────────────────
    sql = re.sub(r'\bIFNULL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bNVL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)

    # GROUP_CONCAT → STRING_AGG
    def _group_concat(match):
        inner = match.group(1).strip()
        sep = "','"
        sep_m = re.search(r"SEPARATOR\s+'([^']*)'", inner, re.IGNORECASE)
        if sep_m:
            sep = f"'{sep_m.group(1)}'"
            inner = re.sub(r"\s*SEPARATOR\s+'[^']*'", '', inner, flags=re.IGNORECASE).strip()
        inner = re.sub(r'^DISTINCT\s+', '', inner, flags=re.IGNORECASE)
        order_m = re.search(r'\s+ORDER\s+BY\s+.+$', inner, re.IGNORECASE)
        order_clause = ''
        if order_m:
            order_clause = ' ' + order_m.group(0).strip()
            inner = inner[:order_m.start()].strip()
        return f"STRING_AGG({inner}::TEXT, {sep}{order_clause})"

    sql = re.sub(r'\bGROUP_CONCAT\s*\(([^)]+)\)', _group_concat, sql, flags=re.IGNORECASE)

    sql = re.sub(r'\bSUBSTR\s*\(', 'SUBSTRING(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bMID\s*\(', 'SUBSTRING(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bLCASE\s*\(', 'LOWER(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bUCASE\s*\(', 'UPPER(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCHARACTER_LENGTH\s*\(', 'CHAR_LENGTH(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bOCTET_LENGTH\s*\(', 'LENGTH(', sql, flags=re.IGNORECASE)
    sql = re.sub(
        r'\bLOCATE\s*\(([^,]+),\s*([^)]+)\)',
        r'POSITION(\1 IN \2)', sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bINSTR\s*\(([^,]+),\s*([^)]+)\)',
        r'POSITION(\2 IN \1)', sql, flags=re.IGNORECASE
    )

    # ── Math ──────────────────────────────────────────────────────────────────
    sql = re.sub(r'\bPOW\s*\(', 'POWER(', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bRAND\s*\(\s*(?:\d+\s*)?\)', 'RANDOM()', sql, flags=re.IGNORECASE)

    # ── Control-flow functions ─────────────────────────────────────────────────
    # IF(cond, true_val, false_val) → CASE WHEN cond THEN true_val ELSE false_val END
    sql = re.sub(
        r'\bIF\s*\(([^,]+),\s*([^,]+),\s*([^)]+)\)',
        r'CASE WHEN \1 THEN \2 ELSE \3 END',
        sql, flags=re.IGNORECASE
    )

    # ── Misc ──────────────────────────────────────────────────────────────────
    sql = re.sub(r'\bLAST_INSERT_ID\s*\(\s*\)', 'lastval()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bFOUND_ROWS\s*\(\s*\)', 'NULL /* FOUND_ROWS() not supported */', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSCHEMA\s*\(\s*\)', 'current_schema()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bDATABASE\s*\(\s*\)', 'current_database()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSLEEP\s*\(([^)]+)\)', r'pg_sleep(\1)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCONVERT\s*\(([^,]+)\s+USING\s+\w+\)', r'\1', sql, flags=re.IGNORECASE)

    return sql


# ─────────────────────────────────────────────────────────────────────────────
# Schema Transformer
# ─────────────────────────────────────────────────────────────────────────────

class SchemaTransformer:
    """Transforms MySQL CREATE TABLE statements to PostgreSQL syntax."""

    def __init__(self):
        self.transformed_schemas: Dict[str, str] = {}
        self.extracted_fks: Dict[str, list] = {}

    def clean_create_table(self, create_stmt: str, table_name: str) -> str:
        sql = create_stmt.strip()

        # 1. Backticks → double-quotes
        sql = sql.replace('`', '"')

        # 2. Extract and store FOREIGN KEY constraints
        if table_name not in self.extracted_fks:
            self.extracted_fks[table_name] = []

        def fk_replacer(match):
            self.extracted_fks[table_name].append(match.group(1).strip())
            return ""

        sql = re.sub(
            r',\s*((?:CONSTRAINT\s+(?:"[^"]*"|\w+)\s+)?FOREIGN\s+KEY\s*\([^)]+\)'
            r'\s*REFERENCES\s+(?:"[^"]*"|\w+)\s*\([^)]+\)'
            r'(?:\s+ON\s+(?:DELETE|UPDATE)\s+(?:CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))*)',
            fk_replacer, sql, flags=re.IGNORECASE
        )

        # 3. Strip everything after last ')' (ENGINE=, CHARSET=, etc.)
        last_paren = sql.rfind(')')
        if last_paren != -1:
            sql = sql[:last_paren + 1]

        # 4. AUTO_INCREMENT column → SERIAL / BIGSERIAL
        sql = re.sub(
            r'("?\w+"?)\s+(?:BIGINT\b(?:\s*\(\s*\d+\s*\))?)\s*(?:UNSIGNED\s+)?(?:NOT\s+NULL\s+)?AUTO_INCREMENT\b',
            r'\1 BIGSERIAL', sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r'("?\w+"?)\s+(?:TINYINT|SMALLINT|MEDIUMINT|INT(?:EGER)?)\b(?:\s*\(\s*\d+\s*\))?\s*(?:UNSIGNED\s+)?(?:NOT\s+NULL\s+)?AUTO_INCREMENT\b',
            r'\1 SERIAL', sql, flags=re.IGNORECASE
        )
        sql = re.sub(r'\s+AUTO_INCREMENT\b', '', sql, flags=re.IGNORECASE)

        # 5. Apply shared type mappings
        sql = _apply_type_mappings(sql)

        # 6. ON UPDATE CURRENT_TIMESTAMP — no PG DDL equivalent
        # Better
        sql = re.sub(r'\s+ON\s+UPDATE\s+CURRENT_TIMESTAMP(?:\s*\(\s*\d*\s*\))?\b', '', sql, flags=re.IGNORECASE)

        # 7. DEFAULT expression fixes
        sql = re.sub(r"\bDEFAULT\s+b'0'\b", "DEFAULT FALSE", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bDEFAULT\s+b'1'\b", "DEFAULT TRUE", sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bDEFAULT\s+CURRENT_TIMESTAMP\s*\(\s*\)', 'DEFAULT CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bDEFAULT\s+NOW\s*\(\s*\)', 'DEFAULT NOW()', sql, flags=re.IGNORECASE)

        # 8. UNIQUE KEY / index cleanup
        sql = re.sub(r'\bUNIQUE\s+KEY\s+"?(\w+)"?\s*\(([^)]+)\)', r'UNIQUE (\2)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bUNIQUE\s+KEY\b', 'UNIQUE', sql, flags=re.IGNORECASE)
        sql = re.sub(r',\s*(?:FULLTEXT\s+|SPATIAL\s+)?KEY\s+"?\w+"?\s*\([^)]+\)', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r',\s*(?:FULLTEXT\s+|SPATIAL\s+)?INDEX\s+"?\w+"?\s*\([^)]+\)', '', sql, flags=re.IGNORECASE)

        # 9. CHECK (col IN (0,1)) redundant after BOOLEAN conversion
        sql = re.sub(
            r',\s*CONSTRAINT\s+"?[^"]*"?\s+CHECK\s*\(\s*\(*\s*"?[^"()]*"?\s+IN\s*\(\s*0\s*,\s*1\s*\)\s*\)*\)',
            '', sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r',\s*CHECK\s*\(\s*\(*\s*"?[^"()]*"?\s+IN\s*\(\s*0\s*,\s*1\s*\)\s*\)*\)',
            '', sql, flags=re.IGNORECASE
        )
        # Unwrap double-nested CHECK ((expr)) → CHECK (expr)
        sql = re.sub(r'\bCHECK\s*\(\s*\(\s*(.+?)\s*\)\s*\)', r'CHECK (\1)', sql, flags=re.IGNORECASE)

        # 10. Strip USING BTREE/HASH from index/PK definitions
        sql = re.sub(r'\bUSING\s+(?:BTREE|HASH)\b', '', sql, flags=re.IGNORECASE)

        # 11. Trailing comma before closing paren
        sql = re.sub(r',\s*\)', ')', sql)

        # 12. Final whitespace normalisation
        sql = re.sub(r'[ \t]+', ' ', sql)
        sql = re.sub(r'\n{3,}', '\n\n', sql)
        sql = sql.strip()

        if not sql.endswith(')'):
            sql += ')'

        logger.info(f"Cleaned schema for table: {table_name}")
        return sql

    def transform_table_schema(self, table_name: str, mysql_create: str) -> str:
        try:
            pg_sql = self.clean_create_table(mysql_create, table_name)
            self.transformed_schemas[table_name] = pg_sql
            return pg_sql
        except Exception as exc:
            logger.error(f"Failed transforming {table_name}: {exc}")
            return mysql_create

    def transform_all_schemas(self, schemas: Dict[str, str]) -> Dict[str, str]:
        for table_name, create_stmt in schemas.items():
            self.transform_table_schema(table_name, create_stmt)
        print_success(f"Transformed {len(self.transformed_schemas)} table schemas")
        return self.transformed_schemas


# ─────────────────────────────────────────────────────────────────────────────
# Routine Transformer
# ─────────────────────────────────────────────────────────────────────────────

class RoutineTransformer:
    """Transforms MySQL stored procedures and functions to PostgreSQL plpgsql."""

    def __init__(self):
        self.transformed_procedures: Dict[str, str] = {}
        self.transformed_functions: Dict[str, str] = {}

    def _transform_routine(self, definition: str, is_function: bool = False) -> str:
        if not definition:
            return ""

        sql = definition

        # ── 1. Strip DEFINER ──────────────────────────────────────────────────
        sql = re.sub(
            r'CREATE\s+DEFINER\s*=\s*`[^`]*`\s*@\s*`[^`]*`\s+',
            'CREATE OR REPLACE ', sql, flags=re.IGNORECASE
        )
        # Ensure CREATE OR REPLACE prefix
        sql = re.sub(
            r'^CREATE\s+(?!OR\s+REPLACE\b)(PROCEDURE|FUNCTION)\b',
            r'CREATE OR REPLACE \1', sql,
            flags=re.IGNORECASE | re.MULTILINE
        )

        # ── 2. Backticks → double-quotes, then unquote routine name ──────────
        sql = sql.replace('`', '"')
        sql = re.sub(
            r'(CREATE\s+OR\s+REPLACE\s+(?:PROCEDURE|FUNCTION)\s+)"([^"]+)"',
            r'\1\2', sql, flags=re.IGNORECASE
        )

        # ── 3. Strip MySQL-only routine properties ────────────────────────────
        for kw in [
            r'\bDETERMINISTIC\b', r'\bNOT\s+DETERMINISTIC\b',
            r'\bREADS\s+SQL\s+DATA\b', r'\bMODIFIES\s+SQL\s+DATA\b',
            r'\bCONTAINS\s+SQL\b', r'\bNO\s+SQL\b',
            r'\bSQL\s+SECURITY\s+(?:DEFINER|INVOKER)\b',
            r"\bCOMMENT\s+'[^']*'",
            r'\bLANGUAGE\s+SQL\b',
        ]:
            sql = re.sub(kw, '', sql, flags=re.IGNORECASE)

        # ── 4. Parameter modes ────────────────────────────────────────────────
        # Strip IN (PG default)
        sql = re.sub(r'\bIN\s+(?=[a-zA-Z_])', '', sql, flags=re.IGNORECASE)
        # OUT/INOUT: valid in PG procedures; for functions add comment
        if is_function:
            sql = re.sub(r'\b(OUT|INOUT)\s+', r'/* \1 */ ', sql, flags=re.IGNORECASE)

        # ── 5. Data types ─────────────────────────────────────────────────────
        sql = _apply_type_mappings(sql)
        # In routines ENUM → VARCHAR (more flexible than TEXT for params)
        sql = re.sub(r'\bENUM\s*\([^)]+\)', 'VARCHAR', sql, flags=re.IGNORECASE)

        # ── 6. SIGNAL → RAISE EXCEPTION ──────────────────────────────────────
        sql = re.sub(
            r"SIGNAL\s+SQLSTATE\s+'[^']+'\s+SET\s+MESSAGE_TEXT\s*=\s*'([^']+)'\s*;",
            r"RAISE EXCEPTION '\1';", sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r"SIGNAL\s+SQLSTATE\s+'[^']+'\s+SET\s+MESSAGE_TEXT\s*=\s*([^;]+);",
            r"RAISE EXCEPTION '%', \1;", sql, flags=re.IGNORECASE
        )
        sql = re.sub(r'\bRESIGNAL\b\s*;', 'RAISE;', sql, flags=re.IGNORECASE)

        # ── 7. Built-in functions ─────────────────────────────────────────────
        sql = _apply_function_mappings(sql)

        # ── 8. LIMIT x,y → LIMIT y OFFSET x ──────────────────────────────────
        sql = re.sub(
            r'\bLIMIT\s+(\d+)\s*,\s*(\d+)',
            r'LIMIT \2 OFFSET \1',
            sql, flags=re.IGNORECASE
        )

        # ── 9. Protect UPDATE statements from the SET rule ───────────────────
        def _protect_update(m):
            return m.group(0).replace('SET', 'UPDATE_SET').replace('=', '==')
        sql = re.sub(r'\bUPDATE\s+.+?\s+SET\s+.+?;', _protect_update, sql, flags=re.IGNORECASE | re.DOTALL)

        # ── 10. SET var = expr → var := expr ──────────────────────────────────
        # Avoid matching SET SESSION / SET GLOBAL / SET NAMES / SET @@
        # FIXED: Only match at start of statement to avoid breaking remaining UPDATE context
        sql = re.sub(
            r'(\bBEGIN\b|;|\n)\s*\bSET\s+(?!SESSION\b|GLOBAL\b|NAMES\b|@@)([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?!=)',
            r'\1 \2 := ',
            sql, flags=re.IGNORECASE
        )

        # ── 11. Restore protected UPDATE statements ───────────────────────────
        sql = sql.replace('UPDATE_SET', 'SET').replace('==', '=')

        # ── 12. ELSEIF → ELSIF ────────────────────────────────────────────────
        sql = re.sub(r'\bELSEIF\b', 'ELSIF', sql, flags=re.IGNORECASE)

        # ── 11. Loop label syntax ─────────────────────────────────────────────
        # FIXED: Use statement boundaries for labels
        sql = re.sub(r'(\bBEGIN\b|;|\n)\s*([a-zA-Z_]\w*):\s*LOOP\b',   r'\1 <<\2>>\nLOOP',   sql, flags=re.IGNORECASE)
        sql = re.sub(r'(\bBEGIN\b|;|\n)\s*([a-zA-Z_]\w*):\s*WHILE\b',  r'\1 <<\2>>\nWHILE',  sql, flags=re.IGNORECASE)
        sql = re.sub(r'(\bBEGIN\b|;|\n)\s*([a-zA-Z_]\w*):\s*REPEAT\b', r'\1 <<\2>>\nREPEAT', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bLEAVE\s+([a-zA-Z_]\w*)\s*;',   r'EXIT \1;',     sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bITERATE\s+([a-zA-Z_]\w*)\s*;', r'CONTINUE \1;', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEND\s+LOOP\s+[a-zA-Z_]\w*\s*;',   'END LOOP;',  sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEND\s+WHILE\s+[a-zA-Z_]\w*\s*;',  'END LOOP;',  sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEND\s+REPEAT\s+[a-zA-Z_]\w*\s*;', 'END LOOP;',  sql, flags=re.IGNORECASE)

        # ── 12. WHILE … DO … END WHILE → WHILE … LOOP … END LOOP ─────────────
        sql = re.sub(r'\bWHILE\s+(.+?)\s+DO\b',  r'WHILE \1 LOOP', sql, flags=re.IGNORECASE | re.DOTALL)
        sql = re.sub(r'\bEND\s+WHILE\b', 'END LOOP', sql, flags=re.IGNORECASE)

        # ── 13. REPEAT … UNTIL … END REPEAT → LOOP … EXIT WHEN … END LOOP ────
        def _repeat_until(m):
            return f"LOOP\n{m.group(1).strip()}\nEXIT WHEN {m.group(2).strip()};\nEND LOOP"

        sql = re.sub(
            r'\bREPEAT\b(.*?)\bUNTIL\b\s+(.+?)\s+\bEND\s+REPEAT\b',
            _repeat_until, sql, flags=re.IGNORECASE | re.DOTALL
        )

        # ── 14. CURSOR-related handlers ───────────────────────────────────────
        sql = re.sub(
            r'DECLARE\s+CONTINUE\s+HANDLER\s+FOR\s+NOT\s+FOUND\s+([^;]+);',
            r'-- CONTINUE HANDLER FOR NOT FOUND: \1  (use loop exit condition)',
            sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r'DECLARE\s+(?:CONTINUE|EXIT)\s+HANDLER\s+FOR\s+SQLEXCEPTION\s+([^;]+);',
            r'-- SQLEXCEPTION HANDLER: \1  (use EXCEPTION WHEN OTHERS THEN block)',
            sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r"DECLARE\s+(?:CONTINUE|EXIT)\s+HANDLER\s+FOR\s+SQLSTATE\s+'[^']+'\s+([^;]+);",
            r'-- SQLSTATE HANDLER: \1  (use EXCEPTION WHEN SQLSTATE \'...\' THEN block)',
            sql, flags=re.IGNORECASE
        )

        # ── 15. UPDATE … JOIN → UPDATE … FROM ────────────────────────────────
        sql = re.sub(
            r'UPDATE\s+(\w+)\s+(\w+)\s+(?:INNER\s+|LEFT\s+(?:OUTER\s+)?)?JOIN\s+(\w+)\s+(\w+)\s+ON\s+(.+?)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?\s*;',
            lambda m: (
                f"UPDATE {m.group(1)} {m.group(2)} SET {m.group(6)} "
                f"FROM {m.group(3)} {m.group(4)} "
                f"WHERE {m.group(5)}"
                + (f" AND ({m.group(7)})" if m.group(7) else "")
                + ";"
            ),
            sql, flags=re.IGNORECASE | re.DOTALL
        )

        # ── 16. INSERT IGNORE → INSERT (with comment) ─────────────────────────
        sql = re.sub(
            r'\bINSERT\s+IGNORE\s+INTO\b',
            'INSERT INTO /* ON CONFLICT DO NOTHING */',
            sql, flags=re.IGNORECASE
        )

        # ── 17. REPLACE INTO → comment ────────────────────────────────────────
        sql = re.sub(
            r'\bREPLACE\s+INTO\b',
            '/* REPLACE INTO → rewrite as INSERT … ON CONFLICT DO UPDATE */ INSERT INTO',
            sql, flags=re.IGNORECASE
        )

        # ── 18. Hoist DECLARE variables above BEGIN ───────────────────────────
        declare_lines: List[str] = re.findall(
            r'^\s*DECLARE\s+(?!(?:CONTINUE|EXIT)\s+HANDLER)([^;]+);',
            sql, flags=re.IGNORECASE | re.MULTILINE
        )
        sql = re.sub(
            r'^\s*DECLARE\s+(?!(?:CONTINUE|EXIT)\s+HANDLER)[^;]+;\n?',
            '', sql, flags=re.IGNORECASE | re.MULTILINE
        )

        formatted_decls: List[str] = []
        for decl in declare_lines:
            decl = re.sub(r'\bDEFAULT\b', ':=', decl, flags=re.IGNORECASE)
            decl = _apply_type_mappings(decl)
            formatted_decls.append(f"    {decl.strip()};")

        # ── 19. Wrap with LANGUAGE plpgsql AS $$ … $$; ───────────────────────
        if is_function:
            # FIXED: Handle complex return types like DECIMAL(10,2) or VARCHAR(255)
            sql = re.sub(
                r'\)\s*RETURNS\s+([A-Za-z_]\w*(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)\s+BEGIN\b',
                r') RETURNS \1 AS $$\n__PG_DECLARE__\nBEGIN',
                sql, flags=re.IGNORECASE
            )
            sql = re.sub(
                r'RETURNS\s+([A-Za-z_]\w*(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)\s+AS\s+\$\$',
                r'RETURNS \1 LANGUAGE plpgsql AS $$',
                sql, flags=re.IGNORECASE
            )
        else:
            sql = re.sub(
                r'\)\s*BEGIN\b',
                ') LANGUAGE plpgsql AS $$\n__PG_DECLARE__\nBEGIN',
                sql, flags=re.IGNORECASE
            )

        if formatted_decls:
            decl_block = "DECLARE\n" + "\n".join(formatted_decls)
            sql = sql.replace("__PG_DECLARE__", decl_block)
        else:
            sql = sql.replace("__PG_DECLARE__\n", "")

        # ── 20. Close with END; $$; ───────────────────────────────────────────
        sql = re.sub(r'\bEND\s*;?\s*$', 'END;\n$$;', sql.rstrip(), flags=re.IGNORECASE)

        # ── 21. Final cleanup ─────────────────────────────────────────────────
        sql = re.sub(r'\n{3,}', '\n\n', sql)

        return sql

    def transform_procedure(self, name: str, definition: str) -> str:
        pg_def = self._transform_routine(definition, is_function=False)
        self.transformed_procedures[name] = pg_def
        return pg_def

    def transform_function(self, name: str, definition: str) -> str:
        pg_def = self._transform_routine(definition, is_function=True)
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
    except Exception as exc:
        print_error(f"Schema transformation failed: {exc}")
        logger.error(f"Schema transformation error: {exc}")
        return False, {}, {}


def transform_routines(
    procedures: Dict[str, str],
    functions: Dict[str, str],
) -> Tuple[bool, Dict[str, str], Dict[str, str]]:
    try:
        transformer = RoutineTransformer()
        procs = transformer.transform_all_procedures(procedures)
        funcs = transformer.transform_all_functions(functions)
        return True, procs, funcs
    except Exception as exc:
        print_error(f"Routine transformation failed: {exc}")
        logger.error(f"Routine transformation error: {exc}")
        return False, {}, {}