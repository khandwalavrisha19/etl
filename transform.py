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

    # ROW_COUNT() → FOUND::INTEGER
    sql = re.sub(r'\bROW_COUNT\s*\(\s*\)', 'FOUND::INTEGER', sql, flags=re.IGNORECASE)

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
    """
    Transforms MySQL stored procedures and functions to PostgreSQL plpgsql.

    Pipeline (each step is a private method called in order):
      1.  _strip_definer          – remove DEFINER clause, normalise CREATE OR REPLACE
      2.  _normalise_backticks    – backticks → double-quotes; unquote routine name
      3.  _strip_mysql_keywords   – remove MySQL-only routine properties
      4.  _fix_param_modes        – IN stripped; OUT/INOUT kept (or commented for funcs)
      5.  _apply_types            – type mapping + ENUM → VARCHAR
      6.  _extract_handlers       – parse & remove DECLARE…HANDLER blocks; store for later
      7.  _extract_declares       – hoist all other DECLARE lines; store for later
      8.  _convert_signal         – SIGNAL → RAISE EXCEPTION
      9.  _apply_functions        – built-in function mapping
      10. _convert_transactions   – comment out START TRANSACTION / COMMIT / ROLLBACK
      11. _convert_set_stmts      – SET var = expr  →  var := expr  (safe, context-aware)
      12. _convert_loops          – WHILE/REPEAT/loop labels → plpgsql equivalents
      13. _convert_cursor_loops   – rewrite cursor FOR loops (MySQL extension)
      14. _fix_select_no_into     – bare SELECT → RAISE NOTICE or PERFORM
      15. _fix_boolean_literals   – TRUE/FALSE string coercions → proper booleans
      16. _fix_misc               – LIMIT x,y; UPDATE…JOIN; INSERT IGNORE; REPLACE INTO
      17. _fix_last_insert_id     – LAST_INSERT_ID() → lastval() with proper assignment
      18. _wrap_body              – inject DECLARE block + LANGUAGE plpgsql AS $$ wrapper
      19. _inject_exception_block – place converted EXCEPTION block before final END
      20. _close_body             – ensure END; $$; terminator
      21. _final_cleanup          – whitespace normalisation
    """

    def __init__(self):
        self.transformed_procedures: Dict[str, str] = {}
        self.transformed_functions: Dict[str, str] = {}

    # ── Public API ────────────────────────────────────────────────────────────

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

    # ── Main pipeline ─────────────────────────────────────────────────────────

    def _transform_routine(self, definition: str, is_function: bool = False) -> str:
        if not definition:
            return ""

        sql = definition

        sql = self._strip_definer(sql)
        sql = self._normalise_backticks(sql)
        sql = self._strip_mysql_keywords(sql)
        sql = self._fix_param_modes(sql, is_function)
        sql = self._apply_types(sql)

        # Extract special DECLARE blocks before hoisting regular ones
        sql, not_found_var, exception_block = self._extract_handlers(sql)
        sql, declare_lines = self._extract_declares(sql)

        sql = self._convert_signal(sql)
        sql = self._apply_functions(sql)
        sql = self._convert_transactions(sql)
        sql = self._convert_set_stmts(sql)
        sql = self._convert_loops(sql)
        sql = self._convert_cursor_loops(sql)
        sql = self._fix_select_no_into(sql)
        sql = self._fix_boolean_literals(sql)
        sql = self._fix_misc(sql)
        sql = self._fix_last_insert_id(sql)
        sql = self._fix_elseif(sql)

        # If we found a NOT FOUND handler, inject EXIT WHEN after every FETCH
        if not_found_var:
            sql = self._inject_not_found_exits(sql, not_found_var)

        sql = self._wrap_body(sql, is_function, declare_lines)
        sql = self._inject_exception_block(sql, exception_block)
        sql = self._close_body(sql)
        sql = self._final_cleanup(sql)

        return sql

    # ── Step 1: Strip DEFINER ─────────────────────────────────────────────────

    def _strip_definer(self, sql: str) -> str:
        sql = re.sub(
            r'CREATE\s+DEFINER\s*=\s*`[^`]*`\s*@\s*`[^`]*`\s+',
            'CREATE OR REPLACE ', sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r'^CREATE\s+(?!OR\s+REPLACE\b)(PROCEDURE|FUNCTION)\b',
            r'CREATE OR REPLACE \1', sql,
            flags=re.IGNORECASE | re.MULTILINE
        )
        return sql

    # ── Step 2: Backticks ─────────────────────────────────────────────────────

    def _normalise_backticks(self, sql: str) -> str:
        sql = sql.replace('`', '"')
        # Unquote the routine name itself (PG doesn't need it quoted)
        sql = re.sub(
            r'(CREATE\s+OR\s+REPLACE\s+(?:PROCEDURE|FUNCTION)\s+)"([^"]+)"',
            r'\1\2', sql, flags=re.IGNORECASE
        )
        return sql

    # ── Step 3: MySQL-only keywords ───────────────────────────────────────────

    def _strip_mysql_keywords(self, sql: str) -> str:
        for kw in [
            r'\bDETERMINISTIC\b', r'\bNOT\s+DETERMINISTIC\b',
            r'\bREADS\s+SQL\s+DATA\b', r'\bMODIFIES\s+SQL\s+DATA\b',
            r'\bCONTAINS\s+SQL\b', r'\bNO\s+SQL\b',
            r'\bSQL\s+SECURITY\s+(?:DEFINER|INVOKER)\b',
            r"\bCOMMENT\s+'[^']*'",
            r'\bLANGUAGE\s+SQL\b',
        ]:
            sql = re.sub(kw, '', sql, flags=re.IGNORECASE)
        return sql

    # ── Step 4: Parameter modes ───────────────────────────────────────────────

    def _fix_param_modes(self, sql: str, is_function: bool) -> str:
        # Strip IN (PostgreSQL default)
        sql = re.sub(r'\bIN\s+(?=[a-zA-Z_])', '', sql, flags=re.IGNORECASE)
        if is_function:
            # PG functions don't support OUT params natively in the same way; mark them
            sql = re.sub(r'\b(OUT|INOUT)\s+', r'/* \1 */ ', sql, flags=re.IGNORECASE)
            
        # OUT/INOUT are valid and kept as-is for procedures
        return sql

    # ── Step 5: Type mappings ─────────────────────────────────────────────────

    def _apply_types(self, sql: str) -> str:
        sql = _apply_type_mappings(sql)
        sql = re.sub(r'\bENUM\s*\([^)]+\)', 'VARCHAR', sql, flags=re.IGNORECASE)
        return sql

    # ── Step 6: Extract DECLARE … HANDLER blocks ─────────────────────────────

    def _extract_handlers(self, sql: str):
        """
        Parse MySQL DECLARE…HANDLER blocks and translate them to PostgreSQL idioms.

        Returns:
            (sql_without_handlers, not_found_var_name_or_None, exception_pg_block_or_None)

        NOT FOUND handler  →  record the flag variable it sets; caller will inject
                               EXIT WHEN <var>; after every FETCH statement.
        SQLEXCEPTION handler → translate the handler body to a PG EXCEPTION block.
        SQLSTATE handler    → same treatment as SQLEXCEPTION.
        """
        not_found_var: str = ""
        exception_lines: List[str] = []

        # ── NOT FOUND handler ─────────────────────────────────────────────────
        # Pattern: DECLARE CONTINUE HANDLER FOR NOT FOUND SET <var> = <val>;
        # (single-statement form)
        def _nf_single(m):
            nonlocal not_found_var
            # Extract the variable being set
            var_m = re.search(
                r'\bSET\s+([a-zA-Z_]\w*)\s*=',
                m.group(1), re.IGNORECASE
            )
            if var_m:
                not_found_var = var_m.group(1)
            return ''  # remove from body

        sql = re.sub(
            r'DECLARE\s+CONTINUE\s+HANDLER\s+FOR\s+NOT\s+FOUND\s+(SET\s+[^;]+);',
            _nf_single, sql, flags=re.IGNORECASE
        )

        # Multi-statement NOT FOUND handler: DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN … END;
        def _nf_block(m):
            nonlocal not_found_var
            body = m.group(1)
            var_m = re.search(r'\bSET\s+([a-zA-Z_]\w*)\s*=', body, re.IGNORECASE)
            if var_m:
                not_found_var = var_m.group(1)
            return ''

        sql = re.sub(
            r'DECLARE\s+CONTINUE\s+HANDLER\s+FOR\s+NOT\s+FOUND\s+BEGIN\s*(.*?)\s*END\s*;',
            _nf_block, sql, flags=re.IGNORECASE | re.DOTALL
        )

        # ── SQLEXCEPTION / SQLSTATE handler → PG EXCEPTION block ─────────────
        def _translate_handler_body(body: str) -> List[str]:
            lines = []
            """Convert the MySQL handler body statements to plpgsql lines."""
            body = re.sub(r'^\s*BEGIN\s*', '', body, flags=re.IGNORECASE)
            body = re.sub(r'\s*END\s*;?\s*$', '', body, flags=re.IGNORECASE)
            # Remove transaction control (invalid in exception handlers too)
            body = re.sub(r'\bROLLBACK\s*;', '', body, flags=re.IGNORECASE)
            body = re.sub(r'\bCOMMIT\s*;', '', body, flags=re.IGNORECASE)
            # ── NEW: Convert RESIGNAL / SIGNAL before SET conversion ──────────────
            body = re.sub(r'\bRESIGNAL\b\s*;', 'RAISE;', body, flags=re.IGNORECASE)
            body = re.sub(
                r"SIGNAL\s+SQLSTATE\s+'[^']+'\s+SET\s+MESSAGE_TEXT\s*=\s*'([^']+)'\s*;",
                r"RAISE EXCEPTION '\1';",
                body, flags=re.IGNORECASE
            )
            body = re.sub(
                r"SIGNAL\s+SQLSTATE\s+'[^']+'\s+SET\s+MESSAGE_TEXT\s*=\s*([^;]+);",
                r"RAISE EXCEPTION '%', \1;",
                body, flags=re.IGNORECASE
            )
            body = re.sub(
                r"SIGNAL\s+SQLSTATE\s+'([^']+)'\s*;",
                r"RAISE EXCEPTION 'SQLSTATE \1';",
                body, flags=re.IGNORECASE
            )
            # ── END NEW ────────────────────────────────────────────────────────────
            # Convert SET statements
            body = re.sub(r'(\bTHEN\b|\bELSE\b|;|\n)\s*\bSET\b', r'\1\n    SET', body, flags=re.IGNORECASE)
            # ... rest unchanged
            body = re.sub(
                r'(\bBEGIN\b|;|\n)\s*\bSET\s+(?!SESSION\b|GLOBAL\b|NAMES\b|@@)([a-zA-Z_]\w*)\s*=\s*(?!=)',
                r'\1\n        \2 := ',
                body, flags=re.IGNORECASE
            )
            body = re.sub(
                r'\bSET\s+(?!SESSION\b|GLOBAL\b|NAMES\b|@@)([a-zA-Z_]\w*)\s*=\s*(?!=)',
                r'\1 := ',
                body, flags=re.IGNORECASE
            )
            for stmt in body.split(';'):
                stmt = stmt.strip()
                if stmt:
                    lines.append(f'        {stmt};')
            return lines

        def _sqlex_block_handler(m):
            body_raw = m.group(1).strip()
            body_lines = _translate_handler_body(body_raw)
            exception_lines.extend(body_lines)
            exception_lines.append('        RAISE;  -- re-raise after cleanup')
            return ''

        def _sqlex_single_handler(m):
            body_raw = m.group(1).strip()
            body_lines = _translate_handler_body(body_raw)
            exception_lines.extend(body_lines)
            exception_lines.append('        RAISE;  -- re-raise after cleanup')
            return ''

        # IMPORTANT: Block form MUST be matched before single-statement form,
        # because the single-statement regex ([^;]+) would greedily consume "BEGIN"
        # and stop at the first semicolon inside the block body.
        sql = re.sub(
            r'DECLARE\s+(?:CONTINUE|EXIT)\s+HANDLER\s+FOR\s+SQLEXCEPTION\s+BEGIN\s*(.*?)\s*END\s*;',
            _sqlex_block_handler, sql, flags=re.IGNORECASE | re.DOTALL
        )
        # Single-statement form (only fires if block form didn't match)
        sql = re.sub(
            r'DECLARE\s+(?:CONTINUE|EXIT)\s+HANDLER\s+FOR\s+SQLEXCEPTION\s+([^;]+);',
            _sqlex_single_handler, sql, flags=re.IGNORECASE
        )

        # SQLSTATE handler (treat same as SQLEXCEPTION) — block form first
        def _sqlstate_block_handler(m):
            body_raw = m.group(2).strip()
            body_lines = _translate_handler_body(body_raw)
            exception_lines.extend(body_lines)
            exception_lines.append('        RAISE;')
            return ''

        def _sqlstate_single_handler(m):
            body_raw = m.group(2).strip()
            body_lines = _translate_handler_body(body_raw)
            exception_lines.extend(body_lines)
            exception_lines.append('        RAISE;')
            return ''

        sql = re.sub(
            r"DECLARE\s+(?:CONTINUE|EXIT)\s+HANDLER\s+FOR\s+SQLSTATE\s+'([^']+)'\s+BEGIN\s*(.*?)\s*END\s*;",
            _sqlstate_block_handler, sql, flags=re.IGNORECASE | re.DOTALL
        )
        sql = re.sub(
            r"DECLARE\s+(?:CONTINUE|EXIT)\s+HANDLER\s+FOR\s+SQLSTATE\s+'([^']+)'\s+([^;]+);",
            _sqlstate_single_handler, sql, flags=re.IGNORECASE
        )

        exception_block = None
        if exception_lines:
            exception_block = (
                "EXCEPTION\n"
                "    WHEN OTHERS THEN\n"
                + "\n".join(exception_lines)
            )

        return sql, not_found_var, exception_block

    # ── Step 7: Extract DECLARE variable / cursor lines ───────────────────────

    def _extract_declares(self, sql: str):
        """
        Hoist all remaining DECLARE lines out of the body.
        Returns (cleaned_sql, list_of_formatted_declare_strings).
        Handles: plain variables, DEFAULT values, CURSOR FOR queries.
        """
        declare_lines: List[str] = []

        def _process_declare(m):
            content = m.group(1).strip()

            # CURSOR declaration — keep as-is in PG (valid syntax)
            if re.search(r'\bCURSOR\b', content, re.IGNORECASE):
                content = _apply_type_mappings(content)
                declare_lines.append(f'    {content};')
                return ''

            # Variable with DEFAULT
            content = re.sub(r'\bDEFAULT\b', ':=', content, flags=re.IGNORECASE)
            content = _apply_type_mappings(content)
            # Fix type mismatch: MySQL allows INT DEFAULT FALSE/TRUE as a boolean flag.
            # After type mapping INT→INTEGER, PG rejects INTEGER := FALSE/TRUE.
            # Detect pattern "varname INTEGER/SMALLINT/BIGINT := TRUE/FALSE" → BOOLEAN.
            content = re.sub(
                r'\b(INTEGER|SMALLINT|BIGINT|INT)\b(\s*:=\s*(?:TRUE|FALSE)\b)',
                r'BOOLEAN\2',
                content, flags=re.IGNORECASE
            )
            declare_lines.append(f'    {content.strip()};')
            return ''

        sql = re.sub(
            r'^\s*DECLARE\s+((?!(?:CONTINUE|EXIT)\s+HANDLER)[^;]+);\n?',
            _process_declare,
            sql,
            flags=re.IGNORECASE | re.MULTILINE
        )

        return sql, declare_lines

    # ── Step 8: SIGNAL → RAISE EXCEPTION ─────────────────────────────────────

    def _convert_signal(self, sql: str) -> str:
        # SIGNAL SQLSTATE '...' SET MESSAGE_TEXT = 'literal'
        sql = re.sub(
            r"SIGNAL\s+SQLSTATE\s+'[^']+'\s+SET\s+MESSAGE_TEXT\s*=\s*'([^']+)'\s*;",
            r"RAISE EXCEPTION '\1';",
            sql, flags=re.IGNORECASE
        )
        # SIGNAL SQLSTATE '...' SET MESSAGE_TEXT = variable_or_expr
        sql = re.sub(
            r"SIGNAL\s+SQLSTATE\s+'[^']+'\s+SET\s+MESSAGE_TEXT\s*=\s*([^;]+);",
            r"RAISE EXCEPTION '%', \1;",
            sql, flags=re.IGNORECASE
        )
        # Bare SIGNAL with no SET (just raise a generic error)
        sql = re.sub(
            r"SIGNAL\s+SQLSTATE\s+'([^']+)'\s*;",
            r"RAISE EXCEPTION 'SQLSTATE \1';",
            sql, flags=re.IGNORECASE
        )
        sql = re.sub(r'\bRESIGNAL\b\s*;', 'RAISE;', sql, flags=re.IGNORECASE)
        return sql

    # ── Step 9: Function mappings ─────────────────────────────────────────────

    def _apply_functions(self, sql: str) -> str:
        return _apply_function_mappings(sql)

    # ── Step 10: Transaction control ──────────────────────────────────────────

    def _convert_transactions(self, sql: str) -> str:
        sql = re.sub(
            r'^\s*START\s+TRANSACTION\s*;',
            '-- START TRANSACTION (not valid inside plpgsql; transactions managed by caller)',
            sql, flags=re.IGNORECASE | re.MULTILINE
        )
        sql = re.sub(
            r'^\s*COMMIT\s*;',
            '-- COMMIT (not valid inside plpgsql; transactions managed by caller)',
            sql, flags=re.IGNORECASE | re.MULTILINE
        )
        sql = re.sub(
            r'^\s*ROLLBACK\s*;',
            '-- ROLLBACK (use RAISE EXCEPTION to abort in PG)',
            sql, flags=re.IGNORECASE | re.MULTILINE
        )
        return sql

    # ── Step 11: SET var = expr  →  var := expr ───────────────────────────────

    def _convert_set_stmts(self, sql: str) -> str:
        # Protect UPDATE … SET … so we don't mangle column assignments
        def _protect_update(m):
            return m.group(0).replace('SET', '\x00UPDATE_SET\x00').replace('=', '\x01')

        sql = re.sub(
            r'\bUPDATE\b.+?\bSET\b.+?;',
            _protect_update,
            sql, flags=re.IGNORECASE | re.DOTALL
        )

        # Protect INSERT … ON CONFLICT DO UPDATE SET
        def _protect_conflict(m):
            return m.group(0).replace('SET', '\x00CONFLICT_SET\x00').replace('=', '\x01')

        sql = re.sub(
            r'\bON\s+CONFLICT\b.+?\bSET\b.+?;',
            _protect_conflict,
            sql, flags=re.IGNORECASE | re.DOTALL
        )

        # Ensure SET on its own line (split after THEN / ELSE / DO / ;)
        for kw in (r'\bTHEN\b', r'\bELSE\b', r'\bDO\b'):
            sql = re.sub(kw + r'\s+\bSET\b', lambda m, k=kw: re.sub(r'\bSET\b', '\n    SET', m.group(0), flags=re.IGNORECASE), sql, flags=re.IGNORECASE)
        sql = re.sub(r'(;)\s+SET\b', r'\1\n    SET', sql, flags=re.IGNORECASE)

        # Convert SET var = expr at statement boundaries
        sql = re.sub(
            r'(?:^|(?<=;)|(?<=\n))\s*\bSET\s+(?!SESSION\b|GLOBAL\b|NAMES\b|@@|TRANSACTION\b)'
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?!=)',
            r'\n    \1 := ',
            sql, flags=re.IGNORECASE | re.MULTILINE
        )

        # Catch any remaining SET (e.g. after THEN that's still on same line)
        sql = re.sub(
            r'\bSET\s+(?!SESSION\b|GLOBAL\b|NAMES\b|@@|TRANSACTION\b)'
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?!=)',
            r'\1 := ',
            sql, flags=re.IGNORECASE
        )

        # Restore protected sections
        sql = (sql
               .replace('\x00UPDATE_SET\x00', 'SET')
               .replace('\x00CONFLICT_SET\x00', 'SET')
               .replace('\x01', '='))

        return sql

    # ── Step 12: Loop conversions ─────────────────────────────────────────────

    def _convert_loops(self, sql: str) -> str:
        # ELSEIF → ELSIF (must come before other IF work)
        sql = re.sub(r'\bELSEIF\b', 'ELSIF', sql, flags=re.IGNORECASE)

        # Named loop labels:  label: LOOP / WHILE / REPEAT
        # Must appear after a statement boundary or start of BEGIN block
        sql = re.sub(
            r'(;|\n|(?<=BEGIN))\s*([a-zA-Z_]\w*)\s*:\s*LOOP\b',
            r'\1\n    <<\2>>\n    LOOP', sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r'(;|\n|(?<=BEGIN))\s*([a-zA-Z_]\w*)\s*:\s*WHILE\b',
            r'\1\n    <<\2>>\n    WHILE', sql, flags=re.IGNORECASE
        )
        sql = re.sub(
            r'(;|\n|(?<=BEGIN))\s*([a-zA-Z_]\w*)\s*:\s*REPEAT\b',
            r'\1\n    <<\2>>\n    REPEAT', sql, flags=re.IGNORECASE
        )

        # LEAVE label → EXIT label
        sql = re.sub(r'\bLEAVE\s+([a-zA-Z_]\w*)\s*;', r'EXIT \1;', sql, flags=re.IGNORECASE)
        # ITERATE label → CONTINUE label
        sql = re.sub(r'\bITERATE\s+([a-zA-Z_]\w*)\s*;', r'CONTINUE \1;', sql, flags=re.IGNORECASE)

        # END LOOP/WHILE/REPEAT label → END LOOP (drop trailing label)
        sql = re.sub(r'\bEND\s+LOOP\s+[a-zA-Z_]\w*\s*;',   'END LOOP;',  sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEND\s+WHILE\s+[a-zA-Z_]\w*\s*;',  'END LOOP;',  sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEND\s+REPEAT\s+[a-zA-Z_]\w*\s*;', 'END LOOP;',  sql, flags=re.IGNORECASE)

        # WHILE cond DO … END WHILE → WHILE cond LOOP … END LOOP
        sql = re.sub(
            r'\bWHILE\b\s+(.+?)\s+\bDO\b',
            r'WHILE \1 LOOP', sql, flags=re.IGNORECASE | re.DOTALL
        )
        sql = re.sub(r'\bEND\s+WHILE\b', 'END LOOP', sql, flags=re.IGNORECASE)

        # REPEAT … UNTIL cond END REPEAT → LOOP … EXIT WHEN cond; END LOOP
        def _repeat_until(m):
            body = m.group(1).strip()
            cond = m.group(2).strip()
            return f"LOOP\n{body}\n    EXIT WHEN {cond};\nEND LOOP"

        sql = re.sub(
            r'\bREPEAT\b(.*?)\bUNTIL\b\s+(.+?)\s+\bEND\s+REPEAT\b',
            _repeat_until, sql, flags=re.IGNORECASE | re.DOTALL
        )

        return sql

    # ── Step 13: Cursor FOR loops (MySQL extension) ───────────────────────────

    def _convert_cursor_loops(self, sql: str) -> str:
        """
        MySQL: FOR row IN cursor_name DO … END FOR;
        PG:    No equivalent — emit a comment; proper conversion requires
               restructuring to OPEN/FETCH/CLOSE which we can't do safely
               without knowing the cursor's SELECT.
        """
        sql = re.sub(
            r'\bFOR\s+([a-zA-Z_]\w*)\s+IN\s+([a-zA-Z_]\w*)\s+DO\b',
            r'/* FOR \1 IN \2 DO -- rewrite using OPEN/FETCH/CLOSE */ LOOP',
            sql, flags=re.IGNORECASE
        )
        sql = re.sub(r'\bEND\s+FOR\b', 'END LOOP', sql, flags=re.IGNORECASE)
        return sql

    # ── Step 14: Bare SELECT → RAISE NOTICE or PERFORM ───────────────────────

    def _fix_select_no_into(self, sql: str) -> str:
        """
        In plpgsql a SELECT must either:
          a) Use INTO to store the result, or
          b) Be wrapped in PERFORM (discards results)

        Rules applied:
        - SELECT of only literals / expressions (no FROM) → RAISE NOTICE
        - SELECT … FROM … without INTO → PERFORM (result discarded, with comment)
        - SELECT … INTO … → left unchanged (valid plpgsql)
        """
        # Find TOP-LEVEL SELECT statements (starts a line or follows a semicolon)
        # that do not contain 'INTO' or 'PERFORM' beforehand.
        pattern = re.compile(
            r'(^|(?<=;))\s*(\bSELECT\b(?!.*?\bINTO\b)[^;]+;)', 
            re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        
        def _replacer(m):
            prefix = m.group(1) # Start of line or semicolon
            indent_stmt = m.group(2) # indentation and SELECT statement
            stmt = indent_stmt.strip()
            
            # Cases:
            # 1. Has FROM → wrap in PERFORM
            if re.search(r'\bFROM\b', stmt, re.IGNORECASE):
                # Replace the standalone SELECT with PERFORM
                return prefix + re.sub(r'\bSELECT\b', 'PERFORM /* result discarded */', indent_stmt, count=1, flags=re.IGNORECASE)
            
            # 2. No FROM → turn into RAISE NOTICE
            expr_m = re.match(r'^\s*\bSELECT\b\s+(.+?)\s*;?\s*$', stmt, re.IGNORECASE | re.DOTALL)
            if expr_m:
                expr = expr_m.group(1).strip().rstrip(';')
                # Clean aliases
                expr = re.sub(r'\s+\bAS\b\s+([a-zA-Z_]\w*|"[^"]+")', '', expr, flags=re.IGNORECASE)
                indent = re.match(r'^\s*', indent_stmt).group(0)
                return f"{prefix}{indent}RAISE NOTICE '%', ({expr})::TEXT;"
            
            return m.group(0) # fallback
            
        return pattern.sub(_replacer, sql)

    # ── Step 15: Boolean literal safety ──────────────────────────────────────

    def _fix_boolean_literals(self, sql: str) -> str:
        """
        Ensure TRUE/FALSE are the bare keywords (not quoted strings).
        Also fix common pitfalls where MySQL stores booleans as 0/1 integers
        in BOOLEAN columns — the load.py already handles the cast, but we
        keep the plpgsql body clean.
        """
        # Unquoted TRUE/FALSE are fine in PG; just normalise case
        sql = re.sub(r"\bTRUE\b",  'TRUE',  sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bFALSE\b", 'FALSE', sql, flags=re.IGNORECASE)
        # b'1' / b'0' MySQL bit literals in routine bodies
        sql = re.sub(r"\bb'1'\b", 'TRUE',  sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bb'0'\b", 'FALSE', sql, flags=re.IGNORECASE)
        return sql

    # ── Step 16: Misc rewrites ────────────────────────────────────────────────

    def _fix_misc(self, sql: str) -> str:
        # LIMIT x,y → LIMIT y OFFSET x
        sql = re.sub(
            r'\bLIMIT\s+(\d+)\s*,\s*(\d+)',
            r'LIMIT \2 OFFSET \1',
            sql, flags=re.IGNORECASE
        )

        # UPDATE … JOIN → UPDATE … FROM (best-effort single-join)
        sql = re.sub(
            r'UPDATE\s+(\w+)\s+(\w+)\s+'
            r'(?:INNER\s+|LEFT\s+(?:OUTER\s+)?)?JOIN\s+(\w+)\s+(\w+)\s+ON\s+(.+?)\s+'
            r'SET\s+(.+?)(?:\s+WHERE\s+(.+?))?\s*;',
            lambda m: (
                f"UPDATE {m.group(1)} {m.group(2)} SET {m.group(6)} "
                f"FROM {m.group(3)} {m.group(4)} "
                f"WHERE {m.group(5)}"
                + (f" AND ({m.group(7)})" if m.group(7) else "")
                + ";"
            ),
            sql, flags=re.IGNORECASE | re.DOTALL
        )

        # INSERT IGNORE → INSERT … ON CONFLICT DO NOTHING
        sql = re.sub(
            r'\bINSERT\s+IGNORE\s+INTO\b',
            'INSERT INTO',
            sql, flags=re.IGNORECASE
        )
        # Add ON CONFLICT DO NOTHING after VALUES clause if not already present
        sql = re.sub(
            r'(INSERT\s+INTO\s+\S+.+?(?:VALUES\s*\([^)]+\)|\)))\s*;',
            lambda m: m.group(0) if 'ON CONFLICT' in m.group(0).upper()
                      else m.group(1) + ' ON CONFLICT DO NOTHING;',
            sql, flags=re.IGNORECASE | re.DOTALL
        )

        # REPLACE INTO → INSERT … ON CONFLICT DO UPDATE (annotated)
        sql = re.sub(
            r'\bREPLACE\s+INTO\b',
            '/* REPLACE INTO → rewrite as INSERT … ON CONFLICT DO UPDATE SET … */ INSERT INTO',
            sql, flags=re.IGNORECASE
        )

        return sql

    # ── Step 17: LAST_INSERT_ID() → lastval() ────────────────────────────────

    def _fix_last_insert_id(self, sql: str) -> str:
        """
        lastval() returns BIGINT in PG.  Cast to INTEGER where assigned to an
        INTEGER OUT param to avoid type mismatch errors.
        """
        sql = re.sub(
            r'\bLAST_INSERT_ID\s*\(\s*\)',
            'lastval()',
            sql, flags=re.IGNORECASE
        )
        # If assigned to an OUT param or variable, add explicit cast
        # pattern:  varname := lastval();
        sql = re.sub(
            r'([a-zA-Z_]\w*)\s*:=\s*lastval\(\)',
            r'\1 := lastval()::INTEGER',
            sql, flags=re.IGNORECASE
        )
        return sql

    # ── Step 11b: ELSEIF (done inside _convert_loops but also here as safety) ─

    def _fix_elseif(self, sql: str) -> str:
        return re.sub(r'\bELSEIF\b', 'ELSIF', sql, flags=re.IGNORECASE)

    # ── Step 18: Wrap body with plpgsql header ────────────────────────────────

    def _wrap_body(self, sql: str, is_function: bool, declare_lines: List[str]) -> str:
        """
        Inject  LANGUAGE plpgsql AS $$ [DECLARE …] BEGIN
        at the right place and build the DECLARE block.
        """
        if declare_lines:
            decl_block = "DECLARE\n" + "\n".join(declare_lines) + "\n"
        else:
            decl_block = ""

        if is_function:
            # RETURNS <type> BEGIN  →  RETURNS <type> LANGUAGE plpgsql AS $$ [DECLARE] BEGIN
            sql = re.sub(
                r'\)\s*RETURNS\s+([A-Za-z_]\w*(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)\s+BEGIN\b',
                r') RETURNS \1 LANGUAGE plpgsql AS $$\n__DECLARE_BLOCK__BEGIN',
                sql, flags=re.IGNORECASE
            )
            # Handle case where RETURNS … AS $$ already partially applied
            sql = re.sub(
                r'RETURNS\s+([A-Za-z_]\w*(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)\s+AS\s+\$\$',
                r'RETURNS \1 LANGUAGE plpgsql AS $$',
                sql, flags=re.IGNORECASE
            )
        else:
            # ) BEGIN  →  ) LANGUAGE plpgsql AS $$ [DECLARE] BEGIN
            sql = re.sub(
                r'\)\s*BEGIN\b',
                ') LANGUAGE plpgsql AS $$\n__DECLARE_BLOCK__BEGIN',
                sql, flags=re.IGNORECASE
            )

        sql = sql.replace('__DECLARE_BLOCK__', decl_block)
        return sql

    # ── Step 19: Inject EXCEPTION block ──────────────────────────────────────

    def _inject_exception_block(self, sql: str, exception_block) -> str:
        """
        The PostgreSQL EXCEPTION clause must appear just before the final END of
        the outermost BEGIN block.

        At this point in the pipeline _close_body has NOT yet run, so the body
        ends with a bare  END  (no $$; yet).  We find the final END at the end
        of the string and insert the EXCEPTION block before it.
        """
        if not exception_block:
            return sql

        # Match the last standalone END (with optional ;) at or near end of string
        m = re.search(r'\n(\s*END\s*;?\s*)$', sql.rstrip(), re.IGNORECASE)
        if m:
            insert_pos = m.start()
            sql = sql[:insert_pos] + '\n' + exception_block + '\n' + sql[insert_pos:]
        else:
            # Fallback: just append before closing $$ if somehow already present
            sql = re.sub(
                r'(\bEND\s*;?\s*\n\$\$;)',
                exception_block + '\n' + r'\1',
                sql, count=1, flags=re.IGNORECASE
            )
        return sql

    # ── NOT FOUND exit injection ───────────────────────────────────────────────

    def _inject_not_found_exits(self, sql: str, flag_var: str) -> str:
        """
        After each FETCH … INTO … statement inject:
            IF <flag_var> THEN EXIT; END IF;
        to replicate the MySQL NOT FOUND handler behaviour.

        Skip injection if the FETCH is immediately followed by an IF that already
        checks the same flag variable — this prevents double-exit blocks when the
        original MySQL body already contained an explicit  IF done THEN LEAVE …
        statement (which has already been converted to  EXIT <label>;).
        """
        def _add_exit(m):
            fetch_stmt = m.group(0)
            rest = sql[m.end():]
            # Look at what comes right after this FETCH (skip blank lines)
            next_nonblank = rest.lstrip('\n ')
            # If the next statement is already "IF <flag_var> THEN" → don't inject
            already_checked = re.match(
                r'IF\s+' + re.escape(flag_var) + r'\s+THEN\b',
                next_nonblank, re.IGNORECASE
            )
            if already_checked:
                return fetch_stmt  # leave as-is
            return fetch_stmt + f'\n        IF {flag_var} THEN EXIT; END IF;'

        # We need access to the full sql string inside the replacer, so use
        # a manual loop instead of re.sub to get correct offsets.
        pattern = re.compile(r'FETCH\s+\w+\s+INTO\s+[^;]+;', re.IGNORECASE)
        result = []
        last_end = 0
        for m in pattern.finditer(sql):
            result.append(sql[last_end:m.start()])
            fetch_stmt = m.group(0)
            rest = sql[m.end():]
            next_nonblank = rest.lstrip('\n ')
            already_checked = re.match(
                r'IF\s+' + re.escape(flag_var) + r'\s+THEN\b',
                next_nonblank, re.IGNORECASE
            )
            if already_checked:
                result.append(fetch_stmt)
            else:
                result.append(fetch_stmt + f'\n        IF {flag_var} THEN EXIT; END IF;')
            last_end = m.end()
        result.append(sql[last_end:])
        return ''.join(result)

    # ── Step 20: Close body ───────────────────────────────────────────────────

    def _close_body(self, sql: str) -> str:
        """Ensure the routine ends with  END; $$;"""
        sql = sql.rstrip()
        # If it already ends with $$; we're done
        if sql.endswith('$$;'):
            return sql
        # Replace trailing END (with or without semicolon)
        sql = re.sub(r'\bEND\s*;?\s*$', 'END;\n$$;', sql, flags=re.IGNORECASE)
        # If $$ wrapper was added but $$; is missing, append it
        if '$$' in sql and not sql.endswith('$$;'):
            sql = sql.rstrip(';').rstrip() + '\n$$;'
        return sql

    # ── Step 21: Final cleanup ────────────────────────────────────────────────

    def _final_cleanup(self, sql: str) -> str:
        # Collapse runs of blank lines
        sql = re.sub(r'\n{3,}', '\n\n', sql)
        # Remove stray semicolons left after handler extraction
        sql = re.sub(r';\s*;', ';', sql)
        # Trim trailing whitespace on each line
        sql = '\n'.join(line.rstrip() for line in sql.split('\n'))
        return sql.strip() + '\n'




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