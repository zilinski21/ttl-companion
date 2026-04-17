"""
Database helper module.

Supports both SQLite (local dev) and PostgreSQL (production).
When DATABASE_URL is set, uses PostgreSQL. Otherwise uses SQLite.

Automatically translates SQLite-style SQL (? placeholders, INSERT OR IGNORE)
to PostgreSQL syntax, so dashboard.py doesn't need to change most queries.
"""

import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

# Try to import psycopg2 for PostgreSQL support
try:
    import psycopg2
    import psycopg2.extras
    HAS_PG = True
except ImportError:
    HAS_PG = False

# Will be set by dashboard.py at startup
DB_FILE = None
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def init_db_path(path):
    """Call once at app startup to tell this module where the SQLite DB lives."""
    global DB_FILE
    DB_FILE = path


def is_postgres():
    """Returns True if we're using PostgreSQL (production)."""
    return bool(DATABASE_URL) and HAS_PG


def _translate_sql(sql):
    """Translate SQLite-style SQL to PostgreSQL-compatible SQL."""
    if not is_postgres():
        return sql

    # Replace ? placeholders with %s
    # Be careful not to replace ? inside strings
    result = []
    in_string = False
    quote_char = None
    for ch in sql:
        if in_string:
            result.append(ch)
            if ch == quote_char:
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            quote_char = ch
            result.append(ch)
        elif ch == '?':
            result.append('%s')
        else:
            result.append(ch)
    sql = ''.join(result)

    # INSERT OR IGNORE INTO → INSERT INTO ... ON CONFLICT DO NOTHING
    sql = re.sub(
        r'INSERT\s+OR\s+IGNORE\s+INTO',
        'INSERT INTO',
        sql,
        flags=re.IGNORECASE,
    )
    if 'ON CONFLICT' not in sql.upper() and 'INSERT INTO' in sql.upper():
        # Only add ON CONFLICT DO NOTHING if it was originally INSERT OR IGNORE
        pass  # We handle this below

    return sql


def _translate_insert_or_ignore(sql):
    """
    Specifically handle INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING.
    Returns the translated SQL.
    """
    if not is_postgres():
        return sql
    if re.search(r'INSERT\s+OR\s+IGNORE', sql, re.IGNORECASE):
        sql = re.sub(r'INSERT\s+OR\s+IGNORE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)
        # Append ON CONFLICT DO NOTHING at the end (before any trailing whitespace/semicolons)
        sql = sql.rstrip().rstrip(';')
        sql += ' ON CONFLICT DO NOTHING'
    return sql


def _translate_insert_or_replace(sql):
    """
    Handle INSERT OR REPLACE → PostgreSQL upsert.
    For PG, we convert to INSERT ... ON CONFLICT DO UPDATE SET ...
    This parses the column list to build the SET clause automatically.
    """
    if not is_postgres():
        return sql
    match = re.match(
        r'INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)',
        sql.strip(),
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return sql

    table = match.group(1)
    cols_str = match.group(2).strip()
    vals_str = match.group(3).strip()
    cols = [c.strip() for c in cols_str.split(',')]

    # Known primary keys for each table
    pk_map = {
        'items': ['item_name', 'show_id'],
        'sku_presets': ['sku'],
        'image_embeddings': ['image_path', 'show_id'],
    }
    pk_cols = pk_map.get(table, [cols[0]])  # fallback to first column
    non_pk_cols = [c for c in cols if c not in pk_cols]

    conflict_target = ', '.join(pk_cols)
    update_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in non_pk_cols)

    return f"INSERT INTO {table} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause}"


def _full_translate(sql):
    """Apply all SQL translations."""
    sql = _translate_insert_or_replace(sql)
    sql = _translate_insert_or_ignore(sql)
    sql = _translate_sql(sql)
    return sql


class TranslatingCursor:
    """Wraps a DB cursor to auto-translate SQLite SQL to PostgreSQL."""
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, params=None):
        translated = _full_translate(sql)
        if params is None:
            return self._cursor.execute(translated)
        return self._cursor.execute(translated, params)

    def executemany(self, sql, param_list):
        return self._cursor.executemany(_full_translate(sql), param_list)

    def __getattr__(self, name):
        # Delegate everything else (fetchone, fetchall, lastrowid, etc.)
        return getattr(self._cursor, name)


@contextmanager
def get_db(timeout=10):
    """
    Context manager for database connections.

    Usage:
        with get_db() as (conn, cursor):
            cursor.execute("SELECT ...")
            rows = cursor.fetchall()
            conn.commit()  # only if you wrote data

    The cursor auto-translates SQLite SQL (? placeholders, INSERT OR IGNORE, etc.)
    to PostgreSQL syntax when DATABASE_URL is set.
    """
    if is_postgres():
        conn = psycopg2.connect(DATABASE_URL)
        try:
            cursor = TranslatingCursor(conn.cursor())
            yield conn, cursor
        finally:
            conn.close()
    else:
        if DB_FILE is None:
            raise RuntimeError("DB_FILE not set — call init_db_path() first")
        conn = sqlite3.connect(DB_FILE, timeout=timeout)
        conn.execute("PRAGMA busy_timeout = 10000")
        try:
            cursor = conn.cursor()
            yield conn, cursor
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Common query helpers — auto-translate SQL for the active database
# ---------------------------------------------------------------------------

def fetch_one(sql, params=()):
    """Run a SELECT and return the first row, or None."""
    with get_db() as (conn, cursor):
        cursor.execute(_full_translate(sql), params)
        return cursor.fetchone()


def fetch_all(sql, params=()):
    """Run a SELECT and return all rows."""
    with get_db() as (conn, cursor):
        cursor.execute(_full_translate(sql), params)
        return cursor.fetchall()


def execute(sql, params=()):
    """Run an INSERT/UPDATE/DELETE and auto-commit."""
    with get_db() as (conn, cursor):
        cursor.execute(_full_translate(sql), params)
        conn.commit()
        return cursor


def execute_many(sql, param_list):
    """Run an INSERT/UPDATE/DELETE for many rows and auto-commit."""
    with get_db() as (conn, cursor):
        cursor.executemany(_full_translate(sql), param_list)
        conn.commit()
        return cursor


def translate(sql):
    """Public access to SQL translation (for use in with get_db() blocks)."""
    return _full_translate(sql)


def execute_returning_id(sql, params=()):
    """
    Run an INSERT and return the new row's ID.
    On PostgreSQL, appends RETURNING id. On SQLite, uses lastrowid.
    """
    with get_db() as (conn, cursor):
        translated = _full_translate(sql)
        if is_postgres():
            # Add RETURNING id if not already there
            if 'RETURNING' not in translated.upper():
                translated = translated.rstrip().rstrip(';') + ' RETURNING id'
            cursor.execute(translated, params)
            row = cursor.fetchone()
            conn.commit()
            return row[0] if row else None
        else:
            cursor.execute(translated, params)
            conn.commit()
            return cursor.lastrowid
