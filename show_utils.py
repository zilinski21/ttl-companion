"""
Show directory and CSV utilities.

Centralizes the repeated patterns for:
- Sanitizing show names for filesystem use
- Resolving show directories from DB
- Reading/writing CSV files for a show
- Updating individual CSV columns
"""

import csv
import re
from pathlib import Path

from db import get_db

# Will be set by dashboard.py at startup
CAPTURES_DIR = None
CSV_FILE = None  # Legacy fallback CSV


def init_paths(captures_dir, csv_file):
    """Call once at app startup."""
    global CAPTURES_DIR, CSV_FILE
    CAPTURES_DIR = captures_dir
    CSV_FILE = csv_file


# ---------------------------------------------------------------------------
# Name sanitization (was duplicated 19 times)
# ---------------------------------------------------------------------------

_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')


def safe_filename(name):
    """Strip characters that are unsafe in file/folder names."""
    return _UNSAFE_CHARS.sub("", name).strip()


def show_dir_path(show_name, show_date):
    """Build the captures/{safe_name}_{safe_date} path."""
    return CAPTURES_DIR / f"{safe_filename(show_name)}_{safe_filename(show_date)}"


# ---------------------------------------------------------------------------
# Show info from DB
# ---------------------------------------------------------------------------

def get_show_info(show_id, org_id=None):
    """
    Look up a show by ID and return (show_name, show_date, show_dir).
    Returns (None, None, None) if not found.
    """
    with get_db() as (conn, cursor):
        if org_id:
            cursor.execute(
                "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
                (show_id, org_id),
            )
        else:
            cursor.execute("SELECT name, date FROM shows WHERE id = ?", (show_id,))
        result = cursor.fetchone()

    if not result:
        return None, None, None
    show_name, show_date = result
    return show_name, show_date, show_dir_path(show_name, show_date)


def get_show_csv(show_id, org_id=None):
    """
    Get the CSV file path for a show.
    Falls back to the global CSV_FILE if the show-specific one doesn't exist.
    Returns (csv_path, show_name, show_date, show_dir) or (None, ...) if nothing found.
    """
    show_name, show_date, show_dir = get_show_info(show_id, org_id)
    if show_dir:
        csv_file = show_dir / "log.csv"
        if csv_file.exists():
            return csv_file, show_name, show_date, show_dir

    # Fallback to global CSV
    if CSV_FILE and CSV_FILE.exists():
        return CSV_FILE, show_name, show_date, show_dir

    return None, show_name, show_date, show_dir


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------

def read_csv_rows(csv_path):
    """
    Read a CSV file and return (header, data_rows) as raw lists.
    Returns ([], []) if file doesn't exist.
    """
    if not csv_path or not csv_path.exists():
        return [], []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def read_csv_dicts(csv_path):
    """Read a CSV file and return a list of dicts (one per row)."""
    if not csv_path or not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv_rows(csv_path, header, data_rows):
    """Write header + data rows back to a CSV file."""
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data_rows)


# ---------------------------------------------------------------------------
# CSV column update (was duplicated ~5 times for different columns)
# ---------------------------------------------------------------------------

def update_csv_column(show_id, org_id, item_name, column_name, value):
    """
    Update a single column value for an item in its show's CSV.

    Finds the most recent row matching item_name (by item_title, column index 1),
    ensures the column exists in the header, updates the value, and writes back.

    Returns (True, None) on success, or (False, error_message) on failure.
    """
    csv_file, _, _, _ = get_show_csv(show_id, org_id)
    if not csv_file or not csv_file.exists():
        return False, "CSV file not found"

    # Read all rows (header + data)
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return False, "CSV file is empty"

    header = rows[0]

    # Ensure column exists in header
    if column_name not in header:
        header.append(column_name)
        rows[0] = header

    col_idx = header.index(column_name)
    num_columns = len(header)

    # Pad all rows to match header length
    for i in range(1, len(rows)):
        while len(rows[i]) < num_columns:
            rows[i].append("")

    # Find the most recent row for this item (reverse scan)
    updated = False
    for i in range(len(rows) - 1, 0, -1):
        if len(rows[i]) > 1 and rows[i][1] == item_name:
            rows[i][col_idx] = value
            updated = True
            break

    if not updated:
        return False, "Item not found in CSV"

    # Write back
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    return True, None
