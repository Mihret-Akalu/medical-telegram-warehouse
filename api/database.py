"""
Database connection for FastAPI
"""

import sqlite3
from typing import Generator
import os

DATABASE_URL = "data/medical_warehouse.db"

def get_db() -> Generator:
    """Get database connection"""
    conn = sqlite3.connect(DATABASE_URL)
    conn.row_factory = sqlite3.Row  # Return dictionaries
    try:
        yield conn
    finally:
        conn.close()

def test_connection():
    """Test database connection"""
    try:
        conn = sqlite3.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()
        return {"status": "connected", "tables": [t[0] for t in tables]}
    except Exception as e:
        return {"status": "error", "message": str(e)}