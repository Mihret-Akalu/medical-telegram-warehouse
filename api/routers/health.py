"""
Health check endpoints
"""

from fastapi import APIRouter, Depends
import sqlite3
from datetime import datetime
from api.schemas import HealthCheck
from api.database import get_db

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/", response_model=HealthCheck)
async def health_check(db: sqlite3.Connection = Depends(get_db)):
    """Health check endpoint"""
    cursor = db.cursor()
    
    # Check database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    return HealthCheck(
        status="healthy",
        database="connected",
        tables_count=len(tables),
        timestamp=datetime.now()
    )

@router.get("/tables")
async def list_tables(db: sqlite3.Connection = Depends(get_db)):
    """List all tables in database"""
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    table_info = []
    for table in tables:
        table_name = table['name']
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        count = cursor.fetchone()['count']
        table_info.append({
            "table": table_name,
            "row_count": count
        })
    
    return {"tables": table_info}