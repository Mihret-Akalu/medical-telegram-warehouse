"""
Verify Dagster pipeline execution
"""

import sqlite3
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

def verify_pipeline():
    """Verify pipeline execution results"""
    print("\n" + "="*60)
    print("PIPELINE VERIFICATION")
    print("="*60)
    
    # 1. Check database
    print("\n📊 DATABASE VERIFICATION:")
    db_path = "data/medical_warehouse.db"
    
    if Path(db_path).exists():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"Tables in database: {len(tables)}")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"  {table[0]}: {count} rows")
        
        conn.close()
    else:
        print("❌ Database not found")
    
    # 2. Check raw data
    print("\n📁 RAW DATA VERIFICATION:")
    raw_files = list(Path("data/raw/telegram_messages").rglob("*.json"))
    print(f"JSON files: {len(raw_files)}")
    
    if raw_files:
        # Show latest file
        latest_file = max(raw_files, key=lambda x: x.stat().st_mtime)
        print(f"Latest file: {latest_file}")
        
        with open(latest_file, 'r') as f:
            data = json.load(f)
            print(f"Messages in latest file: {len(data)}")
    
    # 3. Check processed data
    print("\n⚙️ PROCESSED DATA VERIFICATION:")
    api_files = list(Path("api/data").glob("*.csv"))
    print(f"API data files: {len(api_files)}")
    
    for file in api_files:
        try:
            df = pd.read_csv(file)
            print(f"  {file.name}: {len(df)} rows")
        except:
            print(f"  {file.name}: Error reading")
    
    # 4. Check logs
    print("\n📝 LOGS VERIFICATION:")
    log_files = list(Path("logs").glob("*.log"))
    print(f"Log files: {len(log_files)}")
    
    if log_files:
        latest_log = max(log_files, key=lambda x: x.stat().st_mtime)
        print(f"Latest log: {latest_log}")
        
        # Show last few lines
        try:
            with open(latest_log, 'r') as f:
                lines = f.readlines()[-10:]
                print("Last 10 lines:")
                for line in lines:
                    print(f"  {line.strip()}")
        except:
            print("  Could not read log file")
    
    print("\n" + "="*60)
    print("VERIFICATION COMPLETE")
    print("="*60)

if __name__ == "__main__":
    verify_pipeline()