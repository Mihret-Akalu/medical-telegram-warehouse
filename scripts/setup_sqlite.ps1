# Quick SQLite setup for Task 2

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "QUICK SQLITE SETUP FOR TASK 2" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Check if we're in the right directory
if (-not (Test-Path "src/scraper.py")) {
    Write-Host "❌ Please run this script from the project root directory" -ForegroundColor Red
    Write-Host "   Expected to find: src/scraper.py" -ForegroundColor Yellow
    exit 1
}

# Install required packages
Write-Host "Installing required packages..." -ForegroundColor Yellow
pip install dbt-sqlite pandas

# Create directories
Write-Host "Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "data"
New-Item -ItemType Directory -Force -Path "logs"
New-Item -ItemType Directory -Force -Path "medical_warehouse/models"
New-Item -ItemType Directory -Force -Path "medical_warehouse/models/staging"
New-Item -ItemType Directory -Force -Path "medical_warehouse/models/marts"

# Create SQLite database
$dbPath = "data/warehouse.db"
Write-Host "Creating SQLite database: $dbPath" -ForegroundColor Yellow

# Python script to setup database
$pythonScript = @"
import sqlite3
import os
import json
import glob
from datetime import datetime
import sys

print("Setting up SQLite database...")

# Create database
db_path = 'data/warehouse.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create raw table
print("Creating raw_telegram_messages table...")
cursor.execute('''
CREATE TABLE IF NOT EXISTS raw_telegram_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    channel_name TEXT NOT NULL,
    channel_username TEXT,
    channel_title TEXT,
    message_date TIMESTAMP,
    message_text TEXT,
    message_length INTEGER,
    has_media BOOLEAN DEFAULT FALSE,
    image_path TEXT,
    views INTEGER DEFAULT 0,
    forwards INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    hashtags TEXT,
    potential_products TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(message_id, channel_name)
)
''')

# Load data from JSON files
print("Loading data from JSON files...")
json_files = glob.glob('data/raw/telegram_messages/*/*.json')

if not json_files:
    print("Warning: No JSON files found in data/raw/telegram_messages/")
    print("Make sure you've run the scraper first: python src/scraper.py")
else:
    total_messages = 0
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            for msg in messages:
                # Extract hashtags and potential products
                hashtags = msg.get('hashtags', [])
                if isinstance(hashtags, list):
                    hashtags_str = ','.join(hashtags)
                else:
                    hashtags_str = str(hashtags)
                
                potential_products = msg.get('potential_products', [])
                if isinstance(potential_products, list):
                    products_str = ','.join(potential_products)
                else:
                    products_str = str(potential_products)
                
                cursor.execute('''
                    INSERT OR IGNORE INTO raw_telegram_messages 
                    (message_id, channel_name, channel_username, channel_title,
                     message_date, message_text, message_length, has_media, image_path,
                     views, forwards, replies, hashtags, potential_products, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    msg.get('message_id'),
                    msg.get('channel_name', ''),
                    msg.get('channel_username'),
                    msg.get('channel_title'),
                    msg.get('message_date'),
                    msg.get('message_text', ''),
                    len(msg.get('message_text', '')),
                    msg.get('has_media', False),
                    msg.get('image_path'),
                    msg.get('views', 0),
                    msg.get('forwards', 0),
                    msg.get('replies', 0),
                    hashtags_str,
                    products_str,
                    msg.get('scraped_at', datetime.now().isoformat())
                ))
            
            total_messages += len(messages)
            print(f"  Loaded {len(messages)} messages from {json_file}")
            
        except Exception as e:
            print(f"  Error loading {json_file}: {e}")

    conn.commit()

    # Get count
    cursor.execute("SELECT COUNT(*) FROM raw_telegram_messages")
    count = cursor.fetchone()[0]

    conn.close()

    print(f"\n✅ Database setup complete!")
    print(f"   Database: {db_path}")
    print(f"   Total messages loaded: {count}")
"@

# Run the Python script
Write-Host "Running database setup..." -ForegroundColor Yellow
$pythonScript | python

# Update .env for SQLite
Write-Host "`nUpdating environment configuration..." -ForegroundColor Yellow
$envContent = @"
# Using SQLite for development
DATABASE_TYPE=sqlite
DATABASE_PATH=data/warehouse.db

# Telegram API Credentials
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+251XXXXXXXXX

# Application Settings
ENVIRONMENT=development
LOG_LEVEL=INFO
"@

$envContent | Out-File -FilePath ".env" -Encoding UTF8
Write-Host "Updated .env file for SQLite" -ForegroundColor Green

# Update dbt profiles for SQLite
Write-Host "Updating dbt profiles..." -ForegroundColor Yellow
$profilesDir = "medical_warehouse"
New-Item -ItemType Directory -Force -Path $profilesDir

$profilesContent = @"
medical_warehouse:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: "../data/warehouse.db"
      schema: main
      threads: 1
"@

$profilesContent | Out-File -FilePath "$profilesDir/profiles.yml" -Encoding UTF8
Write-Host "Updated dbt profiles.yml" -ForegroundColor Green

Write-Host "`n✅ SQLite setup complete!" -ForegroundColor Green

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "NEXT STEPS FOR TASK 2:" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "1. Install dbt-sqlite if not already installed:" -ForegroundColor Yellow
Write-Host "   pip install dbt-sqlite" -ForegroundColor White
Write-Host "2. Run the dbt pipeline:" -ForegroundColor Yellow
Write-Host "   python src/run_dbt_pipeline.py" -ForegroundColor White
Write-Host "3. Or run dbt commands manually:" -ForegroundColor Yellow
Write-Host "   cd medical_warehouse" -ForegroundColor White
Write-Host "   dbt debug" -ForegroundColor White
Write-Host "   dbt run" -ForegroundColor White
Write-Host "   dbt test" -ForegroundColor White
Write-Host "`nNote: This is a temporary setup for Task 2." -ForegroundColor Yellow
Write-Host "For production, install Docker and use PostgreSQL." -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Cyan
