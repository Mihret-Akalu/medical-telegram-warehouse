"""
Load data to PostgreSQL and run dbt for Task 2
"""

import os
import json
import glob
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import subprocess
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'medical_warehouse'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

def create_raw_table(conn):
    """Create raw telegram_messages table"""
    cursor = conn.cursor()
    
    # Create schema if not exists
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    # Create table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        id SERIAL PRIMARY KEY,
        message_id BIGINT NOT NULL,
        channel_name VARCHAR(255) NOT NULL,
        channel_username VARCHAR(255),
        channel_title VARCHAR(255),
        message_date TIMESTAMP,
        message_text TEXT,
        has_media BOOLEAN DEFAULT FALSE,
        image_path VARCHAR(500),
        views INTEGER DEFAULT 0,
        forwards INTEGER DEFAULT 0,
        scraped_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(message_id, channel_name)
    )
    ''')
    
    conn.commit()
    logger.info("✅ Created raw.telegram_messages table")
    cursor.close()

def load_json_to_postgres(conn):
    """Load JSON files to PostgreSQL"""
    cursor = conn.cursor()
    
    # Find JSON files
    json_files = glob.glob('data/raw/telegram_messages/*/*.json')
    
    if not json_files:
        logger.warning("No JSON files found. Run scraper first.")
        return 0
    
    total_messages = 0
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            data_to_insert = []
            for msg in messages:
                data_to_insert.append((
                    msg.get('message_id'),
                    msg.get('channel_name', ''),
                    msg.get('channel_username'),
                    msg.get('channel_title'),
                    msg.get('message_date'),
                    msg.get('message_text', ''),
                    msg.get('has_media', False),
                    msg.get('image_path'),
                    msg.get('views', 0),
                    msg.get('forwards', 0),
                    msg.get('scraped_at', datetime.now().isoformat())
                ))
            
            # Insert in batch
            insert_sql = '''
            INSERT INTO raw.telegram_messages 
            (message_id, channel_name, channel_username, channel_title,
             message_date, message_text, has_media, image_path,
             views, forwards, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id, channel_name) DO NOTHING
            '''
            
            execute_batch(cursor, insert_sql, data_to_insert)
            conn.commit()
            
            total_messages += len(messages)
            logger.info(f"Loaded {len(messages)} messages from {json_file}")
            
        except Exception as e:
            logger.error(f"Error loading {json_file}: {e}")
            conn.rollback()
    
    cursor.close()
    return total_messages

def run_dbt_commands():
    """Run dbt commands"""
    commands = [
        "dbt debug",
        "dbt run",
        "dbt test",
        "dbt docs generate"
    ]
    
    for cmd in commands:
        logger.info(f"Running: {cmd}")
        try:
            result = subprocess.run(
                cmd.split(),
                cwd="medical_warehouse",
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"✅ {cmd} completed successfully")
            if result.stdout:
                logger.info(f"Output: {result.stdout[:500]}...")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ {cmd} failed: {e}")
            if e.stderr:
                logger.error(f"Error: {e.stderr}")
            return False
    
    return True

def main():
    """Main function"""
    print("\n" + "="*60)
    print("TASK 2 - Data Modeling and Transformation")
    print("="*60)
    
    # Step 1: Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        logger.info("Make sure PostgreSQL is running. You can start it with:")
        logger.info("docker-compose up -d postgres")
        return
    
    try:
        # Step 2: Create table
        create_raw_table(conn)
        
        # Step 3: Load data
        print("\nLoading data from JSON files to PostgreSQL...")
        total_messages = load_json_to_postgres(conn)
        
        if total_messages == 0:
            print("⚠️ No data loaded. Make sure you've run the scraper first.")
            print("Run: python src/scraper.py")
            return
        
        print(f"✅ Loaded {total_messages} messages to PostgreSQL")
        
        # Step 4: Run dbt
        print("\nRunning dbt transformations...")
        success = run_dbt_commands()
        
        if success:
            print("\n" + "="*60)
            print("🎉 TASK 2 COMPLETED SUCCESSFULLY!")
            print("="*60)
            print("\nDeliverables created:")
            print("✓ Star schema with dim_dates, dim_channels, fct_messages")
            print("✓ Staging model cleaned and standardized data")
            print("✓ Data quality tests implemented")
            print("✓ Documentation generated")
            print("\nNext steps:")
            print("1. View documentation: dbt docs serve")
            print("2. Check test results in medical_warehouse/target/")
            print("3. Proceed to Task 3 - Data Enrichment with YOLO")
        else:
            print("\n⚠️ dbt transformations had issues")
            print("Check the logs above for details")
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()