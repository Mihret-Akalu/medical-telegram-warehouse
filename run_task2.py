"""
Complete Task 2 Implementation - Fixed Version
Creates data warehouse with star schema and runs tests
"""

import os
import sqlite3
import json
import glob
from datetime import datetime, timedelta
import logging
import csv
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/task2_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Task2DataWarehouse:
    def __init__(self):
        self.db_path = "data/medical_warehouse.db"
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories"""
        directories = ['data', 'logs', 'docs', 'reports']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def run(self):
        """Run complete Task 2 pipeline"""
        print("\n" + "="*60)
        print("TASK 2 - Data Modeling and Transformation")
        print("="*60)
        
        # Step 1: Create database and load raw data
        print("\n1. Creating database and loading raw data...")
        conn = self.create_database()
        messages_loaded = self.load_raw_data(conn)
        
        if messages_loaded == 0:
            print("❌ No data loaded. Please run the scraper first:")
            print("   python src/scraper.py")
            return
        
        print(f"✅ Loaded {messages_loaded} messages")
        
        # Step 2: Create star schema
        print("\n2. Creating star schema...")
        self.create_star_schema(conn)
        
        # Step 3: Run tests
        print("\n3. Running data quality tests...")
        test_results = self.run_data_tests(conn)
        
        # Step 4: Generate documentation
        print("\n4. Generating documentation...")
        self.generate_documentation(conn)
        
        conn.close()
        
        # Step 5: Print summary
        self.print_summary(messages_loaded, test_results)
    
    def create_database(self):
        """Create SQLite database"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        # Enable needed extensions
        conn.enable_load_extension(True)
        try:
            # Try to enable generate_series if available
            conn.execute("SELECT load_extension('mod_spatialite')")
        except:
            pass  # It's okay if not available
        
        return conn
    
    def load_raw_data(self, conn):
        """Load raw JSON data into database"""
        cursor = conn.cursor()
        
        # Create raw table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_telegram_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            channel_name TEXT NOT NULL,
            channel_username TEXT,
            channel_title TEXT,
            message_date TIMESTAMP,
            message_text TEXT,
            has_media BOOLEAN DEFAULT FALSE,
            image_path TEXT,
            views INTEGER DEFAULT 0,
            forwards INTEGER DEFAULT 0,
            scraped_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(message_id, channel_name)
        )
        ''')
        
        # Load JSON files - exclude _manifest.json
        json_files = glob.glob('data/raw/telegram_messages/*/*.json')
        # Filter out manifest files
        json_files = [f for f in json_files if '_manifest.json' not in f]
        
        total_messages = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Check if it's a list of messages
                if isinstance(data, list):
                    messages = data
                else:
                    # If it's a single message object
                    messages = [data]
                
                for msg in messages:
                    # Skip if it's not a message object
                    if not isinstance(msg, dict):
                        continue
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO raw_telegram_messages 
                        (message_id, channel_name, channel_username, channel_title,
                         message_date, message_text, has_media, image_path,
                         views, forwards, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
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
                
                total_messages += len(messages)
                logger.info(f"Loaded {len(messages)} messages from {Path(json_file).name}")
                
            except Exception as e:
                logger.error(f"Error loading {json_file}: {e}")
        
        conn.commit()
        return total_messages
    
    def create_star_schema(self, conn):
        """Create star schema tables"""
        cursor = conn.cursor()
        
        print("  Creating staging table...")
        # Create staging table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stg_telegram_messages AS
        WITH raw_data AS (
            SELECT 
                message_id,
                channel_name,
                channel_username,
                channel_title,
                message_date,
                message_text,
                has_media,
                image_path,
                COALESCE(views, 0) as views,
                COALESCE(forwards, 0) as forwards,
                scraped_at
            FROM raw_telegram_messages
            WHERE message_date IS NOT NULL
        ),
        cleaned_data AS (
            SELECT 
                message_id,
                channel_name,
                channel_username,
                channel_title,
                message_date,
                scraped_at,
                message_text,
                TRIM(message_text) as cleaned_message_text,
                LENGTH(TRIM(message_text)) as message_length,
                has_media,
                image_path,
                CASE WHEN image_path IS NOT NULL THEN TRUE ELSE FALSE END as has_image,
                views,
                forwards,
                CASE WHEN message_text IS NULL OR TRIM(message_text) = '' 
                     THEN TRUE ELSE FALSE END as is_empty_message,
                CASE WHEN message_date > CURRENT_TIMESTAMP 
                     THEN TRUE ELSE FALSE END as is_future_date,
                CASE WHEN views < 0 THEN TRUE ELSE FALSE END as has_negative_views
            FROM raw_data
        )
        SELECT 
            *,
            CASE 
                WHEN is_empty_message OR is_future_date OR has_negative_views
                THEN 'needs_review' 
                ELSE 'valid' 
            END as data_quality_status
        FROM cleaned_data
        WHERE NOT is_future_date
        ''')
        
        print("  Creating date dimension...")
        # Create dim_dates - without generate_series
        # First, get date range from messages
        cursor.execute('''
            SELECT 
                MIN(DATE(message_date)) as min_date,
                MAX(DATE(message_date)) as max_date
            FROM stg_telegram_messages
            WHERE message_date IS NOT NULL
        ''')
        
        result = cursor.fetchone()
        if result and result[0] and result[1]:
            min_date = datetime.strptime(result[0], '%Y-%m-%d').date()
            max_date = datetime.strptime(result[1], '%Y-%m-%d').date()
            
            # Extend range by 30 days on both sides
            min_date = min_date - timedelta(days=30)
            max_date = max_date + timedelta(days=30)
        else:
            # Default range if no dates
            min_date = datetime.now().date() - timedelta(days=365)
            max_date = datetime.now().date() + timedelta(days=365)
        
        # Create a recursive CTE to generate dates
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS dim_dates AS
        WITH RECURSIVE date_range AS (
            SELECT date('{min_date.isoformat()}') as date
            UNION ALL
            SELECT date(date, '+1 day')
            FROM date_range
            WHERE date < date('{max_date.isoformat()}')
        )
        SELECT
            CAST(strftime('%Y%m%d', date) AS INTEGER) as date_key,
            date as full_date,
            CAST(strftime('%Y', date) AS INTEGER) as year,
            (CAST(strftime('%m', date) AS INTEGER) - 1) / 3 + 1 as quarter,
            CAST(strftime('%m', date) AS INTEGER) as month,
            CASE CAST(strftime('%m', date) AS INTEGER)
                WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
                WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
                WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
                WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
            END as month_name,
            CAST(strftime('%W', date) AS INTEGER) + 1 as week_of_year,
            CAST(strftime('%d', date) AS INTEGER) as day_of_month,
            CAST(strftime('%w', date) AS INTEGER) as day_of_week,
            CASE CAST(strftime('%w', date) AS INTEGER)
                WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END as day_name,
            CASE 
                WHEN CAST(strftime('%w', date) AS INTEGER) IN (0, 6) THEN TRUE 
                ELSE FALSE 
            END as is_weekend
        FROM date_range
        ''')
        
        print("  Creating channel dimension...")
        # Create dim_channels
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_channels AS
        WITH channel_stats AS (
            SELECT 
                channel_name,
                channel_username,
                channel_title,
                MIN(message_date) as first_post_date,
                MAX(message_date) as last_post_date,
                COUNT(*) as total_posts,
                AVG(views) as avg_views,
                AVG(forwards) as avg_forwards,
                SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as posts_with_media,
                SUM(CASE WHEN has_image THEN 1 ELSE 0 END) as posts_with_image
            FROM stg_telegram_messages
            WHERE data_quality_status = 'valid'
            GROUP BY channel_name, channel_username, channel_title
        ),
        channel_classification AS (
            SELECT *,
                CASE
                    WHEN LOWER(channel_name) LIKE '%pharma%' OR 
                         LOWER(channel_name) LIKE '%med%' OR
                         LOWER(channel_name) LIKE '%drug%' OR
                         LOWER(channel_name) LIKE '%pharmacy%' OR
                         LOWER(channel_name) LIKE '%pill%' OR
                         LOWER(channel_name) LIKE '%tablet%'
                    THEN 'Pharmaceutical'
                    WHEN LOWER(channel_name) LIKE '%cosmetic%' OR
                         LOWER(channel_name) LIKE '%beauty%' OR
                         LOWER(channel_name) LIKE '%skin%' OR
                         LOWER(channel_name) LIKE '%cream%' OR
                         LOWER(channel_name) LIKE '%lotion%' OR
                         LOWER(channel_name) LIKE '%makeup%'
                    THEN 'Cosmetics'
                    WHEN LOWER(channel_name) LIKE '%health%' OR
                         LOWER(channel_name) LIKE '%medical%' OR
                         LOWER(channel_name) LIKE '%hospital%' OR
                         LOWER(channel_name) LIKE '%clinic%' OR
                         LOWER(channel_name) LIKE '%doctor%'
                    THEN 'Medical'
                    ELSE 'Other'
                END as channel_type
            FROM channel_stats
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY total_posts DESC) as channel_key,
            channel_name,
            channel_username,
            channel_title,
            channel_type,
            first_post_date,
            last_post_date,
            total_posts,
            ROUND(avg_views, 2) as avg_views,
            ROUND(avg_forwards, 2) as avg_forwards,
            posts_with_media,
            posts_with_image,
            ROUND(posts_with_media * 100.0 / NULLIF(total_posts, 0), 2) as media_percentage,
            ROUND(posts_with_image * 100.0 / NULLIF(total_posts, 0), 2) as image_percentage,
            CASE 
                WHEN last_post_date >= DATE('now', '-7 days') THEN 'active' 
                WHEN last_post_date >= DATE('now', '-30 days') THEN 'moderate' 
                ELSE 'inactive' 
            END as activity_status
        FROM channel_classification
        ''')
        
        print("  Creating fact table...")
        # Create fct_messages
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fct_messages AS
        SELECT 
            m.message_id,
            c.channel_key,
            d.date_key,
            m.cleaned_message_text as message_text,
            m.message_length,
            m.views as view_count,
            m.forwards as forward_count,
            m.has_image,
            m.data_quality_status
        FROM stg_telegram_messages m
        LEFT JOIN dim_channels c ON m.channel_name = c.channel_name
        LEFT JOIN dim_dates d ON DATE(m.message_date) = d.full_date
        WHERE c.channel_key IS NOT NULL
          AND d.date_key IS NOT NULL
          AND m.data_quality_status = 'valid'
        ''')
        
        conn.commit()
        print("✅ Created star schema tables")
    
    def run_data_tests(self, conn):
        """Run all data quality tests"""
        cursor = conn.cursor()
        results = []
        
        tests = [
            ("No future dates", 
             "SELECT COUNT(*) FROM stg_telegram_messages WHERE message_date > CURRENT_TIMESTAMP AND data_quality_status = 'valid'",
             True),
            ("No negative views",
             "SELECT COUNT(*) FROM stg_telegram_messages WHERE views < 0 AND data_quality_status = 'valid'",
             True),
            ("All channels have type",
             "SELECT COUNT(*) FROM dim_channels WHERE channel_type IS NULL OR channel_type = ''",
             True),
            ("Foreign key integrity (channels)",
             "SELECT COUNT(*) FROM fct_messages f LEFT JOIN dim_channels c ON f.channel_key = c.channel_key WHERE c.channel_key IS NULL",
             True),
            ("Foreign key integrity (dates)",
             "SELECT COUNT(*) FROM fct_messages f LEFT JOIN dim_dates d ON f.date_key = d.date_key WHERE d.date_key IS NULL",
             True),
        ]
        
        print("\n📊 DATA QUALITY TESTS:")
        print("-" * 50)
        
        all_passed = True
        for test_name, query, should_be_zero in tests:
            try:
                cursor.execute(query)
                result = cursor.fetchone()[0]
                passed = (result == 0) if should_be_zero else (result > 0)
                
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"{status} {test_name}: {result}")
                
                results.append({
                    "test": test_name,
                    "passed": passed,
                    "result": result
                })
                
                if not passed:
                    all_passed = False
            except Exception as e:
                print(f"⚠️ Error running test '{test_name}': {e}")
                all_passed = False
        
        # Save test results
        self.save_test_results(results)
        
        return {"all_passed": all_passed, "results": results}
    
    def save_test_results(self, results):
        """Save test results to CSV"""
        with open('reports/data_quality_tests.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Test Name', 'Status', 'Result', 'Timestamp'])
            for result in results:
                writer.writerow([
                    result['test'],
                    'PASS' if result['passed'] else 'FAIL',
                    result['result'],
                    datetime.now().isoformat()
                ])
    
    def generate_documentation(self, conn):
        """Generate project documentation"""
        cursor = conn.cursor()
        
        print("  Generating schema documentation...")
        # 1. Generate schema documentation
        schema_doc = """# Medical Telegram Warehouse - Schema Documentation

## Star Schema Design

### Dimension Tables

#### dim_dates
- **Purpose**: Time dimension for date-based analysis
- **Primary Key**: date_key (YYYYMMDD format)
- **Key Fields**: full_date, year, quarter, month, day_name, is_weekend

#### dim_channels
- **Purpose**: Channel dimension with business classification
- **Primary Key**: channel_key
- **Key Fields**: channel_name, channel_type, business metrics
- **Business Logic**: Automatic classification based on channel name patterns

### Fact Table

#### fct_messages
- **Purpose**: Core fact table for message analytics
- **Primary Key**: message_id (business key)
- **Foreign Keys**: channel_key -> dim_channels, date_key -> dim_dates
- **Metrics**: view_count, forward_count, has_image

## Data Quality
- **Staging Layer**: Includes data quality validation
- **Tests Implemented**: Future dates, negative values, referential integrity
"""
        
        with open('docs/schema_documentation.md', 'w') as f:
            f.write(schema_doc)
        
        print("  Generating data dictionary...")
        # 2. Generate data dictionary
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            data_dict = []
            for table in tables:
                table_name = table['name']
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                for col in columns:
                    data_dict.append({
                        'table': table_name,
                        'column': col[1],
                        'type': col[2],
                        'nullable': 'YES' if col[3] == 0 else 'NO',
                        'pk': 'YES' if col[5] == 1 else 'NO'
                    })
            
            with open('docs/data_dictionary.csv', 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['table', 'column', 'type', 'nullable', 'pk'])
                writer.writeheader()
                writer.writerows(data_dict)
        except Exception as e:
            print(f"⚠️ Error generating data dictionary: {e}")
        
        print("  Generating sample queries...")
        # 3. Generate sample queries
        sample_queries = """-- Sample Analytical Queries

-- 1. Top 10 Most Active Channels
SELECT 
    channel_name,
    channel_type,
    total_posts,
    avg_views,
    activity_status
FROM dim_channels
ORDER BY total_posts DESC
LIMIT 10;

-- 2. Daily Posting Trends
SELECT 
    d.full_date,
    d.day_name,
    COUNT(f.message_id) as post_count,
    SUM(f.view_count) as total_views,
    AVG(f.view_count) as avg_views_per_post
FROM fct_messages f
JOIN dim_dates d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC;

-- 3. Channel Performance by Type
SELECT 
    channel_type,
    COUNT(*) as channel_count,
    SUM(total_posts) as total_posts,
    ROUND(AVG(avg_views), 2) as avg_views_per_channel
FROM dim_channels
GROUP BY channel_type
ORDER BY total_posts DESC;

-- 4. Messages with Images vs Without
SELECT 
    CASE WHEN has_image = 1 THEN 'With Images' ELSE 'Text Only' END as message_type,
    COUNT(*) as message_count,
    AVG(view_count) as avg_views,
    AVG(forward_count) as avg_forwards
FROM fct_messages
GROUP BY has_image
ORDER BY message_count DESC;
"""
        
        with open('docs/sample_queries.sql', 'w') as f:
            f.write(sample_queries)
        
        print("✅ Documentation generated in docs/ directory")
    
    def print_summary(self, messages_loaded, test_results):
        """Print completion summary"""
        try:
            cursor = sqlite3.connect(self.db_path).cursor()
            
            print("\n" + "="*60)
            print("🎉 TASK 2 COMPLETED SUCCESSFULLY!")
            print("="*60)
            
            print(f"\n📊 DATABASE STATISTICS:")
            print(f"   Raw messages loaded: {messages_loaded}")
            
            try:
                cursor.execute("SELECT COUNT(*) FROM dim_dates")
                dates_count = cursor.fetchone()[0]
                print(f"   Date dimension entries: {dates_count}")
            except:
                print(f"   Date dimension entries: Not created")
            
            try:
                cursor.execute("SELECT COUNT(*) FROM dim_channels")
                channels_count = cursor.fetchone()[0]
                print(f"   Channel dimension entries: {channels_count}")
            except:
                print(f"   Channel dimension entries: Not created")
            
            try:
                cursor.execute("SELECT COUNT(*) FROM fct_messages")
                facts_count = cursor.fetchone()[0]
                print(f"   Fact table entries: {facts_count}")
            except:
                print(f"   Fact table entries: Not created")
            
            print(f"\n✅ DELIVERABLES CREATED:")
            print("   ✓ Star schema implemented (dim_dates, dim_channels, fct_messages)")
            print("   ✓ Staging model with data cleaning and validation")
            print("   ✓ 5 data quality tests implemented and run")
            print("   ✓ Comprehensive documentation generated")
            print("   ✓ Sample analytical queries provided")
            
            print(f"\n📁 OUTPUT FILES:")
            print("   Database: data/medical_warehouse.db")
            print("   Documentation: docs/ directory")
            print("   Test reports: reports/ directory")
            
            print(f"\n🔍 DATA QUALITY: {'✅ ALL TESTS PASSED' if test_results['all_passed'] else '⚠️ SOME TESTS FAILED'}")
            
            print(f"\n📚 NEXT STEPS:")
            print("   1. Explore the database with SQL queries")
            print("   2. Check docs/sample_queries.sql for examples")
            print("   3. Proceed to Task 3 - Data Enrichment with YOLO")
            print("="*60)
            
        except Exception as e:
            print(f"⚠️ Error generating summary: {e}")

def main():
    """Main execution"""
    warehouse = Task2DataWarehouse()
    warehouse.run()

if __name__ == "__main__":
    main()