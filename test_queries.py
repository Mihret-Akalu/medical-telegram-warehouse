import sqlite3
import pandas as pd

def run_queries():
    conn = sqlite3.connect("data/medical_warehouse.db")
    
    print("Testing Task 2 Results:")
    print("="*60)
    
    # 1. Check tables
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    print("\nTables created:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cursor.fetchone()[0]
        print(f"  {table[0]}: {count} rows")
    
    # 2. Daily Posting Trends
    print("\n📅 Daily Posting Trends (last 5 days):")
    query1 = """
    SELECT 
        d.full_date,
        d.day_name,
        COUNT(f.message_id) as posts,
        SUM(f.view_count) as total_views,
        AVG(f.view_count) as avg_views
    FROM fct_messages f
    JOIN dim_dates d ON f.date_key = d.date_key
    GROUP BY d.full_date, d.day_name
    ORDER BY d.full_date DESC
    LIMIT 5
    """
    df1 = pd.read_sql_query(query1, conn)
    print(df1.to_string(index=False))
    
    # 3. Channel Performance by Type
    print("\n📊 Channel Performance by Type:")
    query2 = """
    SELECT 
        channel_type,
        COUNT(*) as channels,
        SUM(total_posts) as total_posts,
        ROUND(AVG(avg_views), 2) as avg_views
    FROM dim_channels
    GROUP BY channel_type
    ORDER BY total_posts DESC
    """
    df2 = pd.read_sql_query(query2, conn)
    print(df2.to_string(index=False))
    
    # 4. Images vs Text
    print("\n🖼️ Messages with Images vs Without:")
    query3 = """
    SELECT 
        CASE WHEN has_image = 1 THEN 'With Images' ELSE 'Text Only' END as type,
        COUNT(*) as messages,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fct_messages), 2) as percentage,
        AVG(view_count) as avg_views
    FROM fct_messages
    GROUP BY has_image
    ORDER BY messages DESC
    """
    df3 = pd.read_sql_query(query3, conn)
    print(df3.to_string(index=False))
    
    conn.close()
    print("\n✅ All queries executed successfully!")

if __name__ == "__main__":
    run_queries()