"""
Test YOLO integration and analyze results
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime

def test_yolo_integration():
    """Test YOLO results integration"""
    db_path = "data/medical_warehouse.db"
    
    try:
        conn = sqlite3.connect(db_path)
        
        print("Testing YOLO Integration...")
        print("="*60)
        
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='yolo_detections'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("❌ yolo_detections table not found")
            return
        
        # Get counts
        cursor.execute("SELECT COUNT(*) FROM yolo_detections")
        total_detections = cursor.fetchone()[0]
        
        print(f"\n📊 YOLO DETECTIONS: {total_detections}")
        
        if total_detections == 0:
            print("⚠️ No YOLO detections found. Run Task 3 first.")
            return
        
        # Get sample data
        print("\n📁 Sample Detections:")
        query1 = """
        SELECT 
            channel_name,
            image_category,
            detection_count,
            detection_time
        FROM yolo_detections
        ORDER BY detection_time DESC
        LIMIT 5
        """
        
        df1 = pd.read_sql_query(query1, conn)
        print(df1.to_string(index=False))
        
        # Category analysis
        print("\n🎯 Image Categories:")
        query2 = """
        SELECT 
            image_category,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM yolo_detections), 2) as percentage,
            AVG(detection_count) as avg_detections
        FROM yolo_detections
        GROUP BY image_category
        ORDER BY count DESC
        """
        
        df2 = pd.read_sql_query(query2, conn)
        print(df2.to_string(index=False))
        
        # Combined analysis with messages
        print("\n📈 YOLO + Message Analysis:")
        query3 = """
        SELECT 
            y.image_category,
            COUNT(*) as image_count,
            AVG(f.view_count) as avg_views,
            AVG(f.forward_count) as avg_forwards
        FROM yolo_detections y
        LEFT JOIN fct_messages f ON y.message_id = f.message_id
        WHERE f.message_id IS NOT NULL
        GROUP BY y.image_category
        ORDER BY avg_views DESC
        """
        
        df3 = pd.read_sql_query(query3, conn)
        if not df3.empty:
            print(df3.to_string(index=False))
        else:
            print("No message correlations found")
        
        # Top detected objects
        print("\n🔍 Top Detected Objects:")
        query4 = """
        WITH object_counts AS (
            SELECT 
                json_extract(value, '$') as object_name
            FROM yolo_detections y,
            json_each(y.detected_objects)
            WHERE y.detected_objects IS NOT NULL
        )
        SELECT 
            object_name,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM object_counts), 2) as percentage
        FROM object_counts
        GROUP BY object_name
        ORDER BY count DESC
        LIMIT 10
        """
        
        try:
            df4 = pd.read_sql_query(query4, conn)
            if not df4.empty:
                print(df4.to_string(index=False))
            else:
                print("No object detection data")
        except:
            print("Could not parse object detection data")
        
        conn.close()
        
        print("\n✅ YOLO integration test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def analyze_yolo_questions():
    """Answer Task 3 business questions"""
    print("\n" + "="*60)
    print("TASK 3 - BUSINESS QUESTIONS ANALYSIS")
    print("="*60)
    
    db_path = "data/medical_warehouse.db"
    conn = sqlite3.connect(db_path)
    
    # Question 1: Do "promotional" posts get more views?
    print("\n1. Do 'promotional' posts get more views than 'product_display' posts?")
    print("-" * 60)
    
    query1 = """
    SELECT 
        y.image_category,
        COUNT(*) as post_count,
        AVG(f.view_count) as avg_views,
        AVG(f.forward_count) as avg_forwards,
        SUM(f.view_count) as total_views
    FROM yolo_detections y
    JOIN fct_messages f ON y.message_id = f.message_id
    WHERE y.image_category IN ('promotional', 'product_display')
    GROUP BY y.image_category
    ORDER BY avg_views DESC
    """
    
    df1 = pd.read_sql_query(query1, conn)
    if not df1.empty:
        print(df1.to_string(index=False))
        
        promotional = df1[df1['image_category'] == 'promotional']
        product_display = df1[df1['image_category'] == 'product_display']
        
        if not promotional.empty and not product_display.empty:
            prom_views = promotional['avg_views'].iloc[0]
            prod_views = product_display['avg_views'].iloc[0]
            
            if prom_views > prod_views:
                print(f"\n📈 ANSWER: YES - Promotional posts get {prom_views:.1f} views vs {prod_views:.1f} for product display")
            else:
                print(f"\n📉 ANSWER: NO - Product display posts get more views")
    else:
        print("Insufficient data for comparison")
    
    # Question 2: Which channels use more visual content?
    print("\n\n2. Which channels use more visual content?")
    print("-" * 60)
    
    query2 = """
    SELECT 
        c.channel_name,
        c.channel_type,
        COUNT(y.id) as image_count,
        COUNT(f.message_id) as message_count,
        ROUND(COUNT(y.id) * 100.0 / NULLIF(COUNT(f.message_id), 0), 2) as images_percentage
    FROM dim_channels c
    LEFT JOIN fct_messages f ON c.channel_key = f.channel_key
    LEFT JOIN yolo_detections y ON f.message_id = y.message_id
    GROUP BY c.channel_name, c.channel_type
    HAVING message_count > 0
    ORDER BY images_percentage DESC
    LIMIT 5
    """
    
    df2 = pd.read_sql_query(query2, conn)
    if not df2.empty:
        print(df2.to_string(index=False))
        top_channel = df2.iloc[0]
        print(f"\n🏆 MOST VISUAL CHANNEL: {top_channel['channel_name']} ({top_channel['images_percentage']}% images)")
    else:
        print("No channel data available")
    
    # Question 3: Limitations of pre-trained models
    print("\n\n3. What are the limitations of using pre-trained models?")
    print("-" * 60)
    
    limitations = """
    ✅ LIMITATIONS IDENTIFIED:
    
    1. DOMAIN SPECIFICITY:
       - YOLO trained on COCO dataset (80 general objects)
       - Medical products often not in training data
       - May misclassify medical items as similar-looking objects
    
    2. ETHIOPIAN CONTEXT:
       - Local products, packaging not represented
       - Cultural context not captured
       - Language/text in images not analyzed
    
    3. BUSINESS RELEVANCE:
       - Can't detect specific drugs/medicines
       - Can't read prices or product names
       - Limited business intelligence without custom training
    
    4. RECOMMENDATIONS:
       - Fine-tune YOLO on medical product dataset
       - Add text detection (OCR) for prices
       - Combine with NLP for message text analysis
    """
    
    print(limitations)
    
    conn.close()

if __name__ == "__main__":
    test_yolo_integration()
    analyze_yolo_questions()