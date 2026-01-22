"""
Dagster assets for Telegram data pipeline
"""

import os
import json
import sqlite3
import glob
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import logging
from dagster import asset, Output, MetadataValue
import asyncio
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
import csv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = "data/medical_warehouse.db"
RAW_DATA_DIR = "data/raw/telegram_messages"
IMAGES_DIR = "data/raw/images"
LOGS_DIR = "logs"

@asset(
    description="Scrape raw data from Telegram channels",
    required_resource_keys={"telegram_client"},
    io_manager_key="fs_io_manager"
)
def raw_telegram_data(context):
    """Asset to scrape Telegram data"""
    logger.info("Starting Telegram scraping...")
    
    # Get today's date for partitioning
    today = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Import scraper functions
        from src.scraper import AsyncTelegramScraper
        from src.datalake import ensure_dir
        
        # Setup directories
        ensure_dir(RAW_DATA_DIR)
        ensure_dir(IMAGES_DIR)
        ensure_dir(LOGS_DIR)
        
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
        
        api_id = int(os.getenv("TELEGRAM_API_ID"))
        api_hash = os.getenv("TELEGRAM_API_HASH")
        phone = os.getenv("TELEGRAM_PHONE")
        
        # Target channels
        target_channels = [
            '@lobelia4cosmetics',
            '@tikvahpharma',
        ]
        
        # Run scraper
        async def scrape():
            scraper = AsyncTelegramScraper(api_id, api_hash, phone)
            await scraper.connect()
            messages = await scraper.scrape_all_channels(target_channels, limit=30)
            await scraper.disconnect()
            return messages
        
        # Run async function
        messages = asyncio.run(scrape())
        
        total_messages = len(messages)
        
        # Save metadata
        metadata = {
            "date_scraped": today,
            "total_messages": total_messages,
            "channels": ", ".join(target_channels),
            "output_path": f"{RAW_DATA_DIR}/{today}/"
        }
        
        context.log.info(f"Scraped {total_messages} messages")
        
        return Output(
            value={"messages": messages, "date": today},
            metadata=metadata
        )
        
    except Exception as e:
        logger.error(f"Error scraping Telegram: {e}")
        raise

@asset(
    description="Process and transform raw data into data warehouse",
    deps=["raw_telegram_data"]
)
def processed_telegram_data(context):
    """Asset to process raw data into structured warehouse"""
    logger.info("Processing raw data into data warehouse...")
    
    try:
        # Import processing functions
        import sqlite3
        from datetime import datetime
        
        # Connect to database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 1. Load raw data from JSON files
        json_files = glob.glob(f"{RAW_DATA_DIR}/*/*.json")
        total_messages = 0
        
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            for msg in messages:
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
        
        conn.commit()
        
        # 2. Create or refresh star schema
        # (Reuse the create_star_schema function from Task 2)
        from run_task2_fixed import Task2DataWarehouse
        warehouse = Task2DataWarehouse()
        warehouse.create_star_schema(conn)
        
        # 3. Get statistics
        cursor.execute("SELECT COUNT(*) FROM dim_channels")
        channels_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM fct_messages")
        messages_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT channel_type) FROM dim_channels")
        channel_types = cursor.fetchone()[0]
        
        conn.close()
        
        metadata = {
            "raw_messages_loaded": total_messages,
            "channels_processed": channels_count,
            "messages_in_warehouse": messages_count,
            "channel_types": channel_types,
            "database_path": DB_PATH
        }
        
        context.log.info(f"Processed {total_messages} messages into warehouse")
        
        return Output(
            value={
                "channels": channels_count,
                "messages": messages_count,
                "timestamp": datetime.now().isoformat()
            },
            metadata=metadata
        )
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

@asset(
    description="Enrich data with YOLO object detection",
    deps=["processed_telegram_data"]
)
def yolo_enriched_data(context):
    """Asset to run YOLO object detection on images"""
    logger.info("Running YOLO object detection...")
    
    try:
        from ultralytics import YOLO
        import cv2
        from pathlib import Path
        
        # Load YOLO model
        model = YOLO('yolov8n.pt')  # Using nano model for efficiency
        
        # Find images
        image_files = []
        for ext in ['*.jpg', '*.jpeg', '*.png']:
            image_files.extend(glob.glob(f"{IMAGES_DIR}/**/{ext}", recursive=True))
        
        if not image_files:
            context.log.warning("No images found for YOLO detection")
            return Output(
                value={"images_processed": 0, "detections": []},
                metadata={"status": "no_images_found"}
            )
        
        # Process images
        detections = []
        images_processed = 0
        
        for image_path in image_files[:10]:  # Process first 10 images for demo
            try:
                # Run YOLO detection
                results = model(image_path)
                
                # Extract detections
                for result in results:
                    if result.boxes is not None:
                        for box in result.boxes:
                            detection = {
                                "image_path": image_path,
                                "class": model.names[int(box.cls)],
                                "confidence": float(box.conf),
                                "bbox": box.xyxy[0].tolist(),
                                "timestamp": datetime.now().isoformat()
                            }
                            detections.append(detection)
                
                images_processed += 1
                context.log.info(f"Processed {image_path}")
                
            except Exception as e:
                context.log.warning(f"Error processing {image_path}: {e}")
                continue
        
        # Categorize images based on detections
        categories = {
            "promotional": 0,  # Person + product
            "product_display": 0,  # Bottle/container, no person
            "lifestyle": 0,  # Person, no product
            "other": 0
        }
        
        for detection in detections:
            class_name = detection["class"].lower()
            
            if "person" in class_name:
                if any(prod in class_name for prod in ["bottle", "container", "packet"]):
                    categories["promotional"] += 1
                else:
                    categories["lifestyle"] += 1
            elif any(prod in class_name for prod in ["bottle", "container", "packet"]):
                categories["product_display"] += 1
            else:
                categories["other"] += 1
        
        # Save detections to CSV
        output_csv = f"data/processed/yolo_detections_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        os.makedirs("data/processed", exist_ok=True)
        
        if detections:
            df = pd.DataFrame(detections)
            df.to_csv(output_csv, index=False)
        
        metadata = {
            "images_processed": images_processed,
            "total_detections": len(detections),
            "categories": str(categories),
            "output_csv": output_csv,
            "model_used": "yolov8n.pt"
        }
        
        context.log.info(f"Processed {images_processed} images with {len(detections)} detections")
        
        return Output(
            value={
                "images_processed": images_processed,
                "detections": len(detections),
                "categories": categories
            },
            metadata=metadata
        )
        
    except Exception as e:
        logger.error(f"Error in YOLO detection: {e}")
        raise

@asset(
    description="Prepare data for analytical API",
    deps=["processed_telegram_data", "yolo_enriched_data"]
)
def analytical_api_data(context):
    """Asset to prepare data for FastAPI analytical endpoints"""
    logger.info("Preparing data for analytical API...")
    
    try:
        import sqlite3
        import pandas as pd
        
        # Connect to database
        conn = sqlite3.connect(DB_PATH)
        
        # 1. Prepare top products data
        top_products_query = """
        WITH product_mentions AS (
            SELECT 
                LOWER(TRIM(message_text)) as message_text,
                view_count
            FROM fct_messages
            WHERE message_text IS NOT NULL
              AND message_text != ''
        ),
        product_keywords AS (
            SELECT 
                CASE 
                    WHEN message_text LIKE '%tablet%' OR message_text LIKE '%pill%' THEN 'Tablets'
                    WHEN message_text LIKE '%capsule%' THEN 'Capsules'
                    WHEN message_text LIKE '%cream%' OR message_text LIKE '%ointment%' THEN 'Creams/Ointments'
                    WHEN message_text LIKE '%syrup%' OR message_text LIKE '%liquid%' THEN 'Liquids'
                    WHEN message_text LIKE '%injection%' THEN 'Injections'
                    WHEN message_text LIKE '%vitamin%' THEN 'Vitamins'
                    WHEN message_text LIKE '%supplement%' THEN 'Supplements'
                    WHEN message_text LIKE '%device%' THEN 'Medical Devices'
                    ELSE 'Other'
                END as product_category,
                COUNT(*) as mention_count,
                SUM(view_count) as total_views
            FROM product_mentions
            GROUP BY 1
        )
        SELECT 
            product_category,
            mention_count,
            total_views,
            ROUND(total_views * 1.0 / mention_count, 2) as avg_views_per_mention
        FROM product_keywords
        WHERE product_category != 'Other'
        ORDER BY mention_count DESC
        LIMIT 20
        """
        
        top_products_df = pd.read_sql_query(top_products_query, conn)
        
        # 2. Prepare channel activity data
        channel_activity_query = """
        SELECT 
            channel_name,
            channel_type,
            total_posts,
            ROUND(avg_views, 2) as avg_views,
            activity_status,
            image_percentage,
            DATE(last_post_date) as last_post
        FROM dim_channels
        ORDER BY total_posts DESC
        """
        
        channel_activity_df = pd.read_sql_query(channel_activity_query, conn)
        
        # 3. Prepare visual content stats
        visual_stats_query = """
        SELECT 
            channel_type,
            COUNT(*) as total_messages,
            SUM(CASE WHEN has_image = 1 THEN 1 ELSE 0 END) as image_messages,
            ROUND(SUM(CASE WHEN has_image = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as image_percentage,
            AVG(CASE WHEN has_image = 1 THEN view_count ELSE NULL END) as avg_views_with_images,
            AVG(CASE WHEN has_image = 0 THEN view_count ELSE NULL END) as avg_views_without_images
        FROM fct_messages f
        JOIN dim_channels c ON f.channel_key = c.channel_key
        GROUP BY channel_type
        """
        
        visual_stats_df = pd.read_sql_query(visual_stats_query, conn)
        
        # 4. Prepare daily trends
        daily_trends_query = """
        SELECT 
            d.full_date,
            d.day_name,
            COUNT(f.message_id) as post_count,
            SUM(f.view_count) as total_views,
            AVG(f.view_count) as avg_views
        FROM fct_messages f
        JOIN dim_dates d ON f.date_key = d.date_key
        WHERE d.full_date >= DATE('now', '-30 days')
        GROUP BY d.full_date, d.day_name
        ORDER BY d.full_date DESC
        """
        
        daily_trends_df = pd.read_sql_query(daily_trends_query, conn)
        
        conn.close()
        
        # Save data for API
        api_data_dir = "api/data"
        os.makedirs(api_data_dir, exist_ok=True)
        
        top_products_df.to_csv(f"{api_data_dir}/top_products.csv", index=False)
        channel_activity_df.to_csv(f"{api_data_dir}/channel_activity.csv", index=False)
        visual_stats_df.to_csv(f"{api_data_dir}/visual_stats.csv", index=False)
        daily_trends_df.to_csv(f"{api_data_dir}/daily_trends.csv", index=False)
        
        metadata = {
            "top_products_count": len(top_products_df),
            "channels_analyzed": len(channel_activity_df),
            "visual_stats_categories": len(visual_stats_df),
            "daily_trends_days": len(daily_trends_df),
            "api_data_path": api_data_dir
        }
        
        context.log.info(f"Prepared API data: {len(top_products_df)} products, {len(channel_activity_df)} channels")
        
        return Output(
            value={
                "top_products": len(top_products_df),
                "channels": len(channel_activity_df),
                "visual_categories": len(visual_stats_df),
                "trend_days": len(daily_trends_df)
            },
            metadata=metadata
        )
        
    except Exception as e:
        logger.error(f"Error preparing API data: {e}")
        raise