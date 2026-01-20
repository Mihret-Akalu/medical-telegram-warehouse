"""
Telegram Scraper for Ethiopian Medical Channels - Python 3.14 Compatible
================================================================================
Fixed version for Python 3.14 with proper asyncio event loop handling.
"""

import os
import csv
import json
import asyncio
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageMediaPhoto

# Import your datalake module
try:
    from src.datalake import (
        write_channel_messages_json,
        write_manifest,
        telegram_images_dir,
        ensure_dir
    )
except ImportError:
    # Create simple replacements if datalake not available
    def ensure_dir(path: str) -> None:
        os.makedirs(path, exist_ok=True)
    
    def telegram_images_dir(base_path: str) -> str:
        return os.path.join(base_path, "raw", "images")
    
    def write_channel_messages_json(*, base_path: str, date_str: str, channel_name: str, messages: List[Dict[str, Any]]) -> str:
        json_dir = os.path.join(base_path, "raw", "telegram_messages", date_str)
        ensure_dir(json_dir)
        json_file = os.path.join(json_dir, f"{channel_name}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)
        return json_file
    
    def write_manifest(*, base_path: str, date_str: str, channel_message_counts: Dict[str, int], **kwargs) -> str:
        manifest_dir = os.path.join(base_path, "raw", "telegram_messages", date_str)
        ensure_dir(manifest_dir)
        manifest_file = os.path.join(manifest_dir, "_manifest.json")
        manifest = {
            "date": date_str,
            "run_utc": datetime.now().isoformat(),
            "channels": channel_message_counts,
            "total_messages": sum(channel_message_counts.values()),
        }
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        return manifest_file

# =============================================================================
# CONFIGURATION
# =============================================================================

# Load environment variables
load_dotenv()

# Validate required environment variables
api_id_str = os.getenv("TELEGRAM_API_ID")
api_hash = os.getenv("TELEGRAM_API_HASH")
phone = os.getenv("TELEGRAM_PHONE")

if not api_id_str or not api_hash or not phone:
    print("❌ ERROR: Missing Telegram credentials in .env file")
    print("\nYour .env file should contain:")
    print("TELEGRAM_API_ID=your_api_id")
    print("TELEGRAM_API_HASH=your_api_hash")
    print("TELEGRAM_PHONE=+251XXXXXXXXX")
    sys.exit(1)

api_id = int(api_id_str)

# Date string for partitioning
TODAY = datetime.now().strftime("%Y-%m-%d")

# Target channels
TARGET_CHANNELS = [
    '@lobelia4cosmetics',   # Lobelia Cosmetics
    '@tikvahpharma',        # Tikvah Pharma
]

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logger():
    """Setup logging"""
    LOG_DIR = "logs"
    ensure_dir(LOG_DIR)
    
    logger = logging.getLogger("telegram_scraper")
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"scrape_{TODAY}.log"),
        encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

# =============================================================================
# ASYNCIO FIX FOR PYTHON 3.14
# =============================================================================

class AsyncTelegramScraper:
    """Async scraper with proper event loop handling for Python 3.14"""
    
    def __init__(self, api_id: int, api_hash: str, phone: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.client = None
        
        # Create data directories
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            'data/raw/telegram_messages',
            'data/raw/images',
            'data/raw/csv',
            'logs'
        ]
        
        for directory in directories:
            ensure_dir(directory)
    
    async def connect(self):
        """Connect to Telegram"""
        try:
            self.client = TelegramClient(
                'telegram_session',
                self.api_id,
                self.api_hash
            )
            
            await self.client.start(phone=self.phone)
            
            me = await self.client.get_me()
            logger.info(f"✅ Connected as: {me.first_name} (ID: {me.id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False
    
    async def scrape_channel(self, channel_username: str, limit: int = 30):
        """Scrape a single channel"""
        channel_name = channel_username.strip('@')
        
        try:
            # Get channel
            entity = await self.client.get_entity(channel_username)
            channel_title = getattr(entity, 'title', channel_name)
            
            messages = []
            
            # Create image directory
            img_dir = os.path.join(telegram_images_dir('data'), channel_name)
            ensure_dir(img_dir)
            
            logger.info(f"📡 Scraping {channel_username}...")
            
            # Collect messages
            count = 0
            async for message in self.client.iter_messages(entity, limit=limit):
                # Download image if available
                image_path = None
                if message.media and isinstance(message.media, MessageMediaPhoto):
                    try:
                        filename = f"{message.id}.jpg"
                        image_path = os.path.join(img_dir, filename)
                        await self.client.download_media(message.media, image_path)
                    except Exception as e:
                        logger.warning(f"Failed to download image: {e}")
                        image_path = None
                
                # Create message data
                message_data = {
                    "message_id": message.id,
                    "channel_name": channel_name,
                    "channel_username": channel_username,
                    "channel_title": channel_title,
                    "message_date": message.date.isoformat() if message.date else None,
                    "message_text": message.message or "",
                    "has_media": message.media is not None,
                    "image_path": image_path,
                    "views": getattr(message, 'views', 0),
                    "forwards": getattr(message, 'forwards', 0),
                    "scraped_at": datetime.now().isoformat()
                }
                
                messages.append(message_data)
                count += 1
                
                if count % 10 == 0:
                    logger.info(f"  Scraped {count} messages...")
            
            # Save to JSON
            if messages:
                write_channel_messages_json(
                    base_path='data',
                    date_str=TODAY,
                    channel_name=channel_name,
                    messages=messages
                )
            
            logger.info(f"✅ {channel_username}: {len(messages)} messages")
            return messages
            
        except Exception as e:
            logger.error(f"Error scraping {channel_username}: {e}")
            return []
    
    async def scrape_all_channels(self, channels: List[str], limit: int = 30):
        """Scrape all channels"""
        all_messages = []
        channel_counts = {}
        
        for channel in channels:
            try:
                messages = await self.scrape_channel(channel, limit=limit)
                all_messages.extend(messages)
                channel_counts[channel.strip('@')] = len(messages)
                
                # Save CSV after each channel
                self.save_to_csv(all_messages)
                
                # Wait between channels
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"Failed to scrape {channel}: {e}")
                continue
        
        # Write manifest
        if channel_counts:
            write_manifest(
                base_path='data',
                date_str=TODAY,
                channel_message_counts=channel_counts
            )
        
        return all_messages
    
    def save_to_csv(self, messages: List[Dict[str, Any]]):
        """Save messages to CSV"""
        if not messages:
            return
        
        csv_dir = os.path.join('data', 'raw', 'csv', TODAY)
        ensure_dir(csv_dir)
        
        csv_file = os.path.join(csv_dir, 'telegram_data.csv')
        
        # Write CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            if messages:
                fieldnames = messages[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(messages)
        
        logger.debug(f"CSV saved: {csv_file}")
    
    async def disconnect(self):
        """Disconnect from Telegram"""
        if self.client:
            await self.client.disconnect()
            logger.info("Disconnected from Telegram")

# =============================================================================
# MAIN FUNCTION WITH PROPER EVENT LOOP
# =============================================================================

async def main_async():
    """Main async function"""
    print(f"\n{'='*60}")
    print("🚀 TELEGRAM SCRAPER - TASK 1")
    print(f"{'='*60}")
    print(f"Date: {TODAY}")
    print(f"Channels: {', '.join(TARGET_CHANNELS)}")
    print(f"{'='*60}\n")
    
    # Initialize scraper
    scraper = AsyncTelegramScraper(api_id, api_hash, phone)
    
    try:
        # Connect
        logger.info("Connecting to Telegram...")
        if not await scraper.connect():
            return
        
        # Scrape
        messages = await scraper.scrape_all_channels(TARGET_CHANNELS, limit=20)
        
        # Print summary
        if messages:
            print(f"\n{'='*60}")
            print("📊 SCRAPING COMPLETE!")
            print(f"{'='*60}")
            print(f"Total messages: {len(messages)}")
            
            # Group by channel
            from collections import defaultdict
            channel_stats = defaultdict(int)
            for msg in messages:
                channel_stats[msg['channel_name']] += 1
            
            for channel, count in channel_stats.items():
                print(f"  {channel}: {count} messages")
            
            print(f"\n📁 Data saved to:")
            print(f"  JSON: data/raw/telegram_messages/{TODAY}/")
            print(f"  Images: data/raw/images/")
            print(f"  CSV: data/raw/csv/{TODAY}/telegram_data.csv")
            print(f"  Logs: logs/scrape_{TODAY}.log")
            print(f"{'='*60}")
            print("🎉 TASK 1 COMPLETED SUCCESSFULLY!")
        else:
            print("\n⚠️ No messages were scraped")
            print("Check your .env file and channel names")
        
    except KeyboardInterrupt:
        print("\n⚠️ Scraping interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await scraper.disconnect()

def main():
    """Main entry point with proper event loop handling"""
    try:
        # Create and run event loop
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n⚠️ Program interrupted")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")

# =============================================================================
# QUICK TEST
# =============================================================================

def test_setup():
    """Test if everything is ready"""
    print("🧪 Testing setup...")
    
    # Check .env
    if not Path('.env').exists():
        print("❌ .env file not found")
        return False
    
    # Check credentials
    load_dotenv()
    if not all([os.getenv('TELEGRAM_API_ID'), 
                os.getenv('TELEGRAM_API_HASH'), 
                os.getenv('TELEGRAM_PHONE')]):
        print("❌ Missing credentials in .env")
        return False
    
    print("✅ .env file OK")
    
    # Check Python version
    print(f"Python version: {sys.version}")
    
    return True

if __name__ == "__main__":
    if test_setup():
        print("\n✅ Ready to scrape! Starting...\n")
        main()
    else:
        print("\n❌ Please fix the issues above")
        sys.exit(1)