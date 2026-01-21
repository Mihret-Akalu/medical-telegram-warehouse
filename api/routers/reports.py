"""
Analytical report endpoints
"""

from fastapi import APIRouter, Depends, Query
import sqlite3
from typing import List, Optional
from api.schemas import (
    TopProductsResponse, ProductResponse, 
    ChannelPerformance, DailyTrends, ImageAnalysis
)
from api.database import get_db

router = APIRouter(prefix="/reports", tags=["reports"])

@router.get("/top-products", response_model=TopProductsResponse)
async def get_top_products(
    limit: int = Query(10, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db)
):
    """
    Get top mentioned products across all channels
    
    - **limit**: Number of products to return (default: 10, max: 100)
    """
    cursor = db.cursor()
    
    # Extract product mentions from messages
    query = """
    WITH product_mentions AS (
        SELECT 
            LOWER(TRIM(f.message_text)) as message_text,
            f.view_count,
            c.channel_type
        FROM fct_messages f
        JOIN dim_channels c ON f.channel_key = c.channel_key
        WHERE f.message_text IS NOT NULL
          AND f.message_text != ''
    ),
    product_categories AS (
        SELECT 
            CASE 
                WHEN message_text LIKE '%tablet%' OR message_text LIKE '%pill%' THEN 'Tablets'
                WHEN message_text LIKE '%capsule%' THEN 'Capsules'
                WHEN message_text LIKE '%cream%' OR message_text LIKE '%ointment%' THEN 'Topical'
                WHEN message_text LIKE '%syrup%' OR message_text LIKE '%liquid%' THEN 'Liquids'
                WHEN message_text LIKE '%injection%' THEN 'Injections'
                WHEN message_text LIKE '%vitamin%' THEN 'Vitamins'
                WHEN message_text LIKE '%supplement%' THEN 'Supplements'
                WHEN message_text LIKE '%device%' OR message_text LIKE '%equipment%' THEN 'Devices'
                WHEN message_text LIKE '%mg%' OR message_text LIKE '%ml%' THEN 'Medications'
                ELSE 'Other'
            END as product_category,
            COUNT(*) as mention_count,
            SUM(view_count) as total_views,
            COUNT(DISTINCT channel_type) as channel_types_count
        FROM product_mentions
        WHERE message_text LIKE '%mg%' OR message_text LIKE '%ml%' OR 
              message_text LIKE '%tablet%' OR message_text LIKE '%capsule%' OR
              message_text LIKE '%cream%' OR message_text LIKE '%ointment%' OR
              message_text LIKE '%syrup%' OR message_text LIKE '%injection%' OR
              message_text LIKE '%vitamin%' OR message_text LIKE '%supplement%' OR
              message_text LIKE '%device%' OR message_text LIKE '%equipment%'
        GROUP BY 1
        HAVING mention_count > 0
    )
    SELECT 
        product_category as product_name,
        product_category,
        mention_count,
        channel_types_count as channel_count,
        total_views,
        ROUND(total_views * 1.0 / NULLIF(mention_count, 0), 2) as avg_views,
        ROW_NUMBER() OVER (ORDER BY mention_count DESC) as popularity_rank
    FROM product_categories
    ORDER BY mention_count DESC
    LIMIT ?
    """
    
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    
    products = []
    for row in rows:
        products.append(ProductResponse(
            product_name=row['product_name'],
            product_category=row['product_category'],
            mention_count=row['mention_count'],
            channel_count=row['channel_count'],
            total_views=row['total_views'],
            avg_views=row['avg_views'],
            popularity_rank=row['popularity_rank']
        ))
    
    return TopProductsResponse(
        products=products,
        total_products=len(products)
    )

@router.get("/channel-performance")
async def get_channel_performance(
    min_posts: int = Query(1, ge=1),
    db: sqlite3.Connection = Depends(get_db)
):
    """
    Get channel performance analysis
    
    - **min_posts**: Minimum posts required to be included
    """
    cursor = db.cursor()
    
    query = """
    SELECT 
        c.channel_name,
        c.channel_type,
        c.total_posts,
        c.avg_views,
        c.image_percentage,
        c.activity_status,
        CASE 
            WHEN c.avg_views > 1000 THEN 'High Performer'
            WHEN c.avg_views > 100 THEN 'Medium Performer'
            ELSE 'Low Performer'
        END as performance_category,
        COUNT(f.message_id) as warehouse_messages,
        ROUND(AVG(f.view_count), 2) as avg_message_views
    FROM dim_channels c
    LEFT JOIN fct_messages f ON c.channel_key = f.channel_key
    WHERE c.total_posts >= ?
    GROUP BY c.channel_name, c.channel_type, c.total_posts, c.avg_views, 
             c.image_percentage, c.activity_status
    ORDER BY avg_message_views DESC
    """
    
    cursor.execute(query, (min_posts,))
    rows = cursor.fetchall()
    
    channels = []
    for row in rows:
        channels.append(ChannelPerformance(
            channel_name=row['channel_name'],
            channel_type=row['channel_type'],
            total_posts=row['total_posts'],
            avg_views=row['avg_views'],
            image_percentage=row['image_percentage'],
            activity_status=row['activity_status'],
            performance_category=row['performance_category']
        ))
    
    return {"channels": channels, "total_channels": len(channels)}

@router.get("/daily-trends")
async def get_daily_trends(
    days: int = Query(7, ge=1, le=365),
    db: sqlite3.Connection = Depends(get_db)
):
    """
    Get daily posting trends
    
    - **days**: Number of days to analyze (default: 7, max: 365)
    """
    cursor = db.cursor()
    
    query = """
    SELECT 
        d.full_date,
        d.day_name,
        COUNT(f.message_id) as post_count,
        SUM(f.view_count) as total_views,
        ROUND(AVG(f.view_count), 2) as avg_views_per_post,
        COUNT(DISTINCT f.channel_key) as channels_active
    FROM dim_dates d
    LEFT JOIN fct_messages f ON d.date_key = f.date_key
    WHERE d.full_date >= DATE('now', ?)
    GROUP BY d.full_date, d.day_name
    ORDER BY d.full_date DESC
    """
    
    cursor.execute(query, (f"-{days} days",))
    rows = cursor.fetchall()
    
    trends = []
    for row in rows:
        trends.append(DailyTrends(
            full_date=row['full_date'],
            post_count=row['post_count'],
            total_views=row['total_views'],
            avg_views_per_post=row['avg_views_per_post'],
            channels_active=row['channels_active']
        ))
    
    return {"days_analyzed": days, "trends": trends}

@router.get("/visual-content")
async def get_visual_content_stats(db: sqlite3.Connection = Depends(get_db)):
    """
    Get statistics about image usage across channels
    """
    cursor = db.cursor()
    
    # Overall image analysis
    query1 = """
    SELECT 
        CASE WHEN has_image = 1 THEN 'With Images' ELSE 'Text Only' END as message_type,
        COUNT(*) as message_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fct_messages), 2) as percentage,
        ROUND(AVG(view_count), 2) as avg_views
    FROM fct_messages
    GROUP BY has_image
    ORDER BY message_count DESC
    """
    
    cursor.execute(query1)
    image_stats = cursor.fetchall()
    
    # Channels with most images
    query2 = """
    SELECT 
        c.channel_name,
        c.channel_type,
        c.total_posts,
        c.image_percentage,
        SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) as image_posts,
        ROUND(AVG(CASE WHEN f.has_image THEN f.view_count ELSE NULL END), 2) as avg_image_views
    FROM dim_channels c
    JOIN fct_messages f ON c.channel_key = f.channel_key
    WHERE f.has_image = 1
    GROUP BY c.channel_name, c.channel_type, c.total_posts, c.image_percentage
    ORDER BY image_posts DESC
    LIMIT 10
    """
    
    cursor.execute(query2)
    top_channels = cursor.fetchall()
    
    # Daily image trends
    query3 = """
    SELECT 
        d.full_date,
        COUNT(f.message_id) as total_posts,
        SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) as image_posts,
        ROUND(SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(f.message_id), 0), 2) as image_percentage
    FROM dim_dates d
    LEFT JOIN fct_messages f ON d.date_key = f.date_key
    WHERE d.full_date >= DATE('now', '-30 days')
    GROUP BY d.full_date
    ORDER BY d.full_date DESC
    """
    
    cursor.execute(query3)
    image_trends = cursor.fetchall()
    
    # Prepare response
    image_analysis = []
    for stat in image_stats:
        image_analysis.append(ImageAnalysis(
            message_type=stat['message_type'],
            message_count=stat['message_count'],
            percentage=stat['percentage'],
            avg_views=stat['avg_views']
        ))
    
    channels = []
    for channel in top_channels:
        channels.append(ChannelPerformance(
            channel_name=channel['channel_name'],
            channel_type=channel['channel_type'],
            total_posts=channel['total_posts'],
            avg_views=channel['avg_image_views'] or 0,
            image_percentage=channel['image_percentage'],
            activity_status="",  # Not in this query
            performance_category=""
        ))
    
    trends = []
    for trend in image_trends:
        trends.append(DailyTrends(
            full_date=trend['full_date'],
            post_count=trend['image_posts'],
            total_views=0,  # Not in this query
            avg_views_per_post=0,
            channels_active=0
        ))
    
    return {
        "image_analysis": image_analysis,
        "top_channels_with_images": channels,
        "daily_image_trends": trends
    }