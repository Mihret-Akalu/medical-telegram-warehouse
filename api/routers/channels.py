"""
Channel-specific endpoints
"""

from fastapi import APIRouter, Depends, HTTPException
import sqlite3
from typing import List
from api.schemas import ChannelResponse, MessageResponse, DailyTrends
from api.database import get_db

router = APIRouter(prefix="/channels", tags=["channels"])

@router.get("/", response_model=List[ChannelResponse])
async def list_channels(
    sort_by: str = "total_posts",
    order: str = "desc",
    db: sqlite3.Connection = Depends(get_db)
):
    """
    List all channels with sorting options
    
    - **sort_by**: Field to sort by (total_posts, avg_views, last_post_date)
    - **order**: Sort order (asc, desc)
    """
    valid_sort_fields = ["total_posts", "avg_views", "last_post_date", "channel_name"]
    if sort_by not in valid_sort_fields:
        raise HTTPException(status_code=400, detail=f"Invalid sort field. Valid options: {valid_sort_fields}")
    
    valid_orders = ["asc", "desc"]
    if order not in valid_orders:
        raise HTTPException(status_code=400, detail=f"Invalid order. Valid options: {valid_orders}")
    
    cursor = db.cursor()
    
    query = f"""
    SELECT 
        channel_key,
        channel_name,
        channel_username,
        channel_title,
        channel_type,
        first_post_date,
        last_post_date,
        total_posts,
        avg_views,
        avg_forwards,
        media_percentage,
        image_percentage,
        activity_status
    FROM dim_channels
    ORDER BY {sort_by} {order.upper()}
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    channels = []
    for row in rows:
        channels.append(ChannelResponse(
            channel_key=row['channel_key'],
            channel_name=row['channel_name'],
            channel_username=row['channel_username'],
            channel_title=row['channel_title'],
            channel_type=row['channel_type'],
            first_post_date=row['first_post_date'],
            last_post_date=row['last_post_date'],
            total_posts=row['total_posts'],
            avg_views=row['avg_views'],
            avg_forwards=row['avg_forwards'],
            media_percentage=row['media_percentage'],
            image_percentage=row['image_percentage'],
            activity_status=row['activity_status']
        ))
    
    return channels

@router.get("/{channel_name}/activity")
async def get_channel_activity(
    channel_name: str,
    days: int = 30,
    db: sqlite3.Connection = Depends(get_db)
):
    """
    Get activity details for a specific channel
    
    - **channel_name**: Name of the channel
    - **days**: Number of days to analyze (default: 30)
    """
    cursor = db.cursor()
    
    # Get channel info
    cursor.execute("""
    SELECT 
        channel_key,
        channel_name,
        channel_type,
        total_posts,
        avg_views,
        activity_status
    FROM dim_channels
    WHERE channel_name = ?
    """, (channel_name,))
    
    channel = cursor.fetchone()
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    
    # Get recent messages
    cursor.execute("""
    SELECT 
        f.message_id,
        c.channel_name,
        d.full_date as message_date,
        f.message_text,
        f.message_length,
        f.view_count,
        f.forward_count,
        f.has_image
    FROM fct_messages f
    JOIN dim_channels c ON f.channel_key = c.channel_key
    JOIN dim_dates d ON f.date_key = d.date_key
    WHERE c.channel_name = ?
    ORDER BY d.full_date DESC
    LIMIT 10
    """, (channel_name,))
    
    messages = cursor.fetchall()
    
    # Get daily trends
    cursor.execute("""
    SELECT 
        d.full_date,
        d.day_name,
        COUNT(f.message_id) as post_count,
        SUM(f.view_count) as total_views,
        ROUND(AVG(f.view_count), 2) as avg_views_per_post
    FROM dim_dates d
    LEFT JOIN fct_messages f ON d.date_key = f.date_key
    LEFT JOIN dim_channels c ON f.channel_key = c.channel_key
    WHERE c.channel_name = ?
      AND d.full_date >= DATE('now', ?)
    GROUP BY d.full_date, d.day_name
    ORDER BY d.full_date DESC
    """, (channel_name, f"-{days} days"))
    
    trends = cursor.fetchall()
    
    # Prepare response
    recent_messages = []
    for msg in messages:
        recent_messages.append(MessageResponse(
            message_id=msg['message_id'],
            channel_name=msg['channel_name'],
            channel_type=channel['channel_type'],
            message_date=msg['message_date'],
            message_text=msg['message_text'],
            message_length=msg['message_length'],
            view_count=msg['view_count'],
            forward_count=msg['forward_count'],
            has_image=bool(msg['has_image'])
        ))
    
    daily_trends = []
    for trend in trends:
        daily_trends.append(DailyTrends(
            full_date=trend['full_date'],
            post_count=trend['post_count'],
            total_views=trend['total_views'],
            avg_views_per_post=trend['avg_views_per_post'],
            channels_active=1
        ))
    
    return {
        "channel": dict(channel),
        "recent_messages": recent_messages,
        "daily_trends": daily_trends,
        "days_analyzed": days
    }

@router.get("/{channel_name}/stats")
async def get_channel_stats(channel_name: str, db: sqlite3.Connection = Depends(get_db)):
    """
    Get detailed statistics for a channel
    """
    cursor = db.cursor()
    
    # Get basic stats
    cursor.execute("""
    SELECT 
        channel_name,
        channel_type,
        total_posts,
        avg_views,
        avg_forwards,
        media_percentage,
        image_percentage,
        activity_status,
        first_post_date,
        last_post_date
    FROM dim_channels
    WHERE channel_name = ?
    """, (channel_name,))
    
    channel = cursor.fetchone()
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    
    # Get engagement stats
    cursor.execute("""
    SELECT 
        COUNT(*) as total_messages,
        SUM(view_count) as total_views,
        SUM(forward_count) as total_forwards,
        AVG(view_count) as avg_message_views,
        AVG(forward_count) as avg_message_forwards,
        SUM(CASE WHEN has_image THEN 1 ELSE 0 END) as image_messages,
        ROUND(AVG(CASE WHEN has_image THEN view_count ELSE NULL END), 2) as avg_image_views,
        ROUND(AVG(CASE WHEN NOT has_image THEN view_count ELSE NULL END), 2) as avg_text_views
    FROM fct_messages f
    JOIN dim_channels c ON f.channel_key = c.channel_key
    WHERE c.channel_name = ?
    """, (channel_name,))
    
    engagement = cursor.fetchone()
    
    # Get posting frequency
    cursor.execute("""
    SELECT 
        strftime('%H', d.full_date || 'T' || strftime('%H:%M:%S', m.message_date)) as hour_of_day,
        COUNT(*) as post_count
    FROM fct_messages f
    JOIN dim_channels c ON f.channel_key = c.channel_key
    JOIN dim_dates d ON f.date_key = d.date_key
    JOIN stg_telegram_messages m ON f.message_id = m.message_id
    WHERE c.channel_name = ?
      AND m.message_date IS NOT NULL
    GROUP BY hour_of_day
    ORDER BY post_count DESC
    LIMIT 5
    """, (channel_name,))
    
    posting_times = cursor.fetchall()
    
    # Get best performing messages
    cursor.execute("""
    SELECT 
        f.message_id,
        f.message_text,
        f.view_count,
        f.forward_count,
        f.has_image,
        d.full_date as message_date
    FROM fct_messages f
    JOIN dim_channels c ON f.channel_key = c.channel_key
    JOIN dim_dates d ON f.date_key = d.date_key
    WHERE c.channel_name = ?
    ORDER BY f.view_count DESC
    LIMIT 5
    """, (channel_name,))
    
    top_messages = cursor.fetchall()
    
    return {
        "channel_info": dict(channel),
        "engagement_stats": dict(engagement),
        "posting_patterns": {
            "best_posting_hours": [dict(t) for t in posting_times],
            "total_hours_analyzed": len(posting_times)
        },
        "top_performing_messages": [dict(m) for m in top_messages]
    }