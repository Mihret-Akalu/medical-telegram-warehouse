"""
Search endpoints for messages and channels
"""

from fastapi import APIRouter, Depends, Query
import sqlite3
from typing import List, Optional
from api.schemas import MessageResponse, SearchResponse
from api.database import get_db

router = APIRouter(prefix="/search", tags=["search"])

@router.get("/messages", response_model=SearchResponse)
async def search_messages(
    query: str = Query(..., min_length=2, description="Search term"),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    limit: int = Query(20, ge=1, le=100),
    page: int = Query(1, ge=1),
    db: sqlite3.Connection = Depends(get_db)
):
    """
    Search for messages containing specific keywords
    
    - **query**: Search term (minimum 2 characters)
    - **channel**: Filter by channel name (optional)
    - **limit**: Results per page (default: 20, max: 100)
    - **page**: Page number (default: 1)
    """
    cursor = db.cursor()
    
    # Build query
    sql = """
    SELECT 
        f.message_id,
        c.channel_name,
        c.channel_type,
        d.full_date as message_date,
        f.message_text,
        f.message_length,
        f.view_count,
        f.forward_count,
        f.has_image
    FROM fct_messages f
    JOIN dim_channels c ON f.channel_key = c.channel_key
    JOIN dim_dates d ON f.date_key = d.date_key
    WHERE LOWER(f.message_text) LIKE LOWER(?)
    """
    
    params = [f"%{query}%"]
    
    if channel:
        sql += " AND LOWER(c.channel_name) LIKE LOWER(?)"
        params.append(f"%{channel}%")
    
    # Get total count
    count_sql = f"SELECT COUNT(*) as total FROM ({sql})"
    cursor.execute(count_sql, params)
    total_count = cursor.fetchone()['total']
    
    # Get paginated results
    sql += " ORDER BY d.full_date DESC LIMIT ? OFFSET ?"
    offset = (page - 1) * limit
    params.extend([limit, offset])
    
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    
    messages = []
    for row in rows:
        messages.append(MessageResponse(
            message_id=row['message_id'],
            channel_name=row['channel_name'],
            channel_type=row['channel_type'],
            message_date=row['message_date'],
            message_text=row['message_text'],
            message_length=row['message_length'],
            view_count=row['view_count'],
            forward_count=row['forward_count'],
            has_image=bool(row['has_image'])
        ))
    
    return SearchResponse(
        messages=messages,
        total_count=total_count,
        page=page,
        page_size=limit
    )

@router.get("/channels")
async def search_channels(
    name: Optional[str] = Query(None, description="Channel name search"),
    channel_type: Optional[str] = Query(None, description="Filter by channel type"),
    activity_status: Optional[str] = Query(None, description="Filter by activity status"),
    min_posts: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db)
):
    """
    Search for channels with filters
    
    - **name**: Search by channel name
    - **channel_type**: Filter by type (Pharmaceutical/Cosmetics/Medical/Other)
    - **activity_status**: Filter by activity (active/moderate/inactive)
    - **min_posts**: Minimum number of posts
    """
    cursor = db.cursor()
    
    sql = """
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
    WHERE 1=1
    """
    
    params = []
    
    if name:
        sql += " AND LOWER(channel_name) LIKE LOWER(?)"
        params.append(f"%{name}%")
    
    if channel_type:
        sql += " AND channel_type = ?"
        params.append(channel_type)
    
    if activity_status:
        sql += " AND activity_status = ?"
        params.append(activity_status)
    
    if min_posts > 0:
        sql += " AND total_posts >= ?"
        params.append(min_posts)
    
    sql += " ORDER BY total_posts DESC"
    
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    
    channels = []
    for row in rows:
        channels.append(dict(row))
    
    return {"channels": channels, "total_channels": len(channels)}