"""
Pydantic models for request/response validation
"""

from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import date, datetime

# Channel schemas
class ChannelBase(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float

class ChannelResponse(ChannelBase):
    channel_key: int
    channel_username: Optional[str]
    channel_title: Optional[str]
    first_post_date: Optional[datetime]
    last_post_date: Optional[datetime]
    avg_forwards: float
    media_percentage: float
    image_percentage: float
    activity_status: str

# Message schemas
class MessageBase(BaseModel):
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str

class MessageResponse(MessageBase):
    message_length: int
    view_count: int
    forward_count: int
    has_image: bool
    channel_type: str

# Product schemas
class ProductBase(BaseModel):
    product_name: str
    product_category: str
    mention_count: int

class ProductResponse(ProductBase):
    channel_count: int
    total_views: int
    avg_views: float
    popularity_rank: int

# Analytics schemas
class DailyTrends(BaseModel):
    full_date: date
    post_count: int
    total_views: int
    avg_views_per_post: float
    channels_active: int

class ChannelPerformance(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float
    image_percentage: float
    activity_status: str
    performance_category: str

class ImageAnalysis(BaseModel):
    message_type: str
    message_count: int
    percentage: float
    avg_views: float

# API Response schemas
class TopProductsResponse(BaseModel):
    products: List[ProductResponse]
    total_products: int

class ChannelActivityResponse(BaseModel):
    channel: ChannelResponse
    recent_messages: List[MessageResponse]
    daily_trends: List[DailyTrends]

class SearchResponse(BaseModel):
    messages: List[MessageResponse]
    total_count: int
    page: int
    page_size: int

class VisualContentResponse(BaseModel):
    image_analysis: ImageAnalysis
    channels_with_images: List[ChannelPerformance]
    daily_image_trends: List[DailyTrends]

class HealthCheck(BaseModel):
    status: str
    database: str
    tables_count: int
    timestamp: datetime