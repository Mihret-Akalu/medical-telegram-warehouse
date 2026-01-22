"""
Dagster assets for the medical telegram pipeline
"""

from .telegram_assets import (
    raw_telegram_data,
    processed_telegram_data,
    yolo_enriched_data,
    analytical_api_data
)

__all__ = [
    "raw_telegram_data",
    "processed_telegram_data", 
    "yolo_enriched_data",
    "analytical_api_data"
]