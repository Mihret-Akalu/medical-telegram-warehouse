"""
Dagster pipeline for Medical Telegram Warehouse
"""

from dagster import Definitions, load_assets_from_modules, ScheduleDefinition

from . import assets
from .jobs import daily_pipeline_job, scrape_only_job, process_only_job, yolo_only_job
from .schedules import daily_schedule, hourly_scrape_schedule, weekly_analytics_schedule
from .resources import telegram_client, sqlite_database, yolo_model

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[daily_pipeline_job, scrape_only_job, process_only_job, yolo_only_job],
    schedules=[daily_schedule, hourly_scrape_schedule, weekly_analytics_schedule],
    resources={
        "telegram_client": telegram_client,
        "sqlite_database": sqlite_database,
        "yolo_model": yolo_model
    }
)