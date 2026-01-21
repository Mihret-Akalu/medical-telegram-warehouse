# Medical Telegram Warehouse - Schema Documentation

## Star Schema Design

### Dimension Tables

#### dim_dates
- **Purpose**: Time dimension for date-based analysis
- **Primary Key**: date_key (YYYYMMDD format)
- **Key Fields**: full_date, year, quarter, month, day_name, is_weekend

#### dim_channels
- **Purpose**: Channel dimension with business classification
- **Primary Key**: channel_key
- **Key Fields**: channel_name, channel_type, business metrics
- **Business Logic**: Automatic classification based on channel name patterns

### Fact Table

#### fct_messages
- **Purpose**: Core fact table for message analytics
- **Primary Key**: message_id (business key)
- **Foreign Keys**: channel_key -> dim_channels, date_key -> dim_dates
- **Metrics**: view_count, forward_count, has_image

## Data Quality
- **Staging Layer**: Includes data quality validation
- **Tests Implemented**: Future dates, negative values, referential integrity
