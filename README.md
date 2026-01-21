# Sales ETL Pipeline

A Python ETL (Extract, Transform, Load) pipeline that processes online retail transaction data and loads it into a MySQL database for analysis.

## Overview

This project demonstrates a complete data engineering workflow:

- **Extract**: Reads raw sales data from Excel files
- **Transform**: Cleans, validates, and aggregates data into analytical summaries
- **Load**: Stores processed data in MySQL (also supports SQLite, CSV, and Parquet)

## Data Pipeline Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │
│             │     │             │     │             │
│ Excel File  │     │ Clean Data  │     │   MySQL     │
│ (541K rows) │     │ Aggregations│     │   SQLite    │
│             │     │ Summaries   │     │   CSV       │
└─────────────┘     └─────────────┘     └─────────────┘
```

## Features

- Handles 500K+ transaction records
- Data validation and cleaning (removes nulls, invalid values)
- Creates analytical summaries:
  - **Customer Summary**: Revenue, orders, and segments per customer
  - **Product Summary**: Sales metrics per product
  - **Daily Sales**: Revenue trends with 7-day and 30-day moving averages
  - **Country Summary**: Geographic breakdown of sales
- Multiple output formats (MySQL, PostgreSQL, SQLite, CSV, Parquet)
- Configurable and extensible pipeline class

## Tech Stack

- **Python 3.x**
- **Pandas** - Data manipulation
- **SQLAlchemy** - Database connectivity
- **MySQL** - Data warehouse
- **NumPy** - Numerical operations

## Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/sales-etl-pipeline.git
cd sales-etl-pipeline
```

2. Install dependencies:
```bash
pip install -r data/requirements.txt
```

3. Set up MySQL database:
```sql
CREATE DATABASE sales_etl;
```

4. Configure database credentials in `data/etl.py` or use environment variables.

## Usage

Run the ETL pipeline:
```bash
cd data
python etl.py
```

Or use programmatically:
```python
from etl import SalesETLPipeline

pipeline = SalesETLPipeline(
    source_path="Online Retail.xlsx",
    output_dir="output"
)

mysql_config = {
    'host': 'localhost',
    'port': 3306,
    'database': 'sales_etl',
    'user': 'root',
    'password': 'your_password'
}

summary = pipeline.run(output_formats=['mysql'], mysql_config=mysql_config)
```

## Output Tables

| Table | Records | Description |
|-------|---------|-------------|
| `transactions` | 406,789 | Cleaned sales transactions |
| `customer_summary` | 4,338 | Customer metrics and segments (Bronze/Silver/Gold/Platinum) |
| `product_summary` | 3,894 | Product sales rankings |
| `daily_sales` | 305 | Daily revenue with moving averages |
| `country_summary` | 37 | Revenue breakdown by country |

## Sample Queries

```sql
-- Top 10 customers by revenue
SELECT customerid, totalrevenue, segment
FROM customer_summary
ORDER BY totalrevenue DESC
LIMIT 10;

-- Revenue by country
SELECT country, totalrevenue, revenueshare
FROM country_summary
ORDER BY totalrevenue DESC;

-- Daily sales trend
SELECT date, revenue, revenue_7dma
FROM daily_sales
ORDER BY date DESC
LIMIT 30;
```

## Project Structure

```
sales_etl/
├── README.md
├── data/
│   ├── etl.py              # Main ETL pipeline
│   ├── requirements.txt    # Python dependencies
│   ├── Online Retail.xlsx  # Source data (not included)
│   └── output/             # Generated outputs
│       ├── csv/
│       ├── parquet/
│       └── sales_data.db   # SQLite database
```

## Dataset

This project uses the [Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/online+retail) from UCI Machine Learning Repository. The dataset contains transactions from a UK-based online retailer between 2010-2011.

## License

MIT License

## Author

Angel - Aspiring Data Engineer
