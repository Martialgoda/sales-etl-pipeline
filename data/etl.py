"""
Sales ETL Pipeline
==================
Extract, Transform, and Load pipeline for Online Retail data.

This pipeline:
1. Extracts data from Excel source
2. Cleans and validates the data
3. Creates analytical transformations
4. Loads to various output formats (CSV, Parquet, SQLite)
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sqlite3
import logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SalesETLPipeline:
    """ETL Pipeline for Online Retail sales data."""

    def __init__(self, source_path: str, output_dir: str = "output"):
        self.source_path = Path(source_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.raw_data = None
        self.cleaned_data = None
        self.transformed_data = {}

    # ==================== EXTRACT ====================

    def extract(self) -> pd.DataFrame:
        """Extract data from Excel source file."""
        logger.info(f"Extracting data from {self.source_path}")

        if not self.source_path.exists():
            raise FileNotFoundError(f"Source file not found: {self.source_path}")

        self.raw_data = pd.read_excel(self.source_path)
        logger.info(f"Extracted {len(self.raw_data):,} records with {len(self.raw_data.columns)} columns")

        return self.raw_data

    # ==================== TRANSFORM ====================

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the raw data."""
        logger.info("Starting data cleaning...")

        df = df.copy()
        initial_count = len(df)

        # 1. Remove records with missing CustomerID (can't attribute sales)
        df = df.dropna(subset=['CustomerID'])
        logger.info(f"Removed {initial_count - len(df):,} records with missing CustomerID")

        # 2. Remove records with missing Description
        df = df.dropna(subset=['Description'])

        # 3. Convert CustomerID to integer
        df['CustomerID'] = df['CustomerID'].astype(int)

        # 4. Identify cancelled orders (InvoiceNo starts with 'C')
        df['IsCancelled'] = df['InvoiceNo'].astype(str).str.startswith('C')

        # 5. Filter out invalid quantities and prices for non-cancelled orders
        valid_mask = (
            (df['IsCancelled']) |  # Keep cancelled orders as-is
            ((df['Quantity'] > 0) & (df['UnitPrice'] > 0))  # Valid sales
        )
        removed_invalid = len(df) - valid_mask.sum()
        df = df[valid_mask]
        logger.info(f"Removed {removed_invalid:,} records with invalid quantity/price")

        # 6. Calculate total amount
        df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

        # 7. Extract date components
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df['Year'] = df['InvoiceDate'].dt.year
        df['Month'] = df['InvoiceDate'].dt.month
        df['DayOfWeek'] = df['InvoiceDate'].dt.dayofweek
        df['Hour'] = df['InvoiceDate'].dt.hour
        df['Date'] = df['InvoiceDate'].dt.date

        # 8. Clean description text
        df['Description'] = df['Description'].str.strip().str.upper()

        # 9. Standardize country names
        df['Country'] = df['Country'].str.strip().str.title()

        logger.info(f"Cleaning complete. {len(df):,} records remaining ({len(df)/initial_count*100:.1f}%)")

        self.cleaned_data = df
        return df

    def create_customer_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create customer-level aggregations."""
        logger.info("Creating customer summary...")

        # Filter to non-cancelled orders only
        sales_df = df[~df['IsCancelled']]

        customer_summary = sales_df.groupby('CustomerID').agg({
            'InvoiceNo': 'nunique',
            'TotalAmount': 'sum',
            'Quantity': 'sum',
            'InvoiceDate': ['min', 'max'],
            'Country': 'first'
        }).reset_index()

        # Flatten column names
        customer_summary.columns = [
            'CustomerID', 'TotalOrders', 'TotalRevenue',
            'TotalItems', 'FirstPurchase', 'LastPurchase', 'Country'
        ]

        # Calculate metrics
        customer_summary['AvgOrderValue'] = (
            customer_summary['TotalRevenue'] / customer_summary['TotalOrders']
        )
        customer_summary['DaysSinceFirstPurchase'] = (
            customer_summary['LastPurchase'] - customer_summary['FirstPurchase']
        ).dt.days

        # Customer segments based on revenue
        customer_summary['Segment'] = pd.qcut(
            customer_summary['TotalRevenue'],
            q=4,
            labels=['Bronze', 'Silver', 'Gold', 'Platinum']
        )

        logger.info(f"Created summary for {len(customer_summary):,} customers")
        self.transformed_data['customer_summary'] = customer_summary

        return customer_summary

    def create_product_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create product-level aggregations."""
        logger.info("Creating product summary...")

        sales_df = df[~df['IsCancelled']]

        product_summary = sales_df.groupby(['StockCode', 'Description']).agg({
            'Quantity': 'sum',
            'TotalAmount': 'sum',
            'InvoiceNo': 'nunique',
            'CustomerID': 'nunique',
            'UnitPrice': 'mean'
        }).reset_index()

        product_summary.columns = [
            'StockCode', 'Description', 'TotalQuantitySold',
            'TotalRevenue', 'OrderCount', 'UniqueCustomers', 'AvgUnitPrice'
        ]

        # Sort by revenue
        product_summary = product_summary.sort_values('TotalRevenue', ascending=False)

        logger.info(f"Created summary for {len(product_summary):,} products")
        self.transformed_data['product_summary'] = product_summary

        return product_summary

    def create_daily_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create daily sales aggregations."""
        logger.info("Creating daily sales summary...")

        sales_df = df[~df['IsCancelled']]

        daily_sales = sales_df.groupby('Date').agg({
            'TotalAmount': 'sum',
            'InvoiceNo': 'nunique',
            'CustomerID': 'nunique',
            'Quantity': 'sum'
        }).reset_index()

        daily_sales.columns = [
            'Date', 'Revenue', 'Orders', 'UniqueCustomers', 'ItemsSold'
        ]

        daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])
        daily_sales = daily_sales.sort_values('Date')

        # Add moving averages
        daily_sales['Revenue_7DMA'] = daily_sales['Revenue'].rolling(7).mean()
        daily_sales['Revenue_30DMA'] = daily_sales['Revenue'].rolling(30).mean()

        logger.info(f"Created {len(daily_sales):,} daily records")
        self.transformed_data['daily_sales'] = daily_sales

        return daily_sales

    def create_country_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create country-level aggregations."""
        logger.info("Creating country summary...")

        sales_df = df[~df['IsCancelled']]

        country_summary = sales_df.groupby('Country').agg({
            'TotalAmount': 'sum',
            'InvoiceNo': 'nunique',
            'CustomerID': 'nunique',
            'Quantity': 'sum'
        }).reset_index()

        country_summary.columns = [
            'Country', 'TotalRevenue', 'TotalOrders',
            'UniqueCustomers', 'TotalItems'
        ]

        country_summary['AvgOrderValue'] = (
            country_summary['TotalRevenue'] / country_summary['TotalOrders']
        )
        country_summary['RevenueShare'] = (
            country_summary['TotalRevenue'] / country_summary['TotalRevenue'].sum() * 100
        )

        country_summary = country_summary.sort_values('TotalRevenue', ascending=False)

        logger.info(f"Created summary for {len(country_summary):,} countries")
        self.transformed_data['country_summary'] = country_summary

        return country_summary

    def transform(self) -> dict:
        """Run all transformations."""
        if self.raw_data is None:
            raise ValueError("No data extracted. Run extract() first.")

        logger.info("=" * 50)
        logger.info("Starting transformation phase")
        logger.info("=" * 50)

        # Clean data
        cleaned = self.clean_data(self.raw_data)

        # Create summaries
        self.create_customer_summary(cleaned)
        self.create_product_summary(cleaned)
        self.create_daily_sales(cleaned)
        self.create_country_summary(cleaned)

        # Store cleaned transactions
        self.transformed_data['transactions'] = cleaned

        logger.info("Transformation complete!")
        return self.transformed_data

    # ==================== LOAD ====================

    def load_to_csv(self) -> None:
        """Load all transformed data to CSV files."""
        logger.info("Loading data to CSV files...")

        csv_dir = self.output_dir / "csv"
        csv_dir.mkdir(exist_ok=True)

        for name, df in self.transformed_data.items():
            filepath = csv_dir / f"{name}.csv"
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {name} to {filepath}")

    def load_to_parquet(self) -> None:
        """Load all transformed data to Parquet files."""
        logger.info("Loading data to Parquet files...")

        parquet_dir = self.output_dir / "parquet"
        parquet_dir.mkdir(exist_ok=True)

        for name, df in self.transformed_data.items():
            filepath = parquet_dir / f"{name}.parquet"
            # Convert object columns to string to avoid type inference issues
            df_copy = df.copy()
            for col in df_copy.select_dtypes(include=['object']).columns:
                df_copy[col] = df_copy[col].astype(str)
            df_copy.to_parquet(filepath, index=False)
            logger.info(f"Saved {name} to {filepath}")

    def load_to_sqlite(self, db_name: str = "sales_data.db") -> None:
        """Load all transformed data to SQLite database."""
        logger.info("Loading data to SQLite database...")

        db_path = self.output_dir / db_name
        conn = sqlite3.connect(db_path)

        for name, df in self.transformed_data.items():
            df.to_sql(name, conn, if_exists='replace', index=False)
            logger.info(f"Loaded {name} table to {db_path}")

        conn.close()

    def load_to_postgres(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "sales_etl",
        user: str = "postgres",
        password: str = ""
    ) -> None:
        """Load all transformed data to PostgreSQL database."""
        logger.info(f"Loading data to PostgreSQL ({host}:{port}/{database})...")

        # Build connection string (URL-encode password for special characters)
        if password:
            encoded_password = quote_plus(password)
            conn_str = f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}"
        else:
            conn_str = f"postgresql://{user}@{host}:{port}/{database}"

        engine = create_engine(conn_str)

        for name, df in self.transformed_data.items():
            # Convert column names to lowercase for PostgreSQL convention
            df_copy = df.copy()
            df_copy.columns = [col.lower() for col in df_copy.columns]
            df_copy.to_sql(name, engine, if_exists='replace', index=False)
            logger.info(f"Loaded {name} table to PostgreSQL")

        engine.dispose()
        logger.info("PostgreSQL load complete!")

    def load_to_mysql(
        self,
        host: str = "localhost",
        port: int = 3306,
        database: str = "sales_etl",
        user: str = "root",
        password: str = ""
    ) -> None:
        """Load all transformed data to MySQL database."""
        logger.info(f"Loading data to MySQL ({host}:{port}/{database})...")

        # Build connection string (URL-encode password for special characters)
        encoded_password = quote_plus(password) if password else ""
        conn_str = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}"

        engine = create_engine(conn_str)

        for name, df in self.transformed_data.items():
            df_copy = df.copy()
            df_copy.columns = [col.lower() for col in df_copy.columns]
            df_copy.to_sql(name, engine, if_exists='replace', index=False)
            logger.info(f"Loaded {name} table to MySQL")

        engine.dispose()
        logger.info("MySQL load complete!")

    def load(self, formats: list = None, pg_config: dict = None, mysql_config: dict = None) -> None:
        """Load data to specified formats."""
        if formats is None:
            formats = ['csv', 'parquet', 'sqlite']

        logger.info("=" * 50)
        logger.info("Starting load phase")
        logger.info("=" * 50)

        if 'csv' in formats:
            self.load_to_csv()
        if 'parquet' in formats:
            self.load_to_parquet()
        if 'sqlite' in formats:
            self.load_to_sqlite()
        if 'postgres' in formats:
            config = pg_config or {}
            self.load_to_postgres(**config)
        if 'mysql' in formats:
            config = mysql_config or {}
            self.load_to_mysql(**config)

        logger.info("Load complete!")

    # ==================== RUN PIPELINE ====================

    def run(self, output_formats: list = None, pg_config: dict = None, mysql_config: dict = None) -> dict:
        """Run the complete ETL pipeline."""
        logger.info("=" * 60)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 60)

        start_time = datetime.now()

        # Extract
        self.extract()

        # Transform
        self.transform()

        # Load
        self.load(formats=output_formats, pg_config=pg_config, mysql_config=mysql_config)

        elapsed = datetime.now() - start_time

        logger.info("=" * 60)
        logger.info(f"PIPELINE COMPLETE - Elapsed time: {elapsed}")
        logger.info("=" * 60)

        return self.get_summary()

    def get_summary(self) -> dict:
        """Get a summary of the pipeline results."""
        return {
            'raw_records': len(self.raw_data) if self.raw_data is not None else 0,
            'cleaned_records': len(self.cleaned_data) if self.cleaned_data is not None else 0,
            'datasets_created': list(self.transformed_data.keys()),
            'output_directory': str(self.output_dir)
        }


def main():
    """Main entry point for the ETL pipeline."""
    # Initialize pipeline
    pipeline = SalesETLPipeline(
        source_path="Online Retail.xlsx",
        output_dir="output"
    )

    # MySQL configuration (use environment variables for security)
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', 3306)),
        'database': os.getenv('MYSQL_DATABASE', 'sales_etl'),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', '')
    }

    # Run the pipeline - loading to MySQL
    summary = pipeline.run(output_formats=['mysql'], mysql_config=mysql_config)

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Raw records processed: {summary['raw_records']:,}")
    print(f"Cleaned records: {summary['cleaned_records']:,}")
    print(f"Datasets created: {', '.join(summary['datasets_created'])}")
    print(f"Output directory: {summary['output_directory']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
