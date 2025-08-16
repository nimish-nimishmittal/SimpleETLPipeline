import sqlite3
import pandas as pd
import numpy as np
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import plotly.express as px
import plotly.graph_objects as go
from dataclasses import dataclass
# import yaml

warnings.filterwarnings('ignore')

@dataclass
class ETLConfig:
    """Configuration class for ETL pipeline"""
    db_path: str = "ecommerce_analytics.db"
    log_level: str = "INFO"
    batch_size: int = 1000
    parallel_threads: int = 4
    data_retention_days: int = 365

class ETLLogger:
    """Advanced logging system for ETL pipeline"""
    
    def __init__(self, log_level: str = "INFO"):
        self.logger = logging.getLogger("ETL_Pipeline")
        self.logger.setLevel(getattr(logging, log_level))
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def info(self, message: str): self.logger.info(message)
    def error(self, message: str): self.logger.error(message)
    def warning(self, message: str): self.logger.warning(message)
    def debug(self, message: str): self.logger.debug(message)

class DataQualityChecker:
    """Comprehensive data quality validation"""
    
    @staticmethod
    def check_data_quality(df: pd.DataFrame, table_name: str) -> Dict:
        """Perform comprehensive data quality checks"""
        quality_report = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'data_types': df.dtypes.to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'quality_score': 0
        }
        
        # Calculate quality score
        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()
        duplicate_penalty = quality_report['duplicate_rows'] * len(df.columns)
        
        quality_score = max(0, (total_cells - missing_cells - duplicate_penalty) / total_cells * 100)
        quality_report['quality_score'] = round(quality_score, 2)
        
        return quality_report

class DatabaseManager:
    """Advanced SQLite database management with optimization"""
    
    def __init__(self, db_path: str, logger: ETLLogger):
        self.db_path = db_path
        self.logger = logger
        self.lock = threading.Lock()
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database with optimized settings"""
        with sqlite3.connect(self.db_path) as conn:
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            
            # Create tables with proper indexing
            self._create_tables(conn)
    
    def _create_tables(self, conn: sqlite3.Connection):
        """Create optimized database schema"""
        
        # Customers table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT,
                registration_date DATE NOT NULL,
                country TEXT,
                city TEXT,
                total_orders INTEGER DEFAULT 0,
                total_spent REAL DEFAULT 0.0,
                customer_segment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Products table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS products (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT,
                brand TEXT,
                unit_price REAL NOT NULL CHECK(unit_price >= 0),
                cost_price REAL NOT NULL CHECK(cost_price >= 0),
                stock_quantity INTEGER DEFAULT 0,
                weight_kg REAL,
                dimensions TEXT,
                supplier_id INTEGER,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Orders table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date DATE NOT NULL,
                order_status TEXT NOT NULL,
                shipping_method TEXT,
                payment_method TEXT,
                subtotal REAL NOT NULL CHECK(subtotal >= 0),
                tax_amount REAL DEFAULT 0.0,
                shipping_cost REAL DEFAULT 0.0,
                discount_amount REAL DEFAULT 0.0,
                total_amount REAL NOT NULL CHECK(total_amount >= 0),
                shipping_address TEXT,
                order_priority TEXT DEFAULT 'Medium',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
            )
        ''')
        
        # Order items table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS order_items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity > 0),
                unit_price REAL NOT NULL CHECK(unit_price >= 0),
                discount_percent REAL DEFAULT 0.0,
                line_total REAL NOT NULL CHECK(line_total >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (order_id) REFERENCES orders (order_id),
                FOREIGN KEY (product_id) REFERENCES products (product_id)
            )
        ''')
        
        # ETL metadata table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS etl_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                source_file TEXT NOT NULL,
                records_processed INTEGER NOT NULL,
                records_inserted INTEGER NOT NULL,
                records_updated INTEGER NOT NULL,
                processing_time_seconds REAL NOT NULL,
                data_quality_score REAL,
                etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                checksum TEXT
            )
        ''')
        
        # Create performance indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)",
            "CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(customer_segment)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)",
            "CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand)",
            "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)",
            "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Tuple]:
        """Execute query with proper error handling"""
        with self.lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    return cursor.fetchall()
            except Exception as e:
                self.logger.error(f"Query execution failed: {e}")
                raise

class AdvancedETLPipeline:
    """Production-grade ETL Pipeline with comprehensive features"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = ETLLogger(config.log_level)
        self.db_manager = DatabaseManager(config.db_path, self.logger)
        self.quality_checker = DataQualityChecker()
        self.metrics = {
            'total_records_processed': 0,
            'total_processing_time': 0,
            'successful_batches': 0,
            'failed_batches': 0
        }
    
    def generate_sample_data(self):
        """Generate realistic sample datasets for demonstration"""
        self.logger.info("Generating sample datasets...")
        
        np.random.seed(42)
        
        # Generate customers data
        customers_data = self._generate_customers_data()
        customers_df = pd.DataFrame(customers_data)
        customers_df.to_csv('sample_customers.csv', index=False)
        
        # Generate products data
        products_data = self._generate_products_data()
        products_df = pd.DataFrame(products_data)
        products_df.to_csv('sample_products.csv', index=False)
        
        # Generate orders data
        orders_data = self._generate_orders_data(customers_df, products_df)
        orders_df = pd.DataFrame(orders_data)
        orders_df.to_csv('sample_orders.csv', index=False)
        
        # Generate order items data
        order_items_data = self._generate_order_items_data(orders_df, products_df)
        order_items_df = pd.DataFrame(order_items_data)
        order_items_df.to_csv('sample_order_items.csv', index=False)
        
        self.logger.info("Sample datasets generated successfully!")
        return customers_df, products_df, orders_df, order_items_df
    
    def _generate_customers_data(self) -> List[Dict]:
        """Generate realistic customer data"""
        countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'India', 'Japan']
        cities = ['New York', 'London', 'Paris', 'Toronto', 'Sydney', 'Mumbai', 'Berlin', 'Tokyo']
        segments = ['Premium', 'Regular', 'Budget', 'VIP']
        
        customers = []
        for i in range(1, 2001):  # 2000 customers
            reg_date = datetime.now() - timedelta(days=np.random.randint(1, 730))
            customers.append({
                'customer_id': i,
                'first_name': f'Customer{i}',
                'last_name': f'Lastname{i}',
                'email': f'customer{i}@email.com',
                'phone': f'+1-{np.random.randint(100, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}',
                'registration_date': reg_date.strftime('%Y-%m-%d'),
                'country': np.random.choice(countries),
                'city': np.random.choice(cities),
                'customer_segment': np.random.choice(segments, p=[0.1, 0.5, 0.3, 0.1])
            })
        return customers
    
    def _generate_products_data(self) -> List[Dict]:
        """Generate realistic product data"""
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Beauty']
        brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
        
        products = []
        for i in range(1, 501):  # 500 products
            category = np.random.choice(categories)
            cost = np.random.uniform(10, 200)
            margin = np.random.uniform(1.2, 3.0)
            
            products.append({
                'product_id': i,
                'product_name': f'{category} Product {i}',
                'category': category,
                'subcategory': f'{category} Sub{np.random.randint(1, 5)}',
                'brand': np.random.choice(brands),
                'unit_price': round(cost * margin, 2),
                'cost_price': round(cost, 2),
                'stock_quantity': np.random.randint(0, 1000),
                'weight_kg': round(np.random.uniform(0.1, 5.0), 2),
                'supplier_id': np.random.randint(1, 21)
            })
        return products
    
    def _generate_orders_data(self, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> List[Dict]:
        """Generate realistic orders data"""
        statuses = ['Completed', 'Processing', 'Shipped', 'Cancelled', 'Returned']
        shipping_methods = ['Standard', 'Express', 'Overnight', 'Economy']
        payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Digital Wallet']
        priorities = ['Low', 'Medium', 'High', 'Urgent']
        
        orders = []
        order_id = 1
        
        for customer_id in customers_df['customer_id'].sample(1500):  # 1500 orders
            order_date = datetime.now() - timedelta(days=np.random.randint(1, 365))
            subtotal = np.random.uniform(50, 1000)
            tax_rate = 0.08
            shipping_cost = np.random.uniform(5, 25)
            discount = np.random.uniform(0, subtotal * 0.2)
            
            orders.append({
                'order_id': order_id,
                'customer_id': customer_id,
                'order_date': order_date.strftime('%Y-%m-%d'),
                'order_status': np.random.choice(statuses, p=[0.7, 0.1, 0.1, 0.05, 0.05]),
                'shipping_method': np.random.choice(shipping_methods),
                'payment_method': np.random.choice(payment_methods),
                'subtotal': round(subtotal, 2),
                'tax_amount': round(subtotal * tax_rate, 2),
                'shipping_cost': round(shipping_cost, 2),
                'discount_amount': round(discount, 2),
                'total_amount': round(subtotal + subtotal * tax_rate + shipping_cost - discount, 2),
                'shipping_address': f'Address {order_id}, City, Country',
                'order_priority': np.random.choice(priorities, p=[0.4, 0.4, 0.15, 0.05])
            })
            order_id += 1
        
        return orders
    
    def _generate_order_items_data(self, orders_df: pd.DataFrame, products_df: pd.DataFrame) -> List[Dict]:
        """Generate realistic order items data"""
        order_items = []
        item_id = 1
        
        for _, order in orders_df.iterrows():
            items_count = np.random.randint(1, 6)  # 1-5 items per order
            selected_products = products_df.sample(items_count)
            
            for _, product in selected_products.iterrows():
                quantity = np.random.randint(1, 4)
                discount_percent = np.random.uniform(0, 20)
                unit_price = product['unit_price']
                line_total = quantity * unit_price * (1 - discount_percent / 100)
                
                order_items.append({
                    'item_id': item_id,
                    'order_id': order['order_id'],
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'discount_percent': round(discount_percent, 2),
                    'line_total': round(line_total, 2)
                })
                item_id += 1
        
        return order_items
    
    def extract_data(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV with error handling and validation"""
        try:
            self.logger.info(f"Extracting data from {file_path}")
            
            # Read CSV with error handling
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Basic validation
            if df.empty:
                raise ValueError(f"File {file_path} is empty")
            
            self.logger.info(f"Successfully extracted {len(df)} records from {file_path}")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"File is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error extracting data from {file_path}: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Advanced data transformation with comprehensive cleaning"""
        self.logger.info(f"Transforming data for {table_name}")
        
        # Create a copy to avoid modifying original
        transformed_df = df.copy()
        
        # Common transformations
        transformed_df = self._clean_basic_data(transformed_df)
        
        # Table-specific transformations
        if table_name == 'customers':
            transformed_df = self._transform_customers(transformed_df)
        elif table_name == 'products':
            transformed_df = self._transform_products(transformed_df)
        elif table_name == 'orders':
            transformed_df = self._transform_orders(transformed_df)
        elif table_name == 'order_items':
            transformed_df = self._transform_order_items(transformed_df)
        
        self.logger.info(f"Transformation completed for {table_name}")
        return transformed_df
    
    def _clean_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning operations"""
        # Remove leading/trailing whitespace from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        return df
    
    def _transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Customer-specific transformations"""
        # Validate email format (basic)
        df = df[df['email'].str.contains('@', na=False)]
        
        # Standardize country names
        country_mapping = {
            'US': 'USA', 'United States': 'USA',
            'UK': 'United Kingdom', 'Britain': 'United Kingdom'
        }
        df['country'] = df['country'].replace(country_mapping)
        
        # Convert registration_date to proper format
        df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
        
        # Initialize calculated fields
        df['total_orders'] = 0
        df['total_spent'] = 0.0
        
        return df
    
    def _transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Product-specific transformations"""
        # Ensure prices are positive
        df = df[df['unit_price'] > 0]
        df = df[df['cost_price'] > 0]
        
        # Calculate profit margin
        df['profit_margin'] = ((df['unit_price'] - df['cost_price']) / df['unit_price'] * 100).round(2)
        
        # Ensure stock quantity is not negative
        df.loc[df['stock_quantity'] < 0, 'stock_quantity'] = 0
        
        return df
    
    def _transform_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Order-specific transformations"""
        # Convert order_date to proper format
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
        
        # Ensure amounts are positive
        numeric_columns = ['subtotal', 'tax_amount', 'shipping_cost', 'total_amount']
        for col in numeric_columns:
            df = df[df[col] >= 0]
        
        # Validate total_amount calculation
        calculated_total = df['subtotal'] + df['tax_amount'] + df['shipping_cost'] - df['discount_amount']
        df = df[abs(df['total_amount'] - calculated_total) < 0.01]  # Allow small rounding differences
        
        return df
    
    def _transform_order_items(self, df: pd.DataFrame) -> pd.DataFrame:
        """Order items specific transformations"""
        # Ensure quantity and prices are positive
        df = df[df['quantity'] > 0]
        df = df[df['unit_price'] >= 0]
        df = df[df['line_total'] >= 0]
        
        # Validate line_total calculation
        calculated_total = df['quantity'] * df['unit_price'] * (1 - df['discount_percent'] / 100)
        df = df[abs(df['line_total'] - calculated_total) < 0.01]
        
        return df
    
    def load_data(self, df: pd.DataFrame, table_name: str, source_file: str) -> Dict:
        """Load data with comprehensive error handling and metadata tracking"""
        start_time = time.time()
        self.logger.info(f"Loading {len(df)} records into {table_name}")
        
        # Generate data checksum for integrity
        checksum = hashlib.md5(df.to_string().encode()).hexdigest()
        
        # Check data quality
        quality_report = self.quality_checker.check_data_quality(df, table_name)
        
        records_inserted = 0
        records_updated = 0
        
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                # Use batch processing for better performance
                for i in range(0, len(df), self.config.batch_size):
                    batch_df = df.iloc[i:i + self.config.batch_size]
                    
                    if table_name in ['customers', 'products', 'orders']:
                        # Use INSERT OR REPLACE for tables with primary keys
                        batch_df.to_sql(table_name, conn, if_exists='append', 
                                       index=False, method='multi')
                        records_inserted += len(batch_df)
                    else:
                        # Use regular insert for detail tables
                        batch_df.to_sql(table_name, conn, if_exists='append', 
                                       index=False, method='multi')
                        records_inserted += len(batch_df)
                
                # Update ETL metadata
                processing_time = time.time() - start_time
                metadata = {
                    'table_name': table_name,
                    'source_file': source_file,
                    'records_processed': len(df),
                    'records_inserted': records_inserted,
                    'records_updated': records_updated,
                    'processing_time_seconds': round(processing_time, 2),
                    'data_quality_score': quality_report['quality_score'],
                    'checksum': checksum
                }
                
                # Insert metadata
                conn.execute('''
                    INSERT INTO etl_metadata 
                    (table_name, source_file, records_processed, records_inserted, 
                     records_updated, processing_time_seconds, data_quality_score, checksum)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', tuple(metadata.values()))
                
                self.logger.info(f"Successfully loaded {records_inserted} records into {table_name}")
                return metadata
                
        except Exception as e:
            self.logger.error(f"Error loading data into {table_name}: {e}")
            raise
    
    def run_full_pipeline(self):
        """Execute the complete ETL pipeline"""
        self.logger.info("Starting ETL Pipeline execution...")
        pipeline_start = time.time()
        
        try:
            # Generate sample data
            sample_data = self.generate_sample_data()
            
            # Define processing order (respecting foreign key constraints)
            processing_order = [
                ('sample_customers.csv', 'customers'),
                ('sample_products.csv', 'products'),
                ('sample_orders.csv', 'orders'),
                ('sample_order_items.csv', 'order_items')
            ]
            
            # Process each table
            for file_name, table_name in processing_order:
                try:
                    # Extract
                    raw_data = self.extract_data(file_name)
                    
                    # Transform
                    clean_data = self.transform_data(raw_data, table_name)
                    
                    # Load
                    load_metadata = self.load_data(clean_data, table_name, file_name)
                    
                    # Update metrics
                    self.metrics['total_records_processed'] += load_metadata['records_processed']
                    self.metrics['successful_batches'] += 1
                    
                except Exception as e:
                    self.logger.error(f"Pipeline failed for {table_name}: {e}")
                    self.metrics['failed_batches'] += 1
                    continue
            
            # Post-processing: Update calculated fields
            self._update_calculated_fields()
            
            # Generate pipeline summary
            pipeline_time = time.time() - pipeline_start
            self.metrics['total_processing_time'] = round(pipeline_time, 2)
            
            self.logger.info("ETL Pipeline completed successfully!")
            self._print_pipeline_summary()
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def _update_calculated_fields(self):
        """Update calculated fields like customer totals"""
        self.logger.info("Updating calculated fields...")
        
        with sqlite3.connect(self.config.db_path) as conn:
            # Update customer statistics
            conn.execute('''
                UPDATE customers 
                SET (total_orders, total_spent) = (
                    SELECT 
                        COUNT(o.order_id),
                        COALESCE(SUM(o.total_amount), 0)
                    FROM orders o 
                    WHERE o.customer_id = customers.customer_id
                        AND o.order_status = 'Completed'
                )
            ''')
    
    def _print_pipeline_summary(self):
        """Print comprehensive pipeline execution summary"""
        print("\n" + "="*60)
        print("ETL PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Total Records Processed: {self.metrics['total_records_processed']:,}")
        print(f"Total Processing Time: {self.metrics['total_processing_time']:.2f} seconds")
        print(f"Successful Batches: {self.metrics['successful_batches']}")
        print(f"Failed Batches: {self.metrics['failed_batches']}")
        print(f"Records per Second: {self.metrics['total_records_processed']/self.metrics['total_processing_time']:.2f}")
        print("="*60)
    
    def generate_analytics_report(self):
        """Generate comprehensive business analytics report"""
        self.logger.info("Generating analytics report...")
        
        with sqlite3.connect(self.config.db_path) as conn:
            # Sales analytics
            sales_by_month = pd.read_sql_query('''
                SELECT 
                    strftime('%Y-%m', order_date) as month,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_revenue
                FROM orders 
                WHERE order_status = 'Completed'
                GROUP BY strftime('%Y-%m', order_date)
                ORDER BY month
            ''', conn)
            
            # Customer analytics
            customer_segments = pd.read_sql_query('''
                SELECT 
                    customer_segment,
                    COUNT(*) as customer_count,
                    AVG(total_spent) as avg_spent,
                    SUM(total_spent) as total_revenue
                FROM customers
                GROUP BY customer_segment
                ORDER BY total_revenue DESC
            ''', conn)
            
            # Product analytics
            top_products = pd.read_sql_query('''
                SELECT 
                    p.product_name,
                    p.category,
                    SUM(oi.quantity) as total_sold,
                    SUM(oi.line_total) as total_revenue
                FROM products p
                JOIN order_items oi ON p.product_id = oi.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_status = 'Completed'
                GROUP BY p.product_id
                ORDER BY total_revenue DESC
                LIMIT 10
            ''', conn)
            
            return {
                'sales_by_month': sales_by_month,
                'customer_segments': customer_segments,
                'top_products': top_products
            }
    
    def create_dashboard_visualizations(self):
        """Create interactive visualizations for the dashboard"""
        analytics_data = self.generate_analytics_report()
        
        # Sales trend visualization
        fig_sales = px.line(
            analytics_data['sales_by_month'], 
            x='month', y='total_revenue',
            title='Monthly Sales Trend',
            labels={'total_revenue': 'Revenue ($)', 'month': 'Month'}
        )
        
        # Customer segment analysis
        fig_segments = px.pie(
            analytics_data['customer_segments'],
            values='total_revenue', names='customer_segment',
            title='Revenue by Customer Segment'
        )
        
        # Top products analysis
        fig_products = px.bar(
            analytics_data['top_products'],
            x='product_name', y='total_revenue',
            title='Top 10 Products by Revenue',
            labels={'total_revenue': 'Revenue ($)', 'product_name': 'Product'}
        )
        fig_products.update_xaxes(tickangle=45)
        
        return {
            'sales_trend': fig_sales,
            'customer_segments': fig_segments,
            'top_products': fig_products
        }

def main():
    """Main execution function"""
    print(" Advanced ETL Pipeline for E-commerce Analytics")
    print("=" * 50)
    
    # Initialize configuration
    config = ETLConfig(
        db_path="ecommerce_analytics.db",
        log_level="INFO",
        batch_size=1000,
        parallel_threads=4
    )
    
    # Initialize ETL pipeline
    etl_pipeline = AdvancedETLPipeline(config)
    
    try:
        # Execute full ETL pipeline
        etl_pipeline.run_full_pipeline()
        
        # Generate analytics report
        print("\n Generating Analytics Report...")
        analytics_data = etl_pipeline.generate_analytics_report()
        
        # Display key insights
        print("\n KEY BUSINESS INSIGHTS:")
        print("-" * 30)
        
        # Sales insights
        total_revenue = analytics_data['sales_by_month']['total_revenue'].sum()
        total_orders = analytics_data['sales_by_month']['total_orders'].sum()
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        
        print(f" Total Revenue: ${total_revenue:,.2f}")
        print(f" Total Orders: {total_orders:,}")
        print(f" Average Order Value: ${avg_order_value:.2f}")
        
        # Customer insights
        top_segment = analytics_data['customer_segments'].iloc[0]
        print(f" Top Customer Segment: {top_segment['customer_segment']}")
        print(f" Total Customers: {analytics_data['customer_segments']['customer_count'].sum():,}")
        
        # Product insights
        if not analytics_data['top_products'].empty:
            top_product = analytics_data['top_products'].iloc[0]
            print(f" Best Selling Product: {top_product['product_name']}")
            print(f" Top Product Revenue: ${top_product['total_revenue']:,.2f}")
        
        # Create visualizations
        print("\nCreating Dashboard Visualizations...")
        visualizations = etl_pipeline.create_dashboard_visualizations()
        
        # Save visualizations as HTML files
        visualizations['sales_trend'].write_html("sales_trend_dashboard.html")
        visualizations['customer_segments'].write_html("customer_segments_dashboard.html")
        visualizations['top_products'].write_html("top_products_dashboard.html")
        
        print(" Dashboard files saved:")
        print("   - sales_trend_dashboard.html")
        print("   - customer_segments_dashboard.html")
        print("   - top_products_dashboard.html")
        
        # Data quality summary
        print("\n DATA QUALITY SUMMARY:")
        print("-" * 30)
        with sqlite3.connect(config.db_path) as conn:
            quality_summary = pd.read_sql_query('''
                SELECT 
                    table_name,
                    AVG(data_quality_score) as avg_quality_score,
                    SUM(records_processed) as total_records,
                    MAX(etl_timestamp) as last_updated
                FROM etl_metadata
                GROUP BY table_name
                ORDER BY avg_quality_score DESC
            ''', conn)
            
            for _, row in quality_summary.iterrows():
                print(f"📋 {row['table_name']}: {row['avg_quality_score']:.1f}% quality, {row['total_records']:,} records")
        
        print("\n✨ ETL Pipeline execution completed successfully!")
        print(" Your project is ready!")
        
    except Exception as e:
        print(f" Pipeline execution failed: {e}")
        raise

class ETLMonitoringDashboard:
    """Real-time monitoring dashboard for ETL pipeline"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status and metrics"""
        with sqlite3.connect(self.db_path) as conn:
            # Latest ETL runs
            latest_runs = pd.read_sql_query('''
                SELECT 
                    table_name,
                    records_processed,
                    data_quality_score,
                    processing_time_seconds,
                    etl_timestamp
                FROM etl_metadata
                WHERE etl_timestamp >= datetime('now', '-24 hours')
                ORDER BY etl_timestamp DESC
            ''', conn)
            
            # Performance metrics
            performance_metrics = pd.read_sql_query('''
                SELECT 
                    AVG(processing_time_seconds) as avg_processing_time,
                    SUM(records_processed) as total_records_24h,
                    AVG(data_quality_score) as avg_quality_score,
                    COUNT(*) as total_runs_24h
                FROM etl_metadata
                WHERE etl_timestamp >= datetime('now', '-24 hours')
            ''', conn)
            
            return {
                'latest_runs': latest_runs,
                'performance_metrics': performance_metrics,
                'status': 'Healthy' if len(latest_runs) > 0 else 'No Recent Activity'
            }
    
    def generate_monitoring_report(self):
        """Generate comprehensive monitoring report"""
        status_data = self.get_pipeline_status()
        
        print("\n ETL PIPELINE MONITORING REPORT")
        print("=" * 40)
        print(f" Pipeline Status: {status_data['status']}")
        
        if not status_data['performance_metrics'].empty:
            metrics = status_data['performance_metrics'].iloc[0]
            print(f" Average Processing Time: {metrics['avg_processing_time']:.2f}s")
            print(f" Records Processed (24h): {int(metrics['total_records_24h']):,}")
            print(f" Average Quality Score: {metrics['avg_quality_score']:.1f}%")
            print(f" Pipeline Runs (24h): {int(metrics['total_runs_24h'])}")
        
        if not status_data['latest_runs'].empty:
            print("\n Latest ETL Runs:")
            for _, run in status_data['latest_runs'].head().iterrows():
                print(f"   {run['table_name']}: {run['records_processed']} records, "
                      f"{run['data_quality_score']:.1f}% quality")

class DataValidationFramework:
    """Comprehensive data validation framework"""
    
    @staticmethod
    def validate_business_rules(df: pd.DataFrame, table_name: str) -> List[str]:
        """Validate business-specific rules"""
        validation_errors = []
        
        if table_name == 'orders':
            # Business rule: Order total should match sum of components
            invalid_totals = df[
                abs(df['total_amount'] - (df['subtotal'] + df['tax_amount'] + 
                df['shipping_cost'] - df['discount_amount'])) > 0.01
            ]
            if not invalid_totals.empty:
                validation_errors.append(f"Found {len(invalid_totals)} orders with invalid total calculations")
            
            # Business rule: Future orders not allowed
            future_orders = df[pd.to_datetime(df['order_date']) > datetime.now()]
            if not future_orders.empty:
                validation_errors.append(f"Found {len(future_orders)} orders with future dates")
        
        elif table_name == 'products':
            # Business rule: Selling price should be higher than cost
            invalid_pricing = df[df['unit_price'] <= df['cost_price']]
            if not invalid_pricing.empty:
                validation_errors.append(f"Found {len(invalid_pricing)} products with invalid pricing")
        
        elif table_name == 'customers':
            # Business rule: Email format validation
            invalid_emails = df[~df['email'].str.contains('@', na=False)]
            if not invalid_emails.empty:
                validation_errors.append(f"Found {len(invalid_emails)} customers with invalid email formats")
        
        return validation_errors

# Additional utility functions for advanced features
class ETLOptimizer:
    """Performance optimization utilities for ETL pipeline"""
    
    @staticmethod
    def analyze_query_performance(db_path: str):
        """Analyze database query performance"""
        with sqlite3.connect(db_path) as conn:
            conn.execute("ANALYZE")
            
            # Check index usage
            index_info = conn.execute('''
                SELECT name, sql FROM sqlite_master 
                WHERE type='index' AND sql IS NOT NULL
            ''').fetchall()
            
            print(" Database Optimization Report:")
            print(f" Active Indexes: {len(index_info)}")
            
            # Table sizes
            tables = ['customers', 'products', 'orders', 'order_items']
            for table in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f" {table}: {count:,} records")
    
    @staticmethod
    def suggest_optimizations(etl_pipeline):
        """Suggest performance optimizations"""
        suggestions = [
            " Consider partitioning large tables by date",
            " Implement incremental loading for large datasets",
            " Use connection pooling for high-frequency operations",
            " Add data archiving for historical records",
            "⚡ Consider using UPSERT operations for better performance"
        ]
        
        print("\n OPTIMIZATION SUGGESTIONS:")
        for suggestion in suggestions:
            print(f"   {suggestion}")

if __name__ == "__main__":
    main()
    
    # Additional monitoring and optimization
    print("\n" + "="*50)
    print(" ADVANCED FEATURES DEMONSTRATION")
    print("="*50)
    
    # Performance analysis
    optimizer = ETLOptimizer()
    optimizer.analyze_query_performance("ecommerce_analytics.db")
    optimizer.suggest_optimizations(None)
    
    # Monitoring dashboard
    monitor = ETLMonitoringDashboard("ecommerce_analytics.db")
    monitor.generate_monitoring_report()
    
