#!/usr/bin/env python3
"""
Generate Sample Transaction Data
================================
Creates sample transaction data using Spark for scalable generation.
Supports generating 100M+ records without loading everything into memory.

Usage:
    python examples/generate_sample_data.py
    
    # Or with custom parameters:
    python examples/generate_sample_data.py --num-records 100000000 --output data/sample.parquet
"""

import argparse
import csv
import random
import string
import os
import math
from datetime import date, timedelta
from typing import List, Tuple, Optional
from pathlib import Path


# Sample MCC codes (Merchant Category Codes)
MCC_CODES = [
    '5411',  # Grocery Stores
    '5412',  # Convenience Stores
    '5541',  # Gas Stations
    '5542',  # Fuel Dispensers
    '5812',  # Restaurants
    '5813',  # Bars/Nightclubs
    '5814',  # Fast Food
    '5912',  # Drug Stores
    '5921',  # Package Stores (Alcohol)
    '5941',  # Sporting Goods
    '5942',  # Book Stores
    '5945',  # Toy Stores
    '5946',  # Camera Stores
    '5947',  # Gift Shops
    '5948',  # Luggage Stores
    '5949',  # Fabric Stores
    '5969',  # Direct Marketing
    '5977',  # Cosmetic Stores
    '5992',  # Florists
    '5993',  # Cigar Stores
    '5994',  # News Dealers
    '5995',  # Pet Shops
    '5996',  # Swimming Pools
    '5997',  # Electric Razor Stores
    '5998',  # Tent Stores
    '5999',  # Miscellaneous Retail
    '7011',  # Hotels/Motels
    '7032',  # Recreational Camps
    '7033',  # Campgrounds
    '7210',  # Laundry Services
    '7211',  # Laundries
    '7216',  # Dry Cleaners
    '7230',  # Beauty Shops
    '7251',  # Shoe Repair
    '7261',  # Funeral Services
    '7273',  # Dating Services
    '7276',  # Tax Preparation
    '7277',  # Counseling Services
    '7278',  # Buying Clubs
    '7296',  # Clothing Rental
    '7297',  # Massage Parlors
    '7298',  # Health Spas
    '7299',  # Miscellaneous Recreation
    '7311',  # Advertising Services
    '7333',  # Commercial Art
    '7338',  # Quick Copy
    '7339',  # Secretarial Services
    '7342',  # Exterminating Services
    '7349',  # Cleaning Services
    '7361',  # Employment Agencies
]


def load_cities_from_csv(csv_path: str) -> List[str]:
    """Load city names from city_province.csv."""
    cities = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Handle potential BOM or whitespace in column names
            row = {k.strip().lstrip('\ufeff'): v for k, v in row.items()}
            cities.append(row['CityName'].strip())
    return list(set(cities))  # Unique cities


def generate_card_number() -> str:
    """Generate a random 16-digit card number."""
    return ''.join(random.choices(string.digits, k=16))


def generate_acceptor_id() -> str:
    """Generate a random acceptor ID."""
    return f"ACC{''.join(random.choices(string.digits, k=8))}"


def generate_transaction_id() -> str:
    """Generate a random transaction ID."""
    return f"TXN{''.join(random.choices(string.ascii_uppercase + string.digits, k=12))}"


def generate_amount(mcc: str = None) -> float:
    """
    Generate a random transaction amount with high variance based on MCC.
    
    Uses MCC-specific distributions to simulate real transaction patterns:
    - Grocery stores: Small-medium amounts with high variance
    - Restaurants: Medium amounts with moderate variance
    - Gas stations: Medium amounts with low variance
    - Hotels: Large amounts with very high variance
    - Car dealers: Very large amounts with extremely high variance
    
    Args:
        mcc: Merchant Category Code (optional, for MCC-specific generation)
    """
    # Define MCC-specific base amounts and variance multipliers
    # IMPORTANT: Each MCC should have VERY different average amounts to ensure
    # stratified sensitivity works correctly. Variance is also increased significantly.
    mcc_patterns = {
        # Grocery & Convenience (SMALL amounts, high variance)
        '5411': {'base': 200000, 'min': 50000, 'max': 2000000, 'variance_mult': 5.0},
        '5412': {'base': 150000, 'min': 30000, 'max': 1500000, 'variance_mult': 6.0},
        
        # Gas Stations (MEDIUM amounts, moderate variance)
        '5541': {'base': 800000, 'min': 300000, 'max': 3000000, 'variance_mult': 3.0},
        '5542': {'base': 800000, 'min': 300000, 'max': 3000000, 'variance_mult': 3.0},
        
        # Restaurants (MEDIUM-LARGE amounts, high variance)
        '5812': {'base': 1500000, 'min': 400000, 'max': 8000000, 'variance_mult': 4.0},
        '5813': {'base': 2500000, 'min': 600000, 'max': 15000000, 'variance_mult': 5.0},
        '5814': {'base': 800000, 'min': 200000, 'max': 4000000, 'variance_mult': 4.0},
        
        # Drug Stores (SMALL-MEDIUM amounts, high variance)
        '5912': {'base': 400000, 'min': 100000, 'max': 5000000, 'variance_mult': 5.0},
        
        # Hotels (VERY LARGE amounts, extremely high variance)
        '7011': {'base': 15000000, 'min': 2000000, 'max': 100000000, 'variance_mult': 8.0},
        '7032': {'base': 8000000, 'min': 1000000, 'max': 50000000, 'variance_mult': 7.0},
        '7033': {'base': 6000000, 'min': 800000, 'max': 40000000, 'variance_mult': 7.5},
        
        # Car Dealers / Auto Sales (EXTREMELY LARGE amounts, maximum variance)
        # Adding some MCCs that simulate high-value transactions
        '5511': {'base': 50000000, 'min': 10000000, 'max': 500000000, 'variance_mult': 10.0},
        '5521': {'base': 40000000, 'min': 8000000, 'max': 400000000, 'variance_mult': 9.0},
        
        # Services (SMALL-MEDIUM amounts, high variance)
        '7210': {'base': 300000, 'min': 80000, 'max': 3000000, 'variance_mult': 5.0},
        '7211': {'base': 350000, 'min': 100000, 'max': 4000000, 'variance_mult': 5.5},
        '7216': {'base': 500000, 'min': 150000, 'max': 5000000, 'variance_mult': 6.0},
        
        # Retail (MEDIUM-LARGE amounts, very high variance)
        '5941': {'base': 3000000, 'min': 500000, 'max': 40000000, 'variance_mult': 7.0},
        '5942': {'base': 1000000, 'min': 200000, 'max': 10000000, 'variance_mult': 6.0},
        '5945': {'base': 2000000, 'min': 300000, 'max': 25000000, 'variance_mult': 7.5},
        
        # Package Stores / Alcohol (MEDIUM amounts, high variance)
        '5921': {'base': 1200000, 'min': 300000, 'max': 10000000, 'variance_mult': 5.0},
        
        # Sporting Goods (MEDIUM-LARGE amounts, high variance)
        '5941': {'base': 2500000, 'min': 400000, 'max': 30000000, 'variance_mult': 6.5},
        
        # Miscellaneous Retail (VARIES, very high variance)
        '5999': {'base': 1000000, 'min': 100000, 'max': 20000000, 'variance_mult': 8.0},
    }
    
    # Get pattern for this MCC or use default
    if mcc and mcc in mcc_patterns:
        pattern = mcc_patterns[mcc]
        base = pattern['base']
        min_val = pattern['min']
        max_val = pattern['max']
        variance_mult = pattern['variance_mult']
    else:
        # Default: medium amount with VERY high variance
        # This ensures even unknown MCCs have significant variance
        base = 1000000
        min_val = 100000
        max_val = 50000000
        variance_mult = 6.0
    
    # Generate amount using log-normal distribution for high variance
    # This creates a distribution with high variance while staying in bounds
    # Use log-normal: log(amount) ~ N(log(base), variance_mult)
    log_base = math.log(base)
    log_min = math.log(min_val)
    log_max = math.log(max_val)
    
    # Standard deviation in log space (controls variance)
    log_std = (log_max - log_min) / (2 * variance_mult)
    
    # Generate log-normal value
    log_value = random.gauss(log_base, log_std)
    
    # Clamp to bounds and convert back
    log_value = max(log_min, min(log_max, log_value))
    amount = round(math.exp(log_value), 0)
    
    # Add EXTRA variance: more frequently generate outliers
    # This ensures MCCs have very different distributions
    if random.random() < 0.15:  # 15% chance for outliers (increased from 10%)
        if random.random() < 0.4:  # 40% of outliers are very small
            # Very small amount (bottom 10% of range)
            amount = round(random.uniform(min_val, min_val + (base - min_val) * 0.1), 0)
        else:  # 60% of outliers are very large
            # Very large amount (top 20% of range)
            amount = round(random.uniform(base + (max_val - base) * 0.8, max_val), 0)
    
    return amount


def generate_sample_data(
    num_records: int,
    output_path: str,
    city_province_path: str,
    num_days: int = 30,
    num_cards: int = 50000,
    num_acceptors: int = 10000,
    seed: int = 42,
    spark_master: Optional[str] = None,
    output_format: str = "parquet"  # "parquet" or "csv"
) -> None:
    """
    Generate sample transaction data using Spark for scalable generation.
    
    Uses Spark to generate data in a distributed manner, avoiding memory issues
    for large datasets (100M+ records).
    
    Args:
        num_records: Number of transaction records to generate
        output_path: Path to output file (Parquet or CSV)
        city_province_path: Path to city_province.csv
        num_days: Number of days to span (default 30)
        num_cards: Number of unique cards to generate
        num_acceptors: Number of unique acceptors to generate
        seed: Random seed for reproducibility
        spark_master: Spark master URL (default: local[*])
        output_format: Output format - "parquet" or "csv" (default: parquet)
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, IntegerType
    )
    import numpy as np
    
    print(f"Initializing Spark session...")
    spark_master = spark_master or "local[*]"
    
    # Always check for existing session first - use it if available
    existing_session = SparkSession.getActiveSession()
    spark_created_by_us = False
    
    if existing_session:
        # Use existing session regardless of master mismatch
        current_master = existing_session.sparkContext.master
        print(f"✅ Using existing Spark session (master: {current_master})")
        if current_master != spark_master:
            print(f"   Note: Desired master was {spark_master}, but using existing {current_master}")
        spark = existing_session
        spark_created_by_us = False  # Don't stop it
    else:
        # No existing session, try to create new one
        print(f"Creating new Spark session (master: {spark_master})")
        try:
            spark = SparkSession.builder \
                .appName("GenerateTransactionData") \
                .master(spark_master) \
                .config("spark.sql.shuffle.partitions", "200") \
                .getOrCreate()
            spark_created_by_us = True
            print(f"✅ Created new Spark session")
        except Exception as e:
            # If creation fails, check if session was created by another thread/process
            print(f"⚠️ Failed to create Spark session: {e}")
            print("   Checking if session exists now...")
            existing_session = SparkSession.getActiveSession()
            if existing_session:
                print(f"✅ Found existing Spark session, using it (master: {existing_session.sparkContext.master})")
                spark = existing_session
                spark_created_by_us = False
            else:
                # Re-raise if we really can't get a session
                print("❌ No Spark session available and cannot create one")
                raise
    
    try:
        print(f"Loading cities from: {city_province_path}")
        cities = load_cities_from_csv(city_province_path)
        print(f"Loaded {len(cities)} unique cities")
        
        # Pre-generate cards and acceptors (small, can be in memory)
        print(f"Generating {num_cards:,} unique cards...")
        random.seed(seed)
        cards = [generate_card_number() for _ in range(num_cards)]
        
        print(f"Generating {num_acceptors:,} unique acceptors...")
        acceptors = []
        for _ in range(num_acceptors):
            acceptors.append({
                'id': generate_acceptor_id(),
                'city': random.choice(cities),
                'mcc': random.choice(MCC_CODES)
            })
        
        # Date range
        start_date = date(2024, 11, 1)
        dates = [start_date + timedelta(days=i) for i in range(num_days)]
        
        # Broadcast small data to all nodes
        cities_broadcast = spark.sparkContext.broadcast(cities)
        cards_broadcast = spark.sparkContext.broadcast(cards)
        acceptors_broadcast = spark.sparkContext.broadcast(acceptors)
        dates_broadcast = spark.sparkContext.broadcast(dates)
        mcc_codes_broadcast = spark.sparkContext.broadcast(MCC_CODES)
        
        # Calculate number of partitions (aim for ~100K records per partition)
        num_partitions = max(1, min(200, num_records // 100000))
        records_per_partition = num_records // num_partitions
        
        print(f"Generating {num_records:,} transactions using {num_partitions} partitions...")
        print(f"Output: {output_path} (format: {output_format})")
        
        # Create DataFrame with range and generate data using UDF
        def generate_transaction_batch(partition_id):
            """Generate a batch of transactions for this partition."""
            import random as py_random
            import string as py_string
            import math as py_math
            
            # Set seed based on partition ID for reproducibility
            py_random.seed(seed + partition_id)
            
            cities = cities_broadcast.value
            cards = cards_broadcast.value
            acceptors = acceptors_broadcast.value
            dates = dates_broadcast.value
            
            # Calculate how many records this partition should generate
            start_idx = partition_id * records_per_partition
            if partition_id == num_partitions - 1:
                # Last partition gets remainder
                end_idx = num_records
            else:
                end_idx = start_idx + records_per_partition
            
            records = []
            for i in range(start_idx, end_idx):
                # Select random card and acceptor
                card = py_random.choice(cards)
                acceptor = py_random.choice(acceptors)
                
                # Generate transaction with MCC-specific amount
                transaction_id = f"TXN{''.join(py_random.choices(py_string.ascii_uppercase + py_string.digits, k=12))}"
                amount = generate_amount(acceptor['mcc'])
                transaction_date = py_random.choice(dates).isoformat()
                
                records.append((
                    transaction_id,
                    int(amount),
                    transaction_date,
                    card,
                    acceptor['id'],
                    acceptor['city'],
                    acceptor['mcc']
                ))
            
            return iter(records)
        
        # Create RDD with partitions
        rdd = spark.sparkContext.parallelize(range(num_partitions), num_partitions) \
            .flatMap(generate_transaction_batch)
        
        # Define schema
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("amount", LongType(), False),
            StructField("transaction_date", StringType(), False),
            StructField("card_number", StringType(), False),
            StructField("acceptor_id", StringType(), False),
            StructField("acceptor_city", StringType(), False),
            StructField("mcc", StringType(), False),
        ])
        
        # Convert to DataFrame
        df = spark.createDataFrame(rdd, schema=schema)
        
        # Create output directory
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        
        # Write output
        if output_format.lower() == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        else:
            # CSV - coalesce to single partition for single file (or remove for multiple files)
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        # Count records
        actual_count = df.count()
        print(f"\n✅ Done! Generated {actual_count:,} transactions")
        print(f"   Output: {output_path}")
        
        # Print some statistics
        if os.path.exists(output_path):
            if os.path.isdir(output_path):
                # Parquet directory
                total_size = sum(
                    os.path.getsize(os.path.join(dirpath, filename))
                    for dirpath, dirnames, filenames in os.walk(output_path)
                    for filename in filenames
                )
            else:
                total_size = os.path.getsize(output_path)
            print(f"   File size: {total_size / (1024*1024):.2f} MB")
        
        # Print MCC statistics
        print("\nMCC Statistics (showing variance in amounts):")
        mcc_stats = df.groupBy("mcc").agg(
            F.avg("amount").alias("avg_amount"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.count("*").alias("count")
        ).orderBy("avg_amount").limit(10)
        mcc_stats.show(truncate=False)
        
    finally:
        # Only stop Spark session if we created it (not if we reused existing one)
        # This prevents closing notebook's Spark session
        if spark_created_by_us:
            print("Stopping Spark session created by generate_sample_data...")
            try:
                spark.stop()
            except Exception as e:
                print(f"Warning: Error stopping Spark session: {e}")
        else:
            print("Keeping existing Spark session (not stopping)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample transaction data for DP testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate 1 million records (default, Parquet format)
    python examples/generate_sample_data.py
    
    # Generate 100 million records (uses Spark, no memory issues)
    python examples/generate_sample_data.py --num-records 100000000
    
    # Generate to CSV format
    python examples/generate_sample_data.py --format csv --output my_data.csv
    
    # Generate with specific random seed and Spark master
    python examples/generate_sample_data.py --seed 123 --spark-master local[40]
        """
    )
    
    parser.add_argument(
        '--num-records', '-n',
        type=int,
        default=1000000,
        help='Number of records to generate (default: 1,000,000)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='data/sample_transactions.parquet',
        help='Output file path (default: data/sample_transactions.parquet). Always outputs Parquet format.'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        choices=['parquet', 'csv'],
        default='parquet',
        help='Output format: parquet (default, recommended for large datasets) or csv. Note: CSV is not recommended for 100M+ records.'
    )
    
    parser.add_argument(
        '--spark-master',
        type=str,
        default=None,
        help='Spark master URL (default: local[*])'
    )
    
    parser.add_argument(
        '--city-province', '-c',
        type=str,
        default='data/city_province.csv',
        help='Path to city_province.csv (default: data/city_province.csv)'
    )
    
    parser.add_argument(
        '--num-days', '-d',
        type=int,
        default=30,
        help='Number of days to span (default: 30)'
    )
    
    parser.add_argument(
        '--num-cards',
        type=int,
        default=50000,
        help='Number of unique cards (default: 50,000)'
    )
    
    parser.add_argument(
        '--num-acceptors',
        type=int,
        default=10000,
        help='Number of unique acceptors (default: 10,000)'
    )
    
    parser.add_argument(
        '--seed', '-s',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    
    args = parser.parse_args()
    
    generate_sample_data(
        num_records=args.num_records,
        output_path=args.output,
        city_province_path=args.city_province,
        num_days=args.num_days,
        num_cards=args.num_cards,
        num_acceptors=args.num_acceptors,
        seed=args.seed,
        spark_master=args.spark_master,
        output_format=args.format
    )


if __name__ == '__main__':
    main()

