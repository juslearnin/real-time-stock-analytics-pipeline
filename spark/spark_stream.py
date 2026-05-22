from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

# 1. Initialize Docker-Optimized Spark Session
spark = SparkSession.builder \
    .appName("StockMarketStreamingAnalytics") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define Schema for Inbound Kafka JSON Data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Docker Container Infrastructure Configuration Matrix
DB_CONFIG = {
    "host": "postgres_db",       # Docker container network DNS hostname
    "database": "stock_analytics",
    "user": "market_user",
    "password": "market_password",
    "port": "5432"
}

# 4. Custom Batch Micro-Sink Writer Engine
def write_to_postgres(batch_df, batch_id):
    # Collect cluster partitions back to the worker process memory space
    rows = batch_df.collect()
    if not rows:
        return

    try:
        # Establish a secure transactional handshake with the database container
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for row in rows:
            query = """
            INSERT INTO stock_analytics 
            (symbol, window_start, window_end, avg_price, max_price, min_price, total_volume, volatility, trend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Unpack Spark Rows safely into relational column types
            cursor.execute(query, (
                row['symbol'],
                row['window']['start'],
                row['window']['end'],
                float(row['avg_price']) if row['avg_price'] is not None else None,
                float(row['max_price']) if row['max_price'] is not None else None,
                float(row['min_price']) if row['min_price'] is not None else None,
                int(row['total_volume']) if row['total_volume'] is not None else None,
                float(row['volatility']) if row['volatility'] is not None else None,
                row['trend']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"[BATCH {batch_id}] Successfully persisted processed metrics to PostgreSQL container.")
    except Exception as e:
        print(f"[BATCH {batch_id}] Database insertion error hook tripped: {e}")

# 5. Read Stream from Container Network Broker
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "latest") \
    .load()

# 6. Parse Payloads
json_df = df.selectExpr("CAST(value AS STRING)")
parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 7. Build Sector Lookup Expressions
full_sector_map = {
    "AAPL": "TECH", "MSFT": "TECH", "GOOGL": "TECH", "META": "TECH", "NVDA": "TECH", "TCS.NS": "TECH", "INFY.NS": "TECH",
    "TSLA": "AUTO",
    "JPM": "BANKING", "HDFCBANK.NS": "BANKING", "ICICIBANK.NS": "BANKING", "KOTAKBANK.NS": "BANKING", "SBIN.NS": "BANKING", "AXISBANK.NS": "BANKING",
    "V": "FINANCIAL_SERVICES", "BAJFINANCE.NS": "FINANCIAL_SERVICES",
    "RELIANCE.NS": "ENERGY",
    "DIS": "ENTERTAINMENT",
    "LT.NS": "CONSTRUCTION",
    "HINDUNILVR.NS": "FMCG"
}
sector_lookup = create_map([lit(x) for item in full_sector_map.items() for x in item])

# 8. Data Transformation Pipeline Execution
transformed_df = parsed_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("symbol", trim(col("symbol"))) \
    .withColumn("sector", coalesce(sector_lookup[trim(col("symbol"))], lit("OTHER")))

# 9. Stateful Aggregations
analytics_df = transformed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute"), col("sector"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),
        max("price").alias("max_price"),
        min("price").alias("min_price"),
        sum("volume").alias("total_volume"),
        stddev("price").alias("volatility")
    )

analytics_df = analytics_df.withColumn(
    "trend",
    when(col("volatility").isNull(), "INITIALIZING")
    .when(col("volatility") > 200.0, "HIGH_VOLATILITY")
    .when((col("max_price") - col("min_price")) > 500.0, "BREAKOUT_ZONE")
    .otherwise("STABLE_TRADING")
)

# 10. TARGET STREAM A: Console Analytics Logger (For Active Verification)
console_query = analytics_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 11. TARGET STREAM B: Relational Transactional DB Sink (Persist Layer)
postgres_query = analytics_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .start()

# Hold execution runtime context open
spark.streams.awaitAnyTermination()