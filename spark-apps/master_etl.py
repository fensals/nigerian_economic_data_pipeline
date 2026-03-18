import requests
import json
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, month, year, lit
from pyspark.sql import functions as F
from pyspark.sql import Window
import os

#API urls
FX_URL = "https://www.cbn.gov.ng/api/GetAllExchangeRates"
INF_URL = "https://www.cbn.gov.ng/api/GetAllInflationRates"


DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# HDFS Connection 
hdfs_client = InsecureClient('http://hdfs-namenode:9870', user='root')
HDFS_LANDING_PATH = '/cbn_project/landing'

def run_cbn_etl():
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CBN_Automated_ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()


    #### BRONZE LAYER - DATA INGESTION

    print("Starting ETL Pipeline...")
            
    def ingest_cbn_data(url, hdfs_filename):
        # Ensure the directory exists, to create if it doesnt exist.
        if hdfs_client.status(HDFS_LANDING_PATH, strict=False) is None:
            print(f"Creating directory: {HDFS_LANDING_PATH}")
            hdfs_client.makedirs(HDFS_LANDING_PATH)

        
        print(f"Connecting to CBN API for {hdfs_filename}...")
        
        try:
            # (Rest of your logic remains the same)
            response = requests.get(url, timeout=30)
            response.raise_for_status() 
            
            data = response.json()
            hdfs_full_path = f"{HDFS_LANDING_PATH}/{hdfs_filename}"
            
            with hdfs_client.write(hdfs_full_path, encoding='utf-8', overwrite=True) as writer:
                json.dump(data, writer)
                
            print(f"Data Ingested succssfully to: {hdfs_full_path}")
            
        except Exception as e:
            print(f"Failed to ingest data: {e}")

    
    ingest_cbn_data(FX_URL, "raw_exchange_rates.json")
    ingest_cbn_data(INF_URL, "raw_inflation_rates.json")
   
    
    ##### SILVER LAYER - DATA CLEANING AND TRANSFORMATION
    print("Processing Silver Layer...")

    # Read and import the raw fx data
    df_exch_raw = spark.read.option("multiLine", "true").json("hdfs://hdfs-namenode:9000/cbn_project/landing/raw_exchange_rates.json")

    # Read  and import the raw Inflation/CPI data
    df_cpi_raw = spark.read.option("multiLine", "true").json("hdfs://hdfs-namenode:9000/cbn_project/landing/raw_inflation_rates.json")

    #clean and transaform the exchange rate data for USD, and filter for the last 20 years (240 months)
    df_usd_silver = df_exch_raw.filter(F.col("currency") == "US DOLLAR") \
    .select(
        F.to_date(F.col("ratedate")).alias("rate_date"),
        F.col("buyingrate").cast("decimal(18,2)").alias("buying_rate"),
        F.col("sellingrate").cast("decimal(18,2)").alias("selling_rate"),
        F.col("centralrate").cast("decimal(18,2)").alias("central_rate")
    ) \
    .filter(
        F.col("rate_date") >= F.add_months(F.current_date(), -240)
    ) \
    .orderBy(F.col("rate_date").asc())
    

    # clean and transform the CPI data, creating a proper date column and filtering for the last 20 years (240 months)
    df_cpi_silver = df_cpi_raw.select(
        # Create a proper date from 'period' or use tyear/tmonth
        F.to_date(F.concat(F.col("tyear"), F.lit("-"), F.col("tmonth"), F.lit("-01"))).alias("report_date"),
        F.col("allItemsYearOn").cast("decimal(10,2)").alias("headline_inflation"),
        F.col("foodYearOn").cast("decimal(10,2)").alias("food_inflation"),
        F.col("allItemsLessFrmProdAndEnergyYearOn").cast("decimal(10,2)").alias("core_inflation")
    ) \
    .filter(F.col("report_date") >= F.add_months(F.current_date(), -240)) \
    .orderBy(F.col("report_date").desc())

    # Save the monthly CPI and daily exchange rate data to the Silver layer in HDFS
    df_usd_silver.write.mode("overwrite").parquet("hdfs://hdfs-namenode:9000/cbn_project/silver/forex_daily")
    df_cpi_silver.write.mode("overwrite").parquet("hdfs://hdfs-namenode:9000/cbn_project/silver/cpi_monthly")
    
    
    #### GOLD LAYER (Aggregation and Index Calculation)
    print("Processing Gold Layer...")
    
    
    
    # df_usd_daily = spark.read.parquet("hdfs://hdfs-namenode:9000/cbn_project/silver/forex_daily")
    # df_cpi_monthly = spark.read.parquet("hdfs://hdfs-namenode:9000/cbn_project/silver/cpi_monthly")

    #using the dataframes from the silver layer.

    
    # Calculate the Average rates and standard deviation for USD (volatility), and group by year and month to align with CPI data
    df_usd_monthly = df_usd_silver.withColumn("year", F.year("rate_date")) \
        .withColumn("month", F.month("rate_date")) \
        .groupBy("year", "month") \
        .agg(
            F.round(F.avg("central_rate"), 2).alias("avg_usd_rate"),
            F.round(F.stddev("central_rate"), 2).alias("usd_volatility"),
            F.max("central_rate").alias("peak_usd_rate")
        )

    
    df_merged = df_usd_monthly.join(
        df_cpi_silver.withColumn("year", F.year("report_date")).withColumn("month", F.month("report_date")),
        on=["year", "month"],
        how="inner"
    ).withColumn(
        "report_date", 
        F.to_date(F.concat(F.col("year"), F.lit("-"), F.col("month"), F.lit("-01")))
    )

    
    window_start = Window.orderBy("report_date")

    # 3. Calculate metrics and KEEP usd_volatility in the final select
    df_final = df_merged.withColumn("first_usd", F.first("avg_usd_rate").over(window_start)) \
        .withColumn("first_cpi", F.first("headline_inflation").over(window_start)) \
        .withColumn("usd_growth_index", F.round((F.col("avg_usd_rate") / F.col("first_usd")) * 100, 2)) \
        .withColumn("inflation_growth_index", F.round((F.col("headline_inflation") / F.col("first_cpi")) * 100, 2)) \
        .select(
            "report_date", 
            "year", "month", 
            "avg_usd_rate", 
            "usd_volatility",        
            "headline_inflation", 
            "usd_growth_index", 
            "inflation_growth_index"
        ) \
        .orderBy(F.col("report_date").desc())


    # SERVING LAYER (Postgres)
    print("Writing to PostgreSQL...")
    
    jdbc_url = "jdbc:postgresql://cbn_postgres:5432/cbn_analytics"
    db_properties = {
        "user": "fensals",
        "password": "fensals",
        "driver": "org.postgresql.Driver"
    }

    df_final.write.jdbc(
        url=jdbc_url, 
        table="gold_macro_economic_data", 
        mode="overwrite", 
        properties=db_properties
    )

    print("Gold data successfully migrated to PostgreSQL!")
    
    print("ETL Job Finished Successfully!")
    spark.stop()

if __name__ == "__main__":
    run_cbn_etl()