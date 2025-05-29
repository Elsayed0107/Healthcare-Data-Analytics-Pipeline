from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("MIMIC-III CSV to Parquet") \
    .getOrCreate()

csv_path = "./"
parquet_path = "./parquet/"

tables = [
    "PATIENTS",
    "ADMISSIONS",
    "DIAGNOSES_ICD",
    "PROCEDURES_ICD",
    "PRESCRIPTIONS",
    "LABEVENTS",
    "CHARTEVENTS",
    "ICUSTAYS"
]

os.makedirs(parquet_path, exist_ok=True)

for table in tables:
    print(f"Converting {table}.csv ...")
    try:
        df = spark.read.csv(f"{csv_path}{table}.csv", header=True, inferSchema=True)
        df_clean = df.dropna(how="all")
        df_clean.write.mode("overwrite").parquet(f"{parquet_path}{table.lower()}")
        print(f"Converted: {table}.csv to parquet/{table.lower()}")
    except Exception as e:
        print(f"Error processing {table}: {e}")

spark.stop()
