import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Read the CSV
df = pd.read_csv('/home/vboxuser/docker-hadoop-spark/database/PATIENTS.csv')

# Convert date columns to datetime format, coerce errors to NaT
date_cols = ['dob', 'dod', 'dod_hosp', 'dod_ssn']
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df[col] = df[col].fillna('9999-12-31 00:00:00')  # Hive-friendly placeholder

# Clean gender column
df['gender'] = df['gender'].astype('category')
df['gender'] = df['gender'].cat.add_categories('Unknown')
df['gender'] = df['gender'].fillna('Unknown')

# Convert expire_flag to nullable boolean
df['expire_flag'] = df['expire_flag'].astype('boolean')

# Print any remaining nulls
print("Remaining NaNs:")
print(df.isna().sum())

# Convert to Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, '/home/vboxuser/docker-hadoop-spark/database/PATIENTS_CLEANED.parquet')

# Optional: print schema
print("Schema:")
print(table.schema)
