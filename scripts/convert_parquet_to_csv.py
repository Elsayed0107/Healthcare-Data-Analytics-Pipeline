import pandas as pd
import pyarrow.parquet as pq

# Read the cleaned Parquet file
table = pq.read_table('/home/vboxuser/docker-hadoop-spark/database/PATIENTS_CLEANED.parquet')
df = table.to_pandas()

# Save as CSV (without index)
df.to_csv('/home/vboxuser/docker-hadoop-spark/database/PATIENTS_CLEANED.csv', index=False)

print("✅ CSV file saved successfully.")
