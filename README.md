Healthcare-Data-Analytics-Pipeline

Purpose

This project aims to build a scalable big data pipeline for processing and analyzing clinical data from the MIMIC-III demo dataset. The pipeline supports batch analytics using MapReduce and Hive, and focuses on extracting key insights like average patient age, gender distribution, ICU readmission trends, length of stay, mortality rates, and more.

It leverages core big data technologies such as HDFS, Hadoop MapReduce, Hive, and Pandas-based preprocessing to prepare the data for efficient analysis and exploration.

Objectives

Extract raw healthcare data from MIMIC-III demo CSV files.

Clean and preprocess data using Python & Pandas.

Convert cleaned CSVs to Parquet format using PySpark.

Store Parquet files in HDFS (via Dockerized Hadoop environment).

Create Hive external tables on top of Parquet files.

Perform analytics via Hive queries and Java-based MapReduce.

Pipeline Overview

Data Extraction

Raw MIMIC-III CSV files used:

PATIENTS.csv

ADMISSIONS.csv

DIAGNOSES_ICD.csv

PROCEDURES_ICD.csv

PRESCRIPTIONS.csv

LABEVENTS.csv

CHARTEVENTS.csv

ICUSTAYS.csv

Data Cleaning and Transformation

Python scripts were used to:

Drop rows with all nulls.

Normalize date formats and handle missing timestamps.

Encode gender and handle unknown values.

Fill missing expiration flags appropriately.

Convert to efficient Parquet format.

Scripts used:

clean_patients.py

convert_csv_to_parquet.py

convert_parquet_to_csv.py

Data Storage (HDFS)

All cleaned datasets (Parquet and CSV) are stored in HDFS inside a Dockerized Hadoop environment.

HDFS paths follow: /user/mimic/parquet/ and /user/mimic/input_clean/

Hive Table Creation

External Hive tables were created for all 8 datasets using scripts in the hive/ directory. Tables are defined on Parquet formats.

Analytics

SQL-based Hive queries were used to explore:

1- Average length of stay per diagnosis
2- ICU readmission distribution
3- Mortality rates by age and gender (filtered to age <= 85)
4- ICU length of stay per diagnosis
5- Relationship between age, length of stay, and mortality 
6- Top prescribed drugs per diagnosis
7- Vital signs over days of stay

Java-based MapReduce job calculates:

Average patient age

Gender distribution (number of male and female patients)

📌 Data Pipeline Architecture

CSV (raw) → Cleaning via Python → Parquet → HDFS → Hive External Tables → Hive SQL + MapReduce → Analytics Outputs

Technologies Used

Python (Pandas, PyArrow)

PySpark

Java (for MapReduce)

Ubuntu 22.04

Hadoop (HDFS + YARN)

Apache Hive

Docker
Project Structure
healthcare-project/
├── database/
│   ├── parquet/
│   │   ├── ADMISSIONS.parquet
│   │   ├── DIAGNOSES_ICD.parquet
│   │   ├── ICUSTAYS.parquet
│   │   ├── LABEVENTS.parquet
│   │   ├── CHARTEVENTS.parquet
│   │   ├── PRESCRIPTIONS.parquet
│   │   ├── PROCEDURES_ICD.parquet
│   │   └── PATIENTS.parquet
│   ├── ADMISSIONS.csv
│   ├── DIAGNOSES_ICD.csv
│   ├── ICUSTAYS.csv
│   ├── LABEVENTS.csv
│   ├── CHARTEVENTS.csv
│   ├── PRESCRIPTIONS.csv
│   ├── PROCEDURES_ICD.csv
│   └── PATIENTS.csv
├── mapreduce/
│   ├── patient_stats/
│   │   └── PatientStats.java
│   └── result/
│       └── part-r-00000
├── sql/
│   ├── create_hive_tables.sql
│   └── queries.sql
├── results/
│   └── output_host/
│       ├── query1_result.txt
│       ├── query2_result.txt
│       ├── query3_result.txt
│       ├── query4_result.txt
│       ├── query5_result.txt
│       ├── query6_result.txt
│       └── query7_result.txt
├── scripts/
│   ├── clean_patients.py
│   ├── convert_csv_to_parquet.py
│   └── convert_parquet_to_csv.py
├── Docs/
│   └── USER_MANUAL.md
├── README.md
Contact
Name: El-Sayed Ehab
Email: elsayed.ehab0107@gmail.com
