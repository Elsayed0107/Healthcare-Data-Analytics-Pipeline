# Manual for Healthcare Data Analytics Pipeline

This project contains a healthcare data analytics pipeline built using Docker, Hadoop, Hive, MapReduce, and PySpark. It processes and analyzes the MIMIC-III Clinical Database (demo version) for batch analytics using Hive SQL and Java-based MapReduce.

---

## Table of Contents

- Overview
- Purpose
- Prerequisites
- Setup
- Running the Pipeline
  - Step 1: Clean & Convert CSVs to Parquet
  - Step 2: Copy Parquet Files to HDFS
  - Step 3: Create Hive Tables
  - Step 4: Run Hive Analytics Queries
  - Step 5: Run MapReduce Job
- Hive Table Definitions
- Hive Analytics Queries

---

## Overview

This manual provides a step-by-step guide to setting up and executing the full pipeline. The data is extracted from 8 CSV files:

- PATIENTS.csv  
- ADMISSIONS.csv  
- DIAGNOSES_ICD.csv  
- PROCEDURES_ICD.csv  
- PRESCRIPTIONS.csv  
- LABEVENTS.csv  
- CHARTEVENTS.csv  
- ICUSTAYS.csv  

Each file is cleaned and converted to Parquet format, stored in HDFS, then analyzed using Hive and MapReduce jobs.

---

## Purpose

The pipeline:

- Cleans raw CSV files and converts them to Parquet format using PySpark.
- Stores cleaned data in HDFS.
- Creates Hive external tables pointing to Parquet files.
- Executes SQL analytics queries over Hive tables.
- Runs a MapReduce job to calculate average age and gender distribution.

---

## Prerequisites

- Python 3.6+
- Docker with Docker Compose
- Java 8 (inside namenode)
- Hadoop CLI (inside namenode)
- Git (for future cloning)

Install dependencies:

```bash
pip install pandas pyarrow
Dataset used: MIMIC-III Clinical Database Demo v1.4
Stored in HDFS under:
/user/mimic/parquet/<table_name>/
/user/mimic/input_clean/PATIENTS_CLEANED.csv
Setup
Install Docker
Install Docker and ensure Docker Compose is included.

Start Docker Hadoop Cluster
Clone the base Hadoop environment:
git clone https://github.com/Marcel-Jan/docker-hadoop-spark
cd docker-hadoop-spark
docker-compose up -d
docker ps
Download Dataset
Download MIMIC-III (demo) CSVs from PhysioNet and extract them into:
/home/vboxuser/docker-hadoop-spark/database/
Files expected:

PATIENTS.csv

ADMISSIONS.csv

DIAGNOSES_ICD.csv

PROCEDURES_ICD.csv

PRESCRIPTIONS.csv

LABEVENTS.csv

CHARTEVENTS.csv

ICUSTAYS.csv
Running the Pipeline
Step 1: Clean & Convert CSVs to Parquet
Run the following script:
cd scripts/
python convert_csv_to_parquet.py
✔ Output Parquet files will be saved to:
/home/vboxuser/docker-hadoop-spark/database/parquet/
Step 2: Copy Parquet Files to HDFS
docker cp ../database/parquet/. namenode:/parquet/
docker exec -it namenode bash

hdfs dfs -mkdir -p /user/mimic/parquet

hdfs dfs -put /parquet/admissions /user/mimic/parquet/
hdfs dfs -put /parquet/diagnoses_icd /user/mimic/parquet/
hdfs dfs -put /parquet/procedures_icd /user/mimic/parquet/
hdfs dfs -put /parquet/prescriptions /user/mimic/parquet/
hdfs dfs -put /parquet/labevents /user/mimic/parquet/
hdfs dfs -put /parquet/chartevents /user/mimic/parquet/
hdfs dfs -put /parquet/icustays /user/mimic/parquet/
hdfs dfs -put /parquet/patients /user/mimic/parquet/
Step 3: Create Hive Tables
docker cp sql/create_hive_tables.sql hive-server:/create_hive_tables.sql
docker exec -it hive-server bash
hive -f /create_hive_tables.sql
Step 4: Run Hive Analytics Queries
docker cp sql/queries.sql hive-server:/queries.sql
docker exec -it hive-server bash
hive -f /queries.sql > /output_host/analytics_results.txt
exit
docker cp hive-server:/output_host/analytics_results.txt results/output_host/
Step 5: Run MapReduce Job
cd mapreduce/patient_stats/
docker cp PatientStats.java namenode:/root/
docker exec -it namenode bash

cd /root
javac -classpath $(hadoop classpath) -d . PatientStats.java
jar cf PatientStatsJob.jar *.class

hdfs dfs -mkdir -p /user/mimic/input_clean
hdfs dfs -put /input_clean/PATIENTS_CLEANED.csv /user/mimic/input_clean/

hadoop jar PatientStatsJob.jar PatientStats /user/mimic/input_clean/PATIENTS_CLEANED.csv /user/mimic/output/patient_stats
hdfs dfs -cat /user/mimic/output/patient_stats/part-r-00000

mkdir -p /output_host/patient_stats
hdfs dfs -get /user/mimic/output/patient_stats /output_host/
exit
docker cp namenode:/output_host/patient_stats mapreduce/result/
Hive Table Definitions
🧾 File: sql/create_hive_tables.sql
-- Table: patients
CREATE EXTERNAL TABLE IF NOT EXISTS patients (
  subject_id INT,
  gender STRING,
  dob TIMESTAMP,
  dod TIMESTAMP,
  dod_hosp TIMESTAMP,
  dod_ssn TIMESTAMP,
  expire_flag INT
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/patients';

-- Table: admissions
CREATE EXTERNAL TABLE IF NOT EXISTS admissions (
  subject_id INT,
  hadm_id INT,
  admittime TIMESTAMP,
  dischtime TIMESTAMP,
  deathtime TIMESTAMP,
  admission_type STRING,
  admission_location STRING,
  discharge_location STRING,
  insurance STRING,
  language STRING,
  religion STRING,
  marital_status STRING,
  ethnicity STRING,
  edregtime TIMESTAMP,
  edouttime TIMESTAMP,
  diagnosis STRING,
  hospital_expire_flag INT,
  has_chartevents_data INT
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/admissions';

-- Table: diagnoses_icd
CREATE EXTERNAL TABLE IF NOT EXISTS diagnoses_icd (
  row_id INT,
  subject_id INT,
  hadm_id INT,
  seq_num INT,
  icd9_code STRING
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/diagnoses_icd';

-- Table: procedures_icd
CREATE EXTERNAL TABLE IF NOT EXISTS procedures_icd (
  row_id INT,
  subject_id INT,
  hadm_id INT,
  seq_num INT,
  icd9_code STRING
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/procedures_icd';

-- Table: prescriptions
CREATE EXTERNAL TABLE IF NOT EXISTS prescriptions (
  row_id INT,
  subject_id INT,
  hadm_id INT,
  icustay_id INT,
  startdate TIMESTAMP,
  enddate TIMESTAMP,
  drug_type STRING,
  drug STRING,
  drug_name_poe STRING,
  drug_name_generic STRING,
  formulary_drug_cd STRING,
  gsn STRING,
  ndc STRING,
  prod_strength STRING,
  dose_val_rx STRING,
  dose_unit_rx STRING,
  form_val_disp STRING,
  form_unit_disp STRING,
  route STRING
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/prescriptions';

-- Table: labevents
CREATE EXTERNAL TABLE IF NOT EXISTS labevents (
  row_id INT,
  subject_id INT,
  hadm_id INT,
  itemid INT,
  charttime TIMESTAMP,
  value STRING,
  valuenum DOUBLE,
  valueuom STRING,
  flag STRING
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/labevents';

-- Table: chartevents
CREATE EXTERNAL TABLE IF NOT EXISTS chartevents (
  row_id INT,
  subject_id INT,
  hadm_id INT,
  icustay_id INT,
  itemid INT,
  charttime TIMESTAMP,
  storetime TIMESTAMP,
  cgid INT,
  value STRING,
  valuenum DOUBLE,
  valueuom STRING,
  warning INT,
  error INT,
  resultstatus STRING,
  stopped STRING
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/chartevents';

-- Table: icustays
CREATE EXTERNAL TABLE IF NOT EXISTS icustays (
  row_id INT,
  subject_id INT,
  hadm_id INT,
  icustay_id INT,
  dbsource STRING,
  first_careunit STRING,
  last_careunit STRING,
  first_wardid INT,
  last_wardid INT,
  intime TIMESTAMP,
  outtime TIMESTAMP,
  los DOUBLE
)
STORED AS PARQUET
LOCATION '/user/mimic/parquet/icustays';
Hive Analytics Queries
📄 File: sql/queries.sql

-- 1. Average length of stay per diagnosis
-- Calculates the average hospital length of stay for each ICD-9 diagnosis code
SELECT d.icd9_code, AVG(d.dischtime - d.admittime) AS avg_los
FROM diagnoses_icd d
JOIN admissions a ON d.hadm_id = a.hadm_id
GROUP BY d.icd9_code;

-- 2. ICU readmission distribution
-- Counts the number of ICU readmissions per first care unit
SELECT first_careunit, COUNT(*) AS readmissions
FROM icustays
GROUP BY first_careunit;

-- 3. Mortality rates by age and gender (filtered to age <= 85)
-- Returns the number of deaths per gender and age group, excluding outliers older than 85
SELECT gender, FLOOR(DATEDIFF(dod, dob)/365) AS age, COUNT(*) AS deaths
FROM patients
WHERE dod IS NOT NULL
AND FLOOR(DATEDIFF(dod, dob)/365) <= 85
GROUP BY gender, FLOOR(DATEDIFF(dod, dob)/365)
ORDER BY age;

-- 4. ICU length of stay per diagnosis
-- Computes average ICU stay duration for each diagnosis code
SELECT d.icd9_code, AVG(i.los) AS avg_icu_los
FROM diagnoses_icd d
JOIN icustays i ON d.hadm_id = i.hadm_id
GROUP BY d.icd9_code;

-- 5. Relationship between age, length of stay, and mortality
-- Shows age, ICU stay, and whether the patient is deceased for correlation analysis
SELECT p.subject_id,
FLOOR(DATEDIFF(NOW(), p.dob)/365) AS age,
i.los,
CASE WHEN p.dod IS NOT NULL THEN 1 ELSE 0 END AS deceased
FROM patients p
JOIN admissions a ON p.subject_id = a.subject_id
JOIN icustays i ON a.hadm_id = i.hadm_id;

-- 6. Top prescribed drugs per diagnosis
-- Lists the most commonly prescribed drugs for each diagnosis code
SELECT d.icd9_code, pr.drug, COUNT(*) AS times_prescribed
FROM diagnoses_icd d
JOIN prescriptions pr ON d.hadm_id = pr.hadm_id
GROUP BY d.icd9_code, pr.drug
ORDER BY times_prescribed DESC;

-- 7. Vital signs over days of stay
-- Extracts raw charted vital signs by time to analyze patient trends
SELECT subject_id, charttime, itemid, valuenum
FROM chartevents
WHERE valuenum IS NOT NULL
ORDER BY subject_id, charttime;

