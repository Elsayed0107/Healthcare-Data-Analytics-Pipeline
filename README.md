# 🚑 Healthcare Data Analytics Pipeline

## 📌 Purpose

This project builds a **scalable Big Data pipeline** to process and analyze clinical data from the **MIMIC-III demo dataset**. The pipeline supports both **batch analytics** using **MapReduce** and **Apache Hive**, and aims to extract vital healthcare insights including:

- Average patient age.
- Gender distribution.
- ICU readmission trends.
- Length of stay.
- Mortality rates.
- Top prescriptions and procedures.
- Changes in vital signs during ICU stays.

## 🎯 Objectives

- ✅ Extract raw healthcare data from MIMIC-III CSV files.
- ✅ Clean and preprocess the data using **Python & Pandas**.
- ✅ Convert cleaned CSV files to **Parquet** format using **PySpark**.
- ✅ Store data in **HDFS** inside a Dockerized Hadoop environment.
- ✅ Create **Hive external tables** on top of Parquet files.
- ✅ Perform analytics using **Hive SQL** and **Java-based MapReduce**.

---

## 🔁 Pipeline Overview
CSV (raw)
↓
Cleaning via Python (Pandas)
↓
Parquet Conversion (PySpark)
↓
Storage in HDFS
↓
Hive External Tables
↓
Analytics via Hive SQL + Java MapReduce
---

## 📂 Datasets Used (from MIMIC-III Demo)

- `PATIENTS.csv`
- `ADMISSIONS.csv` 
- `DIAGNOSES_ICD.csv` 
- `PROCEDURES_ICD.csv` 
- `PRESCRIPTIONS.csv` 
- `LABEVENTS.csv` 
- `CHARTEVENTS.csv`
- `ICUSTAYS.csv`

---

## 🧹 Data Cleaning & Transformation

**Scripts used:**

- `clean_patients.py`.
- `convert_csv_to_parquet.py`.
- `convert_parquet_to_csv.py`.

**Cleaning tasks:**

- Remove rows with all null values.
- Normalize date formats and timestamps. 
- Encode categorical fields like gender.
- Fill or flag missing values appropriately.
- Convert cleaned data to efficient **Parquet** format.

---

## 📦 Data Storage

All data is stored in **HDFS**, organized as:

- `/user/mimic/input_clean/` → Cleaned CSVs. 
- `/user/mimic/parquet/` → Parquet files.

HDFS runs within a **Dockerized Hadoop** environment for easy deployment and testing.

---

## 🐝 Hive Integration

External **Hive tables** were created over the Parquet files using the `create_hive_tables.sql` script.

📄 Location: `sql/create_hive_tables.sql`

---

## 📊 Analytics

### 🔎 Hive SQL Queries (in `queries.sql`)

1. Average length of stay per diagnosis. 
2. ICU readmission distribution.
3. Mortality rates by age and gender (filtered to age ≤ 85). 
4. ICU length of stay per diagnosis.
5. Relationship between age, length of stay, and mortality. 
6. Top prescribed drugs per diagnosis. 
7. Vital signs trend over days of ICU stay. 

📁 Results saved to: `results/output_host/`

---

### ☕ Java MapReduce Jobs

**Location:** `mapreduce/patient_stats/PatientStats.java` 
**Output file:** `mapreduce/result/part-r-00000`

Calculated:

- **Average patient age** 
- **Gender distribution** (number of male and female patients)

---

## ⚙️ Technologies Used

- **Python** (Pandas, PyArrow) 
- **PySpark** 
- **Java** (MapReduce) 
- **Apache Hadoop** (HDFS + YARN) 
- **Apache Hive** 
- **Docker** 
- **Ubuntu 22.04**

---

## 📁 Project Structure
healthcare-project/
├── database/
│ ├── parquet/
│ ├── *.csv
├── mapreduce/
│ ├── patient_stats/
│ └── result/
├── sql/
│ ├── create_hive_tables.sql
│ └── queries.sql
├── results/
│ └── output_host/
├── scripts/
│ ├── clean_patients.py
│ ├── convert_csv_to_parquet.py
│ └── convert_parquet_to_csv.py
├── Docs/
│ └── USER_MANUAL.md
├── README.md

---

## 📞 Contact

Name: El-Sayed Ehab 
Email: elsayed.ehab0107@gmail.com

---
