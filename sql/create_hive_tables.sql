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
