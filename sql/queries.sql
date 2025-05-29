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
