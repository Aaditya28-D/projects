-- DATA
-- 1. Retrieve all records from the nsw_lga_code_stg view
SELECT * FROM nsw_lga_code_stg nlcs;

-- SHAPE OF DATA
-- 2. Count the total number of records in the nsw_lga_code_stg view
SELECT COUNT(*) AS total_records
FROM nsw_lga_code_stg;

-- 3. Count the total number of columns in the nsw_lga_code_stg view
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_name = 'nsw_lga_code_stg';

-- TOTAL DUPLICATES IN THE DATA
-- 4. Find duplicate records in the nsw_lga_code_stg view based on lga_code and lga_name
SELECT *
FROM nsw_lga_code_stg
WHERE (lga_code, lga_name) IN (
   SELECT lga_code, lga_name
   FROM nsw_lga_code_stg
   GROUP BY lga_code, lga_name
   HAVING COUNT(*) > 1
);

-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in lga_code and lga_name columns
SELECT
  COUNT(DISTINCT lga_code) AS unique_lga_codes,
  COUNT(DISTINCT lga_name) AS unique_lga_names
FROM nsw_lga_code_stg;

-- DATA CONSISTENCY
-- 6. Find distinct lga_code values that are not numeric
SELECT DISTINCT lga_code
FROM nsw_lga_code_stg
WHERE lga_code::text ~ E'^\\D+$';


-- 7. Find distinct lga_name values that contain non-alphabetical characters or whitespace
SELECT DISTINCT lga_name
FROM nsw_lga_code_stg
WHERE lga_name ~ '[^a-zA-Z\s]';

-- DATA TYPE
-- 8. Retrieve column names and data types of columns in the nsw_lga_code_stg view
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'nsw_lga_code_stg';

-- NULL VALUES CHECK
-- 9. Count the number of records with null values in the lga_code column
SELECT COUNT(*)
FROM nsw_lga_code_stg
WHERE lga_code IS NULL;

-- 10. Count the number of records with null values in the lga_name column
SELECT COUNT(*)
FROM nsw_lga_code_stg
WHERE lga_name IS NULL;
