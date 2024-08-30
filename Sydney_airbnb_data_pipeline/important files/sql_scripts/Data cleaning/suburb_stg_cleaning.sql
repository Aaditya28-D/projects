
-- DATA 
-- 1. Retrieve all records from the nsw_lga_suburb_stg view
SELECT * FROM nsw_lga_suburb_stg nlss;


-- SHAPE OF DATA
-- 2. Count the total number of records in the nsw_lga_suburb_stg view
SELECT COUNT(*) AS total_records
FROM nsw_lga_suburb_stg;

-- 3. Count the total number of columns in the nsw_lga_suburb_stg view
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_name = 'nsw_lga_suburb_stg';


-- TOTAL DUPLICATES IN THE DATA
-- 4. Find duplicate records in the nsw_lga_suburb_stg view based on suburb_name and lga_name
SELECT *
FROM nsw_lga_suburb_stg
WHERE (suburb_name, lga_name) IN (
   SELECT suburb_name, lga_name
   FROM nsw_lga_suburb_stg
   GROUP BY suburb_name, lga_name
   HAVING COUNT(*) > 1
);


-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in suburb_name and lga_name columns
SELECT
  COUNT(DISTINCT suburb_name) AS unique_suburb_names,
  COUNT(DISTINCT lga_name) AS unique_lga_names
FROM nsw_lga_suburb_stg;


--  DATA CONSISTENCY 
-- 6. Find distinct suburb_name values that contain non-alphabetical characters or whitespace
SELECT DISTINCT suburb_name
FROM nsw_lga_suburb_stg
WHERE suburb_name ~ '[^a-zA-Z\s]';

-- 7. Find distinct lga_name values that contain non-alphabetical characters or whitespace
SELECT DISTINCT lga_name
FROM nsw_lga_suburb_stg
WHERE lga_name ~ '[^a-zA-Z\s]';

-- DATA TYPE
-- 8. Retrieve column names and data types of columns in the nsw_lga_suburb_stg view
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'nsw_lga_suburb_stg';


-- NUll VALUES CHECK 
-- 9. Count the number of records with null values in the lga_name column
SELECT COUNT(*)
FROM nsw_lga_suburb_stg
WHERE lga_name IS NULL;

-- 10. Count the number of records with null values in the suburb_name column
SELECT COUNT(*)
FROM nsw_lga_suburb_stg
WHERE suburb_name IS NULL;
