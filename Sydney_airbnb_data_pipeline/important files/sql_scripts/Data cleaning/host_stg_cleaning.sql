-- DATA
-- 1. Retrieve all records from the host_stg view
SELECT * FROM host_stg;

-- SHAPE OF DATA
-- 2. Count the total number of records in the host_stg view
SELECT COUNT(*) AS total_records
FROM host_stg;

-- 3. Count the total number of columns in the host_stg view
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_name = 'host_stg';

-- TOTAL DUPLICATES IN THE DATA
-- Find records with the same host_id and scraped_date
SELECT host_id, scraped_date, COUNT(*)
FROM host_stg
GROUP BY host_id, scraped_date
HAVING COUNT(*) > 1;


-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in each column of host_stg view
SELECT
  COUNT(DISTINCT host_id) AS unique_host_id,
  COUNT(DISTINCT host_since) AS unique_host_since,
  COUNT(DISTINCT host_is_superhost) AS unique_host_is_superhost,
  COUNT(DISTINCT scraped_date) AS unique_scraped_date,
  COUNT(DISTINCT host_name) AS unique_host_name,
  COUNT(DISTINCT host_neighbourhood) AS unique_host_neighbourhood
FROM host_stg;

-- DATA TYPE
-- 6. Retrieve column names and data types of columns in the host_stg view
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'host_stg';

-- NULL VALUES CHECK
-- 7. Count the number of records with null values in each column of the host_stg view
SELECT
  COUNT(*) FILTER (WHERE host_id IS NULL) AS null_host_id,
  COUNT(*) FILTER (WHERE host_since IS NULL) AS null_host_since,
  COUNT(*) FILTER (WHERE host_is_superhost IS NULL) AS null_host_is_superhost,
  COUNT(*) FILTER (WHERE scraped_date IS NULL) AS null_scraped_date,
  COUNT(*) FILTER (WHERE host_name IS NULL) AS null_host_name,
  COUNT(*) FILTER (WHERE host_neighbourhood IS NULL) AS null_host_neighbourhood
FROM host_stg;

