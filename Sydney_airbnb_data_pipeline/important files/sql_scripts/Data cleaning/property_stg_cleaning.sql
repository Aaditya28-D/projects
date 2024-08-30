-- DATA
-- 1. Retrieve all records from the property_stg view
SELECT * FROM property_stg;

-- SHAPE OF DATA
-- 2. Count the total number of records in the property_stg view
SELECT COUNT(*) AS total_records
FROM property_stg;

-- 3. Count the total number of columns in the property_stg view
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_name = 'property_stg';

-- TOTAL DUPLICATES IN THE DATA
-- Find records with the same listing_id and scraped_date
SELECT listing_id, scraped_date, COUNT(*)
FROM property_stg
GROUP BY listing_id, scraped_date
HAVING COUNT(*) > 1;

-- Retrieve duplicate records for the listing_id 40763396 in the property_stg view
SELECT *
FROM property_stg
WHERE listing_id = 40763396;

-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in each column of property_stg view
SELECT
  COUNT(DISTINCT listing_id) AS unique_listing_id,
  COUNT(DISTINCT scrape_id) AS unique_scrape_id,
  COUNT(DISTINCT scraped_date) AS unique_scraped_date,
  COUNT(DISTINCT listing_neighbourhood) AS unique_listing_neighbourhood,
  COUNT(DISTINCT property_type) AS unique_property_type,
  COUNT(DISTINCT room_type) AS unique_room_type,
  COUNT(DISTINCT accommodates) AS unique_accommodates,
  COUNT(DISTINCT price) AS unique_price,
  COUNT(DISTINCT has_availability) AS unique_has_availability,
  COUNT(DISTINCT availability_30) AS unique_availability_30,
  COUNT(DISTINCT host_id) AS unique_host_id
FROM property_stg;

-- DATA TYPE
-- 6. Retrieve column names and data types of columns in the property_stg view
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'property_stg';

-- NULL VALUES CHECK
-- 7. Count the number of records with null values in each column of the property_stg view
SELECT
  COUNT(*) FILTER (WHERE listing_id IS NULL) AS null_listing_id,
  COUNT(*) FILTER (WHERE scrape_id IS NULL) AS null_scrape_id,
  COUNT(*) FILTER (WHERE scraped_date IS NULL) AS null_scraped_date,
  COUNT(*) FILTER (WHERE listing_neighbourhood IS NULL) AS null_listing_neighbourhood,
  COUNT(*) FILTER (WHERE property_type IS NULL) AS null_property_type,
  COUNT(*) FILTER (WHERE room_type IS NULL) AS null_room_type,
  COUNT(*) FILTER (WHERE accommodates IS NULL) AS null_accommodates,
  COUNT(*) FILTER (WHERE price IS NULL) AS null_price,
  COUNT(*) FILTER (WHERE has_availability IS NULL) AS null_has_availability,
  COUNT(*) FILTER (WHERE availability_30 IS NULL) AS null_availability_30,
  COUNT(*) FILTER (WHERE host_id IS NULL) AS null_host_id
FROM property_stg;
