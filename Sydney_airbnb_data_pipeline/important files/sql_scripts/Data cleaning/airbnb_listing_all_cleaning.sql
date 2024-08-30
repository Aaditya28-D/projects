-- DATA
-- 1. Retrieve all records from the airbnb_listing_all table
SELECT * FROM raw_schema.airbnb_listing_all;

-- SHAPE OF DATA
-- 2. Count the total number of records in the airbnb_listing_all table
SELECT COUNT(*) AS total_records
FROM raw_schema.airbnb_listing_all;

-- 3. Count the total number of columns in the airbnb_listing_all table
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'raw_schema' AND table_name = 'airbnb_listing_all';

-- TOTAL DUPLICATES IN THE DATA
-- Find records with the same listing_id and scraped_date
SELECT listing_id, scraped_date, COUNT(*)
FROM raw_schema.airbnb_listing_all
GROUP BY listing_id, scraped_date
HAVING COUNT(*) > 1;

-- Retrieve duplicate records for the listing_id 40763396 in the airbnb_listing_all table
SELECT *
FROM raw_schema.airbnb_listing_all
WHERE listing_id = 40763396;


-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in each column of airbnb_listing_all table
SELECT
  COUNT(DISTINCT listing_id) AS unique_listing_id,
  COUNT(DISTINCT scrape_id) AS unique_scrape_id,
  COUNT(DISTINCT scraped_date) AS unique_scraped_date,
  COUNT(DISTINCT host_id) AS unique_host_id,
  COUNT(DISTINCT host_name) AS unique_host_name,
  COUNT(DISTINCT host_since) AS unique_host_since,
  COUNT(DISTINCT host_is_superhost) AS unique_host_is_superhost,
  COUNT(DISTINCT host_neighbourhood) AS unique_host_neighbourhood,
  COUNT(DISTINCT listing_neighbourhood) AS unique_listing_neighbourhood,
  COUNT(DISTINCT property_type) AS unique_property_type,
  COUNT(DISTINCT room_type) AS unique_room_type,
  COUNT(DISTINCT accommodates) AS unique_accommodates,
  COUNT(DISTINCT price) AS unique_price,
  COUNT(DISTINCT has_availability) AS unique_has_availability,
  COUNT(DISTINCT availability_30) AS unique_availability_30,
  COUNT(DISTINCT number_of_reviews) AS unique_number_of_reviews,
  COUNT(DISTINCT review_scores_rating) AS unique_review_scores_rating,
  COUNT(DISTINCT review_scores_accuracy) AS unique_review_scores_accuracy,
  COUNT(DISTINCT review_scores_cleanliness) AS unique_review_scores_cleanliness,
  COUNT(DISTINCT review_scores_checkin) AS unique_review_scores_checkin,
  COUNT(DISTINCT review_scores_communication) AS unique_review_scores_communication,
  COUNT(DISTINCT review_scores_value) AS unique_review_scores_value
FROM raw_schema.airbnb_listing_all;

-- DATA TYPE
-- 6. Retrieve column names and data types of columns in the airbnb_listing_all table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw_schema' AND table_name = 'airbnb_listing_all';


-- NULL VALUES CHECK
-- 7. Count the number of records with null values in each column of the airbnb_listing_all table
SELECT
  COUNT(*) FILTER (WHERE listing_id IS NULL) AS null_listing_id,
  COUNT(*) FILTER (WHERE scrape_id IS NULL) AS null_scrape_id,
  COUNT(*) FILTER (WHERE scraped_date IS NULL) AS null_scraped_date,
  COUNT(*) FILTER (WHERE host_id IS NULL) AS null_host_id,
  COUNT(*) FILTER (WHERE host_name IS NULL) AS null_host_name,
  COUNT(*) FILTER (WHERE host_since IS NULL) AS null_host_since,
  COUNT(*) FILTER (WHERE host_is_superhost IS NULL) AS null_host_is_superhost,
  COUNT(*) FILTER (WHERE host_neighbourhood IS NULL) AS null_host_neighbourhood,
  COUNT(*) FILTER (WHERE listing_neighbourhood IS NULL) AS null_listing_neighbourhood,
  COUNT(*) FILTER (WHERE property_type IS NULL) AS null_property_type,
  COUNT(*) FILTER (WHERE room_type IS NULL) AS null_room_type,
  COUNT(*) FILTER (WHERE accommodates IS NULL) AS null_accommodates,
  COUNT(*) FILTER (WHERE price IS NULL) AS null_price,
  COUNT(*) FILTER (WHERE has_availability IS NULL) AS null_has_availability,
  COUNT(*) FILTER (WHERE availability_30 IS NULL) AS null_availability_30,
  COUNT(*) FILTER (WHERE number_of_reviews IS NULL) AS null_number_of_reviews,
  COUNT(*) FILTER (WHERE review_scores_rating IS NULL) AS null_review_scores_rating,
  COUNT(*) FILTER (WHERE review_scores_accuracy IS NULL) AS null_review_scores_accuracy,
  COUNT(*) FILTER (WHERE review_scores_cleanliness IS NULL) AS null_review_scores_cleanliness,
  COUNT(*) FILTER (WHERE review_scores_checkin IS NULL) AS null_review_scores_checkin,
  COUNT(*) FILTER (WHERE review_scores_communication IS NULL) AS null_review_scores_communication,
  COUNT(*) FILTER (WHERE review_scores_value IS NULL) AS null_review_scores_value
FROM raw_schema.airbnb_listing_all;





