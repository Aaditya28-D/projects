-- DATA
-- 1. Retrieve all records from the property_review_stg view
SELECT * FROM property_review_stg;

-- SHAPE OF DATA
-- 2. Count the total number of records in the property_review_stg view
SELECT COUNT(*) AS total_records
FROM property_review_stg;

-- 3. Count the total number of columns in the property_review_stg view
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_name = 'property_review_stg';


-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in each column of property_review_stg view
SELECT
  COUNT(DISTINCT listing_id) AS unique_listing_id,
  COUNT(DISTINCT number_of_reviews) AS unique_number_of_reviews,
  COUNT(DISTINCT review_scores_rating) AS unique_review_scores_rating,
  COUNT(DISTINCT review_scores_accuracy) AS unique_review_scores_accuracy,
  COUNT(DISTINCT review_scores_cleanliness) AS unique_review_scores_cleanliness,
  COUNT(DISTINCT review_scores_checkin) AS unique_review_scores_checkin,
  COUNT(DISTINCT review_scores_communication) AS unique_review_scores_communication,
  COUNT(DISTINCT review_scores_value) AS unique_review_scores_value
FROM property_review_stg;

-- DATA TYPE
-- 6. Retrieve column names and data types of columns in the property_review_stg view
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'property_review_stg';

-- NULL VALUES CHECK
-- 7. Count the number of records with null values in each column of the property_review_stg view
SELECT
  COUNT(*) FILTER (WHERE listing_id IS NULL) AS null_listing_id,
  COUNT(*) FILTER (WHERE number_of_reviews IS NULL) AS null_number_of_reviews,
  COUNT(*) FILTER (WHERE review_scores_rating IS NULL) AS null_review_scores_rating,
  COUNT(*) FILTER (WHERE review_scores_accuracy IS NULL) AS null_review_scores_accuracy,
  COUNT(*) FILTER (WHERE review_scores_cleanliness IS NULL) AS null_review_scores_cleanliness,
  COUNT(*) FILTER (WHERE review_scores_checkin IS NULL) AS null_review_scores_checkin,
  COUNT(*) FILTER (WHERE review_scores_communication IS NULL) AS null_review_scores_communication,
  COUNT(*) FILTER (WHERE review_scores_value IS NULL) AS null_review_scores_value
FROM property_review_stg;
