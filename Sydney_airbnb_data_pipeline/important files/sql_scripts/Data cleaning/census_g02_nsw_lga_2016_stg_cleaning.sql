-- DATA
-- 1. Retrieve all records from the census_g02_nsw_lga_2016_stg view
SELECT * FROM raw_schema.census_g02_nsw_lga_2016_stg;

-- SHAPE OF DATA
-- 2. Count the total number of records in the census_g02_nsw_lga_2016_stg view
SELECT COUNT(*) AS total_records
FROM raw_schema.census_g02_nsw_lga_2016_stg;

-- 3. Count the total number of columns in the census_g02_nsw_lga_2016_stg view
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'raw_schema' AND table_name = 'census_g02_nsw_lga_2016_stg';

-- TOTAL DUPLICATES IN THE DATA
-- 4. Find duplicate records in the census_g02_nsw_lga_2016_stg view based on LGA_CODE_2016
SELECT *
FROM raw_schema.census_g02_nsw_lga_2016_stg
WHERE (LGA_CODE_2016) IN (
   SELECT LGA_CODE_2016
   FROM raw_schema.census_g02_nsw_lga_2016_stg
   GROUP BY LGA_CODE_2016
   HAVING COUNT(*) > 1
);

-- TOTAL NUMBER OF DISTINCT VALUES IN EACH COLUMN
-- 5. Count the number of unique values in LGA_CODE_2016 column
SELECT
  COUNT(DISTINCT LGA_CODE_2016) AS unique_LGA_CODE_2016,
  COUNT(DISTINCT Median_age_persons) AS unique_Median_age_persons,
  COUNT(DISTINCT Median_mortgage_repay_monthly) AS unique_Median_mortgage_repay_monthly,
  COUNT(DISTINCT Median_tot_prsnl_inc_weekly) AS unique_Median_tot_prsnl_inc_weekly,
  COUNT(DISTINCT Median_rent_weekly) AS unique_Median_rent_weekly,
  COUNT(DISTINCT Median_tot_fam_inc_weekly) AS unique_Median_tot_fam_inc_weekly,
  COUNT(DISTINCT Average_num_psns_per_bedroom) AS unique_Average_num_psns_per_bedroom,
  COUNT(DISTINCT Median_tot_hhd_inc_weekly) AS unique_Median_tot_hhd_inc_weekly,
  COUNT(DISTINCT Average_household_size) AS unique_Average_household_size
FROM raw_schema.census_g02_nsw_lga_2016_stg;

-- DATA TYPE
-- 7. Retrieve column names and data types of columns in the census_g02_nsw_lga_2016_stg view
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw_schema' AND table_name = 'census_g02_nsw_lga_2016_stg';

 -- NULL VALUE CHECK 
  SELECT
  COUNT(*) FILTER (WHERE LGA_CODE_2016 IS NULL) AS null_LGA_CODE_2016,
  COUNT(*) FILTER (WHERE Median_age_persons IS NULL) AS null_Median_age_persons,
  COUNT(*) FILTER (WHERE Median_mortgage_repay_monthly IS NULL) AS null_Median_mortgage_repay_monthly,
  COUNT(*) FILTER (WHERE Median_tot_prsnl_inc_weekly IS NULL) AS null_Median_tot_prsnl_inc_weekly,
  COUNT(*) FILTER (WHERE Median_rent_weekly IS NULL) AS null_Median_rent_weekly,
  COUNT(*) FILTER (WHERE Median_tot_fam_inc_weekly IS NULL) AS null_Median_tot_fam_inc_weekly,
  COUNT(*) FILTER (WHERE Average_num_psns_per_bedroom IS NULL) AS null_Average_num_psns_per_bedroom,
  COUNT(*) FILTER (WHERE Median_tot_hhd_inc_weekly IS NULL) AS null_Median_tot_hhd_inc_weekly,
  COUNT(*) FILTER (WHERE Average_household_size IS NULL) AS null_Average_household_size
FROM raw_schema.census_g02_nsw_lga_2016_stg;

