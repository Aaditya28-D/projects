-- Drop the 'Census_G02_NSW_LGA_2016' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.Census_G02_NSW_LGA_2016;

-- Create the 'Census_G02_NSW_LGA_2016' table in the 'raw_schema'
CREATE TABLE raw_schema.Census_G02_NSW_LGA_2016 (
    LGA_CODE_2016 VARCHAR(10),
    Median_age_persons INT,
    Median_mortgage_repay_monthly INT,
    Median_tot_prsnl_inc_weekly INT,
    Median_rent_weekly INT,
    Median_tot_fam_inc_weekly INT,
    Average_num_psns_per_bedroom FLOAT,
    Median_tot_hhd_inc_weekly INT,
    Average_household_size FLOAT
);

-- Insert values into the 'Census_G02_NSW_LGA_2016' table
INSERT INTO raw_schema.Census_G02_NSW_LGA_2016 (LGA_CODE_2016, Median_age_persons, Median_mortgage_repay_monthly, Median_tot_prsnl_inc_weekly, Median_rent_weekly, Median_tot_fam_inc_weekly, Average_num_psns_per_bedroom, Median_tot_hhd_inc_weekly, Average_household_size) VALUES ('LGA10050', 39, 1421, 642, 231, 1532, 0.8, 1185, 2.3);

-- Select all records from the 'Census_G02_NSW_LGA_2016' table
SELECT * FROM raw_schema.Census_G02_NSW_LGA_2016;

DELETE FROM raw_schema.Census_G02_NSW_LGA_2016;


