-- Drop the 'airbnb_guest' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.airbnb_listing_all;

-- Create the 'airbnb_listing' table in the 'raw_schema'
CREATE TABLE raw_schema.airbnb_listing_all (
listing_id	bigint,
scrape_id	bigint,
scraped_date	date,
host_id	bigint,
host_name	character varying,
host_since	date,
host_is_superhost	boolean,
host_neighbourhood	character varying,
listing_neighbourhood	character varying,
property_type	character varying,
room_type	character varying,
accommodates	integer,
price	numeric,
has_availability	boolean,
availability_30	integer,
number_of_reviews	integer,
review_scores_rating	numeric,
review_scores_accuracy	numeric,
review_scores_cleanliness	numeric,
review_scores_checkin	numeric,
review_scores_communication	numeric,
review_scores_value	numeric
);


-- Insert values into the 'airbnb_listing' table
INSERT INTO raw_schema.airbnb_listing_all (LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME, HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD, LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30, NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE) VALUES (
    11156, 2.02101E+13, '2021-01-13', 40855, 'Colleen', '2009-09-23', 'f', 'Potts Point', 'Sydney', 'Private room in apartment', 'Private room', 1, 65, 't', 1, 196, 92, 10, 9, 10, 10, 10
);

-- Select all records from the 'airbnb_listing' table
SELECT * FROM raw_schema.airbnb_listing_all;

-- Delete all records from the 'airbnb_listing' table in the 'raw_schema'
DELETE FROM raw_schema.airbnb_listing_all;


----------------------------------------------------------------------------------------------

-- Drop the 'Census_G01_NSW_LGA_2016' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.Census_G01_NSW_LGA_2016;

-- Create the 'Census_G01_NSW_LGA_2016' table in the 'raw_schema'
CREATE TABLE raw_schema.Census_G01_NSW_LGA_2016 (
    LGA_CODE_2016 VARCHAR(10),
    Tot_P_M INT,
    Tot_P_F INT,
    Tot_P_P INT,
    Age_0_4_yr_M INT,
    Age_0_4_yr_F INT,
    Age_0_4_yr_P INT,
    Age_5_14_yr_M INT,
    Age_5_14_yr_F INT,
    Age_5_14_yr_P INT,
    Age_15_19_yr_M INT,
    Age_15_19_yr_F INT,
    Age_15_19_yr_P INT,
    Age_20_24_yr_M INT,
    Age_20_24_yr_F INT,
    Age_20_24_yr_P INT,
    Age_25_34_yr_M INT,
    Age_25_34_yr_F INT,
    Age_25_34_yr_P INT,
    Age_35_44_yr_M INT,
    Age_35_44_yr_F INT,
    Age_35_44_yr_P INT,
    Age_45_54_yr_M INT,
    Age_45_54_yr_F INT,
    Age_45_54_yr_P INT,
    Age_55_64_yr_M INT,
    Age_55_64_yr_F INT,
    Age_55_64_yr_P INT,
    Age_65_74_yr_M INT,
    Age_65_74_yr_F INT,
    Age_65_74_yr_P INT,
    Age_75_84_yr_M INT,
    Age_75_84_yr_F INT,
    Age_75_84_yr_P INT,
    Age_85ov_M INT,
    Age_85ov_F INT,
    Age_85ov_P INT,
    Counted_Census_Night_home_M INT,
    Counted_Census_Night_home_F INT,
    Counted_Census_Night_home_P INT,
    Count_Census_Nt_Ewhere_Aust_M INT,
    Count_Census_Nt_Ewhere_Aust_F INT,
    Count_Census_Nt_Ewhere_Aust_P INT,
    Indigenous_psns_Aboriginal_M INT,
    Indigenous_psns_Aboriginal_F INT,
    Indigenous_psns_Aboriginal_P INT,
    Indig_psns_Torres_Strait_Is_M INT,
    Indig_psns_Torres_Strait_Is_F INT,
    Indig_psns_Torres_Strait_Is_P INT,
    Indig_Bth_Abor_Torres_St_Is_M INT,
    Indig_Bth_Abor_Torres_St_Is_F INT,
    Indig_Bth_Abor_Torres_St_Is_P INT,
    Indigenous_P_Tot_M INT,
    Indigenous_P_Tot_F INT,
    Indigenous_P_Tot_P INT,
    Birthplace_Australia_M INT,
    Birthplace_Australia_F INT,
    Birthplace_Australia_P INT,
    Birthplace_Elsewhere_M INT,
    Birthplace_Elsewhere_F INT,
    Birthplace_Elsewhere_P INT,
    Lang_spoken_home_Eng_only_M INT,
    Lang_spoken_home_Eng_only_F INT,
    Lang_spoken_home_Eng_only_P INT,
    Lang_spoken_home_Oth_Lang_M INT,
    Lang_spoken_home_Oth_Lang_F INT,
    Lang_spoken_home_Oth_Lang_P INT,
    Australian_citizen_M INT,
    Australian_citizen_F INT,
    Australian_citizen_P INT,
    Age_psns_att_educ_inst_0_4_M INT,
    Age_psns_att_educ_inst_0_4_F INT,
    Age_psns_att_educ_inst_0_4_P INT,
    Age_psns_att_educ_inst_5_14_M INT,
    Age_psns_att_educ_inst_5_14_F INT,
    Age_psns_att_educ_inst_5_14_P INT,
    Age_psns_att_edu_inst_15_19_M INT,
    Age_psns_att_edu_inst_15_19_F INT,
    Age_psns_att_edu_inst_15_19_P INT,
    Age_psns_att_edu_inst_20_24_M INT,
    Age_psns_att_edu_inst_20_24_F INT,
    Age_psns_att_edu_inst_20_24_P INT,
    Age_psns_att_edu_inst_25_ov_M INT,
    Age_psns_att_edu_inst_25_ov_F INT,
    Age_psns_att_edu_inst_25_ov_P INT,
    High_yr_schl_comp_Yr_12_eq_M INT,
    High_yr_schl_comp_Yr_12_eq_F INT,
    High_yr_schl_comp_Yr_12_eq_P INT,
    High_yr_schl_comp_Yr_11_eq_M INT,
    High_yr_schl_comp_Yr_11_eq_F INT,
    High_yr_schl_comp_Yr_11_eq_P INT,
    High_yr_schl_comp_Yr_10_eq_M INT,
    High_yr_schl_comp_Yr_10_eq_F INT,
    High_yr_schl_comp_Yr_10_eq_P INT,
    High_yr_schl_comp_Yr_9_eq_M INT,
    High_yr_schl_comp_Yr_9_eq_F INT,
    High_yr_schl_comp_Yr_9_eq_P INT,
    High_yr_schl_comp_Yr_8_belw_M INT,
    High_yr_schl_comp_Yr_8_belw_F INT,
    High_yr_schl_comp_Yr_8_belw_P INT,
    High_yr_schl_comp_D_n_g_sch_M INT,
    High_yr_schl_comp_D_n_g_sch_F INT,
    High_yr_schl_comp_D_n_g_sch_P INT,
    Count_psns_occ_priv_dwgs_M INT,
    Count_psns_occ_priv_dwgs_F INT,
    Count_psns_occ_priv_dwgs_P INT,
    Count_Persons_other_dwgs_M INT,
    Count_Persons_other_dwgs_F INT,
    Count_Persons_other_dwgs_P INT
);

-- Insert values into the 'Census_G01_NSW_LGA_2016' table
INSERT INTO raw_schema.Census_G01_NSW_LGA_2016 (LGA_CODE_2016, Tot_P_M, Tot_P_F, Tot_P_P, Age_0_4_yr_M, Age_0_4_yr_F, Age_0_4_yr_P, Age_5_14_yr_M, Age_5_14_yr_F, Age_5_14_yr_P, Age_15_19_yr_M, Age_15_19_yr_F, Age_15_19_yr_P, Age_20_24_yr_M, Age_20_24_yr_F, Age_20_24_yr_P, Age_25_34_yr_M, Age_25_34_yr_F, Age_25_34_yr_P, Age_35_44_yr_M, Age_35_44_yr_F, Age_35_44_yr_P, Age_45_54_yr_M, Age_45_54_yr_F, Age_45_54_yr_P, Age_55_64_yr_M, Age_55_64_yr_F, Age_55_64_yr_P, Age_65_74_yr_M, Age_65_74_yr_F, Age_65_74_yr_P, Age_75_84_yr_M, Age_75_84_yr_F, Age_75_84_yr_P, Age_85ov_M, Age_85ov_F, Age_85ov_P, Counted_Census_Night_home_M, Counted_Census_Night_home_F, Counted_Census_Night_home_P, Count_Census_Nt_Ewhere_Aust_M, Count_Census_Nt_Ewhere_Aust_F, Count_Census_Nt_Ewhere_Aust_P, Indigenous_psns_Aboriginal_M, Indigenous_psns_Aboriginal_F, Indigenous_psns_Aboriginal_P, Indig_psns_Torres_Strait_Is_M, Indig_psns_Torres_Strait_Is_F, Indig_psns_Torres_Strait_Is_P, Indig_Bth_Abor_Torres_St_Is_M, Indig_Bth_Abor_Torres_St_Is_F, Indig_Bth_Abor_Torres_St_Is_P, Indigenous_P_Tot_M, Indigenous_P_Tot_F, Indigenous_P_Tot_P, Birthplace_Australia_M, Birthplace_Australia_F, Birthplace_Australia_P, Birthplace_Elsewhere_M, Birthplace_Elsewhere_F, Birthplace_Elsewhere_P, Lang_spoken_home_Eng_only_M, Lang_spoken_home_Eng_only_F, Lang_spoken_home_Eng_only_P, Lang_spoken_home_Oth_Lang_M, Lang_spoken_home_Oth_Lang_F, Lang_spoken_home_Oth_Lang_P, Australian_citizen_M, Australian_citizen_F, Australian_citizen_P, Age_psns_att_educ_inst_0_4_M, Age_psns_att_educ_inst_0_4_F, Age_psns_att_educ_inst_0_4_P, Age_psns_att_educ_inst_5_14_M, Age_psns_att_educ_inst_5_14_F, Age_psns_att_educ_inst_5_14_P, Age_psns_att_edu_inst_15_19_M, Age_psns_att_edu_inst_15_19_F, Age_psns_att_edu_inst_15_19_P, Age_psns_att_edu_inst_20_24_M, Age_psns_att_edu_inst_20_24_F, Age_psns_att_edu_inst_20_24_P, Age_psns_att_edu_inst_25_ov_M, Age_psns_att_edu_inst_25_ov_F, Age_psns_att_edu_inst_25_ov_P, High_yr_schl_comp_Yr_12_eq_M, High_yr_schl_comp_Yr_12_eq_F, High_yr_schl_comp_Yr_12_eq_P, High_yr_schl_comp_Yr_11_eq_M, High_yr_schl_comp_Yr_11_eq_F, High_yr_schl_comp_Yr_11_eq_P, High_yr_schl_comp_Yr_10_eq_M, High_yr_schl_comp_Yr_10_eq_F, High_yr_schl_comp_Yr_10_eq_P, High_yr_schl_comp_Yr_9_eq_M, High_yr_schl_comp_Yr_9_eq_F, High_yr_schl_comp_Yr_9_eq_P, High_yr_schl_comp_Yr_8_belw_M, High_yr_schl_comp_Yr_8_belw_F, High_yr_schl_comp_Yr_8_belw_P, High_yr_schl_comp_D_n_g_sch_M, High_yr_schl_comp_D_n_g_sch_F, High_yr_schl_comp_D_n_g_sch_P, Count_psns_occ_priv_dwgs_M, Count_psns_occ_priv_dwgs_F, Count_psns_occ_priv_dwgs_P, Count_Persons_other_dwgs_M, Count_Persons_other_dwgs_F, Count_Persons_other_dwgs_P) VALUES (
    'LGA10050', 24662, 26411, 51076, 1689, 1594, 3286, 3208, 3117, 6328, 1611, 1635, 3248, 1695, 1810, 3508, 3194, 3299, 6498, 2972, 3228, 6205, 3169, 3329, 6497, 3045, 3327, 6372, 2328, 2573, 4907, 1251, 1659, 2913, 490, 841, 1329, 23024, 24811, 47832, 1639, 1597, 3244, 661, 700, 1363, 19, 15, 29, 12, 8, 23, 687, 725, 1417, 20071, 21514, 41590, 2679, 2866, 5540, 21282, 22839, 44120, 1634, 1808, 3446, 21701, 23328, 45032, 328, 335, 669, 2933, 2868, 5798, 1095, 1167, 2258, 412, 640, 1046, 618, 1140, 1754, 7677, 9423, 17096, 2183, 2231, 4413, 5236, 5180, 10414, 1649, 1639, 3287, 1040, 1036, 2076, 134, 154, 287, 22056, 23627, 45686, 2555, 2523, 5081
);

-- Select all records from the 'Census_G01_NSW_LGA_2016' table
SELECT * FROM raw_schema.Census_G01_NSW_LGA_2016;

DELETE FROM raw_schema.Census_G01_NSW_LGA_2016;



----------------------------------------------------------------------------------------------


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


---------------------------------------------------------------------------------------------


-- Drop the 'NSW_LGA_CODE' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.NSW_LGA_CODE;

-- Create the 'NSW_LGA_CODE' table in the 'raw_schema'
CREATE TABLE raw_schema.NSW_LGA_CODE (
    LGA_CODE INT,
    LGA_NAME VARCHAR(255)
);

-- Insert the values into the 'NSW_LGA_CODE' table
INSERT INTO raw_schema.NSW_LGA_CODE (LGA_CODE, LGA_NAME) VALUES (10050, 'Albury');

-- Select all records from the 'NSW_LGA_CODE' table
SELECT * FROM raw_schema.NSW_LGA_CODE;

-- Delete all records from the 'NSW_LGA_CODE' table in the 'raw_schema'
DELETE FROM raw_schema.NSW_LGA_CODE;



------------------------------------------------------------------------------------------------

-- Drop the 'NSW_LGA_SUBURB' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.NSW_LGA_SUBURB;

-- Create the 'NSW_LGA_SUBURB' table in the 'raw_schema'
CREATE TABLE raw_schema.NSW_LGA_SUBURB (
    LGA_NAME VARCHAR(255),
    SUBURB_NAME VARCHAR(255)
);

-- Insert values into the 'NSW_LGA_SUBURB' table
INSERT INTO raw_schema.NSW_LGA_SUBURB (LGA_NAME, SUBURB_NAME) VALUES ('MID-WESTERN REGIONAL', 'AARONS PASS');

-- Select all records from the 'NSW_LGA_SUBURB' table
SELECT * FROM raw_schema.NSW_LGA_SUBURB;

DELETE FROM raw_schema.NSW_LGA_SUBURB;



