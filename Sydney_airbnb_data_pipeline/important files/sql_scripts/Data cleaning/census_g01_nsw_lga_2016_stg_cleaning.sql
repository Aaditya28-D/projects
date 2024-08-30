-- 1. Retrieve all records from the census_g01_nsw_lga_2016_stg table
SELECT * FROM raw_schema.census_g01_nsw_lga_2016_stg;

-- 2. Count the total number of records in the census_g01_nsw_lga_2016_stg table
SELECT COUNT(*) AS total_records
FROM raw_schema.census_g01_nsw_lga_2016_stg;

-- 3. Count the total number of columns in the census_g01_nsw_lga_2016_stg table
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'raw_schema' AND table_name = 'census_g01_nsw_lga_2016_stg';

-- 4. Find duplicate records in the census_g01_nsw_lga_2016_stg table based on LGA_CODE_2016
SELECT *
FROM raw_schema.census_g01_nsw_lga_2016_stg
WHERE LGA_CODE_2016 IN (
   SELECT LGA_CODE_2016
   FROM raw_schema.census_g01_nsw_lga_2016_stg
   GROUP BY LGA_CODE_2016
   HAVING COUNT(*) > 1
);

-- 5. Count the number of unique LGA_CODE_2016 values
SELECT COUNT(DISTINCT LGA_CODE_2016) AS unique_LGA_codes
FROM raw_schema.census_g01_nsw_lga_2016_stg;

-- 6. Find distinct LGA_CODE_2016 values that contain non-alphanumeric characters or whitespace
SELECT DISTINCT LGA_CODE_2016
FROM raw_schema.census_g01_nsw_lga_2016_stg
WHERE LGA_CODE_2016 ~ '[^a-zA-Z0-9]';


-- 7. Retrieve column names and data types of columns in the census_g01_nsw_lga_2016_stg table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw_schema' AND table_name = 'census_g01_nsw_lga_2016_stg';

-- NULL VALUES CHECK
SELECT *
FROM raw_schema.census_g01_nsw_lga_2016_stg
WHERE Tot_P_M IS NULL
   OR Tot_P_F IS NULL
   OR Tot_P_P IS NULL
   OR Age_0_4_yr_M IS NULL
   OR Age_0_4_yr_F IS NULL
   OR Age_0_4_yr_P IS NULL
   OR Age_5_14_yr_M IS NULL
   OR Age_5_14_yr_F IS NULL
   OR Age_5_14_yr_P IS NULL
   OR Age_15_19_yr_M IS NULL
   OR Age_15_19_yr_F IS NULL
   OR Age_15_19_yr_P IS NULL
   OR Age_20_24_yr_M IS NULL
   OR Age_20_24_yr_F IS NULL
   OR Age_20_24_yr_P IS NULL
   OR Age_25_34_yr_M IS NULL
   OR Age_25_34_yr_F IS NULL
   OR Age_25_34_yr_P IS NULL
   OR Age_35_44_yr_M IS NULL
   OR Age_35_44_yr_F IS NULL
   OR Age_35_44_yr_P IS NULL
   OR Age_45_54_yr_M IS NULL
   OR Age_45_54_yr_F IS NULL
   OR Age_45_54_yr_P IS NULL
   OR Age_55_64_yr_M IS NULL
   OR Age_55_64_yr_F IS NULL
   OR Age_55_64_yr_P IS NULL
   OR Age_65_74_yr_M IS NULL
   OR Age_65_74_yr_F IS NULL
   OR Age_65_74_yr_P IS NULL
   OR Age_75_84_yr_M IS NULL
   OR Age_75_84_yr_F IS NULL
   OR Age_75_84_yr_P IS NULL
   OR Age_85ov_M IS NULL
   OR Age_85ov_F IS NULL
   OR Age_85ov_P IS NULL
   OR Counted_Census_Night_home_M IS NULL
   OR Counted_Census_Night_home_F IS NULL
   OR Counted_Census_Night_home_P IS NULL
   OR Count_Census_Nt_Ewhere_Aust_M IS NULL
   OR Count_Census_Nt_Ewhere_Aust_F IS NULL
   OR Count_Census_Nt_Ewhere_Aust_P IS NULL
   OR Indigenous_psns_Aboriginal_M IS NULL
   OR Indigenous_psns_Aboriginal_F IS NULL
   OR Indigenous_psns_Aboriginal_P IS NULL
   OR Indig_psns_Torres_Strait_Is_M IS NULL
   OR Indig_psns_Torres_Strait_Is_F IS NULL
   OR Indig_psns_Torres_Strait_Is_P IS NULL
   OR Indig_Bth_Abor_Torres_St_Is_M IS NULL
   OR Indig_Bth_Abor_Torres_St_Is_F IS NULL
   OR Indig_Bth_Abor_Torres_St_Is_P IS NULL
   OR Indigenous_P_Tot_M IS NULL
   OR Indigenous_P_Tot_F IS NULL
   OR Indigenous_P_Tot_P IS NULL
   OR Birthplace_Australia_M IS NULL
   OR Birthplace_Australia_F IS NULL
   OR Birthplace_Australia_P IS NULL
   OR Birthplace_Elsewhere_M IS NULL
   OR Birthplace_Elsewhere_F IS NULL
   OR Birthplace_Elsewhere_P IS NULL
   OR Lang_spoken_home_Eng_only_M IS NULL
   OR Lang_spoken_home_Eng_only_F IS NULL
   OR Lang_spoken_home_Eng_only_P IS NULL
   OR Lang_spoken_home_Oth_Lang_M IS NULL
   OR Lang_spoken_home_Oth_Lang_F IS NULL
   OR Lang_spoken_home_Oth_Lang_P IS NULL
   OR Australian_citizen_M IS NULL
   OR Australian_citizen_F IS NULL
   OR Australian_citizen_P IS NULL
   OR Age_psns_att_educ_inst_0_4_M IS NULL
   OR Age_psns_att_educ_inst_0_4_F IS NULL
   OR Age_psns_att_educ_inst_0_4_P IS NULL
   OR Age_psns_att_educ_inst_5_14_M IS NULL
   OR Age_psns_att_educ_inst_5_14_F IS NULL
   OR Age_psns_att_educ_inst_5_14_P IS NULL
   OR Age_psns_att_edu_inst_15_19_M IS NULL
   OR Age_psns_att_edu_inst_15_19_F IS NULL
   OR Age_psns_att_edu_inst_15_19_P IS NULL
   OR Age_psns_att_edu_inst_20_24_M IS NULL
   OR Age_psns_att_edu_inst_20_24_F IS NULL
   OR Age_psns_att_edu_inst_20_24_P IS NULL
   OR Age_psns_att_edu_inst_25_ov_M IS NULL
   OR Age_psns_att_edu_inst_25_ov_F IS NULL
   OR Age_psns_att_edu_inst_25_ov_P IS NULL
   OR High_yr_schl_comp_Yr_12_eq_M IS NULL
   OR High_yr_schl_comp_Yr_12_eq_F IS NULL
   OR High_yr_schl_comp_Yr_12_eq_P IS NULL
   OR High_yr_schl_comp_Yr_11_eq_M IS NULL
   OR High_yr_schl_comp_Yr_11_eq_F IS NULL
   OR High_yr_schl_comp_Yr_11_eq_P IS NULL
   OR High_yr_schl_comp_Yr_10_eq_M IS NULL
   OR High_yr_schl_comp_Yr_10_eq_F IS NULL
   OR High_yr_schl_comp_Yr_10_eq_P IS NULL
   OR High_yr_schl_comp_Yr_9_eq_M IS NULL
   OR High_yr_schl_comp_Yr_9_eq_F IS NULL
   OR High_yr_schl_comp_Yr_9_eq_P IS NULL
   OR High_yr_schl_comp_Yr_8_belw_M IS NULL
   OR High_yr_schl_comp_Yr_8_belw_F IS NULL
   OR High_yr_schl_comp_Yr_8_belw_P IS NULL
   OR High_yr_schl_comp_D_n_g_sch_M IS NULL
   OR High_yr_schl_comp_D_n_g_sch_F IS NULL
   OR High_yr_schl_comp_D_n_g_sch_P IS NULL
   OR Count_psns_occ_priv_dwgs_M IS NULL
   OR Count_psns_occ_priv_dwgs_F IS NULL
   OR Count_psns_occ_priv_dwgs_P IS NULL
   OR Count_Persons_other_dwgs_M IS NULL
   OR Count_Persons_other_dwgs_F IS NULL
   OR Count_Persons_other_dwgs_P IS NULL;
