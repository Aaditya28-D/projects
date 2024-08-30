-- SELECT * FROM raw_schema.census_g02_nsw_lga_2016

SELECT
    CAST(SUBSTRING(lga_code_2016, 4) AS INTEGER) AS lga_code_2016, 
    median_age_persons,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly,
    average_household_size
FROM raw_schema.census_g02_nsw_lga_2016 