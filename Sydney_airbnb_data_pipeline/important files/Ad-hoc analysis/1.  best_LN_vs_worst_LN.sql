-- CTE to calculate average estimated revenue per active listing for each neighbourhood
WITH Neighbourhood_Revenue AS (
    SELECT
        listing_neighbourhood_lga,
        SUM(estimated_revenue) AS Total_Revenue,
        COUNT(listing_id) AS Active_Listings,
        SUM(estimated_revenue) / NULLIF(COUNT(listing_id), 0) AS Avg_Revenue_Per_Listing
    FROM 
        dim_property
    WHERE 
        has_availability = 't'
    GROUP BY 
        listing_neighbourhood_lga
),
-- CTE to find the best and worst performing neighbourhoods
Best_Worst_Performing AS (
    SELECT
        listing_neighbourhood_lga,
        Avg_Revenue_Per_Listing,
        RANK() OVER (ORDER BY Avg_Revenue_Per_Listing DESC) as best_rank,
        RANK() OVER (ORDER BY Avg_Revenue_Per_Listing ASC) as worst_rank
    FROM 
        Neighbourhood_Revenue
),
-- CTE to select the best and the worst neighbourhoods
Best_and_Worst_Neighbourhoods AS (
    SELECT listing_neighbourhood_lga
    FROM Best_Worst_Performing
    WHERE best_rank = 1 OR worst_rank = 1
),
-- CTE to get census data for the best and worst neighbourhoods
Census_Data AS (
    SELECT
        g.lga_code,
        g.lga_name,
        g2.median_age_persons,
        g2.median_mortgage_repay_monthly,
        g2.median_tot_prsnl_inc_weekly,
        g2.median_rent_weekly,
        g2.median_tot_fam_inc_weekly,
        g2.average_num_psns_per_bedroom,
        g2.median_tot_hhd_inc_weekly,
        g2.average_household_size
    FROM
        Best_and_Worst_Neighbourhoods bwn
    JOIN
        dim_nsw_lga_code g ON bwn.listing_neighbourhood_lga = g.lga_name
    JOIN
        dim_census_g02_nsw_lga_2016 g2 ON g.lga_code = g2.lga_code_2016
)
-- Final SELECT to output the population differences between the best and worst neighbourhoods
SELECT
    *
FROM
    Census_Data;
