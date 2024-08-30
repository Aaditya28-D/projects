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

   
   
-----------------------------------------------------------------------------------------------------
   
   
   
   
 WITH Revenue_Per_Neighbourhood AS (
    SELECT 
        listing_neighbourhood_lga,
        SUM(estimated_revenue) AS Total_Revenue,
        COUNT(listing_id) AS Active_Listings
    FROM 
        dim_property
    WHERE 
        has_availability = 't'
    GROUP BY 
        listing_neighbourhood_lga
),
Average_Revenue AS (
    SELECT 
        listing_neighbourhood_lga,
        Total_Revenue / NULLIF(Active_Listings, 0) AS Avg_Revenue
    FROM 
        Revenue_Per_Neighbourhood
),
Top_Neighbourhoods AS (
    SELECT 
        listing_neighbourhood_lga
    FROM 
        Average_Revenue
    ORDER BY 
        Avg_Revenue DESC
    LIMIT 5
),
Listing_Analysis AS (
    SELECT 
        p.property_type,
        p.room_type,
        p.accommodates,
        (30 - p.availability_30) AS Number_of_Stays
    FROM 
        dim_property p
    JOIN 
        Top_Neighbourhoods t ON p.listing_neighbourhood_lga = t.listing_neighbourhood_lga
    WHERE 
        p.has_availability = 't'
)
SELECT 
    property_type,
    room_type,
    accommodates,
    AVG(Number_of_Stays) AS Avg_Stays
FROM 
    Listing_Analysis
GROUP BY 
    property_type, room_type, accommodates
ORDER BY 
    Avg_Stays DESC;

   
   
---------------------------------------------------------------------------------------------------
   

   
WITH Host_Residential_LGA AS (
    SELECT
        h.host_id,
        g.lga_name AS Host_LGA
    FROM
        dim_host h
    JOIN
        dim_geographic g ON h.host_neighbourhood_suburb = g.suburb_name
),
Multiple_Listings_Hosts AS (
    SELECT 
        host_id,
        COUNT(listing_id) AS Listing_Count
    FROM 
        dim_property
    GROUP BY 
        host_id
    HAVING 
        COUNT(listing_id) > 1
),
Host_Listing_LGA_Comparison AS (
    SELECT 
        hl.host_id,
        hl.Host_LGA,
        p.listing_neighbourhood_lga AS Listing_LGA,
        (CASE WHEN hl.Host_LGA = p.listing_neighbourhood_lga THEN 1 ELSE 0 END) AS Is_Same_LGA
    FROM 
        Host_Residential_LGA hl
    JOIN 
        Multiple_Listings_Hosts mlh ON hl.host_id = mlh.host_id
    JOIN 
        dim_property p ON hl.host_id = p.host_id
),
LGA_Match_Count AS (
    SELECT 
        host_id,
        SUM(Is_Same_LGA) AS Same_LGA_Count,
        COUNT(*) AS Total_Listings
    FROM 
        Host_Listing_LGA_Comparison
    GROUP BY 
        host_id
)
SELECT 
    host_id,
    Same_LGA_Count,
    Total_Listings,
    (Same_LGA_Count * 100.0 / Total_Listings) AS Same_LGA_Percentage
FROM 
    LGA_Match_Count;

   
   
   
 ------------------------------------------------------------------------------------------------------
   
   
   
   
   
WITH Single_Listing_Hosts AS (
    SELECT
        host_id,
        SUM(estimated_revenue) AS Annual_Revenue
    FROM
        dim_property
    WHERE
        has_availability = 't'
    GROUP BY
        host_id
    HAVING
        COUNT(DISTINCT listing_id) = 1
),
Mortgage_Repayment AS (
    SELECT
        lc.lga_code,
        lc.lga_name,
        c.median_mortgage_repay_monthly * 12 AS Annual_Median_Mortgage_Repayment
    FROM
        dim_census_g02_nsw_lga_2016 c
    JOIN
        dim_nsw_lga_code lc ON c.lga_code_2016 = lc.lga_code
),
Revenue_vs_Mortgage AS (
    SELECT
        slh.host_id,
        slh.Annual_Revenue,
        mr.Annual_Median_Mortgage_Repayment,
        slh.Annual_Revenue >= mr.Annual_Median_Mortgage_Repayment AS Can_Cover_Mortgage,
        p.listing_neighbourhood_lga
    FROM
        Single_Listing_Hosts slh
    JOIN
        dim_property p ON slh.host_id = p.host_id
    JOIN
        Mortgage_Repayment mr ON p.listing_neighbourhood_lga = mr.lga_name
)
SELECT
    host_id,
    Annual_Revenue,
    Annual_Median_Mortgage_Repayment,
    Can_Cover_Mortgage
FROM
    Revenue_vs_Mortgage;

   -------------------------------------------------------------------------------------------