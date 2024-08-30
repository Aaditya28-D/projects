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
