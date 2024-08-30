{{ config(materialized='table') }}

SELECT
    *
FROM
    {{ ref('census_g02_nsw_lga_2016_stg') }}
