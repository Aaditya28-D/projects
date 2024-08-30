{{ config(materialized='table') }}

SELECT
  *
FROM
    {{ ref('census_g01_nsw_lga_2016_stg') }}
