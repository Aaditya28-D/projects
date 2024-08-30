{{ config(materialized='table') }}

SELECT
    *
FROM
    {{ ref('nsw_lga_suburb_stg') }} 
