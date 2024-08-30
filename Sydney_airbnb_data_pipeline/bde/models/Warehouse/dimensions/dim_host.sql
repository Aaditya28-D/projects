{{ config(materialized='table') }}

SELECT
   *
FROM
    {{ ref('host_stg') }}
