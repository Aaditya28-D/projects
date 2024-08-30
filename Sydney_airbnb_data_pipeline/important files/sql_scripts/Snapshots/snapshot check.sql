-- This should ideally be zero, indicating all records in the main table have a corresponding record in the snapshot table.
SELECT COUNT(*) 
FROM airbnb_listing_all_1 
WHERE listing_id NOT IN (SELECT DISTINCT listing_id FROM listing_snapshot);

SELECT s1.listing_id, s1.dbt_valid_from, s1.dbt_valid_to
FROM listing_snapshot s1
JOIN listing_snapshot s2 
ON s1.listing_id = s2.listing_id 
WHERE s1.dbt_valid_from < s2.dbt_valid_to 
AND s1.dbt_valid_to > s2.dbt_valid_from 
AND s1.dbt_valid_from <> s2.dbt_valid_from
LIMIT 10;

CREATE TEMP TABLE OverlapsTemp AS
SELECT s1.listing_id, s1.dbt_valid_from, s1.dbt_valid_to
FROM listing_snapshot s1
JOIN listing_snapshot s2 
ON s1.listing_id = s2.listing_id 
WHERE s1.dbt_valid_from < s2.dbt_valid_to 
AND s1.dbt_valid_to > s2.dbt_valid_from 
AND s1.dbt_valid_from <> s2.dbt_valid_from;

SELECT * FROM OverlapsTemp;

SELECT listing_id, dbt_valid_from, COUNT(*)
FROM listing_snapshot
GROUP BY listing_id, dbt_valid_from
HAVING COUNT(*) > 1

SELECT listing_id, COUNT(*)
FROM listing_snapshot
WHERE dbt_valid_to IS NULL
GROUP BY listing_id
HAVING COUNT(*) > 1

-- This should ideally return zero, indicating every unique record in the main table has one and only one corresponding latest version in the snapshot table.
WITH LatestRecords AS (
    SELECT listing_id, COUNT(*) as count
    FROM listing_snapshot
    WHERE dbt_valid_to IS NULL
    GROUP BY listing_id
)
SELECT COUNT(*) 
FROM LatestRecords 
WHERE count > 1;

