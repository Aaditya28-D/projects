UPDATE raw_schema.listing_snapshot AS t1
SET dbt_valid_to = t2.next_date
FROM (
    SELECT 
        listing_id, 
        scraped_date, 
        LEAD(scraped_date) OVER (PARTITION BY listing_id ORDER BY scraped_date) AS next_date
    FROM raw_schema.listing_snapshot
) AS t2
WHERE t1.listing_id = t2.listing_id AND t1.scraped_date = t2.scraped_date;

select*from raw_schema.listing_snapshot;

UPDATE raw_schema.property_snapshot AS t1
SET dbt_valid_to = t2.next_date
FROM (
    SELECT 
        listing_id, 
        scraped_date, 
        LEAD(scraped_date) OVER (PARTITION BY listing_id ORDER BY scraped_date) AS next_date
    FROM raw_schema.property_snapshot
) AS t2
WHERE t1.listing_id = t2.listing_id AND t1.scraped_date = t2.scraped_date;


select*from raw_schema.property_snapshot;

UPDATE raw_schema.host_snapshot AS t1
SET dbt_valid_to = t2.next_date
FROM (
    SELECT 
        host_id, 
        scraped_date, 
        LEAD(scraped_date) OVER (PARTITION BY host_id ORDER BY scraped_date) AS next_date
    FROM raw_schema.host_snapshot
) AS t2
WHERE t1.host_id = t2.host_id AND t1.scraped_date = t2.scraped_date;

select*from raw_schema.host_snapshot;