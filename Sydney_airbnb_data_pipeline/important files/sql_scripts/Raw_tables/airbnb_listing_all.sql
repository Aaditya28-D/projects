-- Drop the 'airbnb_guest' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.airbnb_listing_all;

-- Create the 'airbnb_listing' table in the 'raw_schema'
CREATE TABLE raw_schema.airbnb_listing_all (
listing_id	bigint,
scrape_id	bigint,
scraped_date	date,
host_id	bigint,
host_name	character varying,
host_since	date,
host_is_superhost	boolean,
host_neighbourhood	character varying,
listing_neighbourhood	character varying,
property_type	character varying,
room_type	character varying,
accommodates	integer,
price	numeric,
has_availability	boolean,
availability_30	integer,
number_of_reviews	integer,
review_scores_rating	numeric,
review_scores_accuracy	numeric,
review_scores_cleanliness	numeric,
review_scores_checkin	numeric,
review_scores_communication	numeric,
review_scores_value	numeric
);


-- Insert values into the 'airbnb_listing' table
INSERT INTO raw_schema.airbnb_listing_all (LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME, HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD, LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30, NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE) VALUES (
    11156, 2.02101E+13, '2021-01-13', 40855, 'Colleen', '2009-09-23', 'f', 'Potts Point', 'Sydney', 'Private room in apartment', 'Private room', 1, 65, 't', 1, 196, 92, 10, 9, 10, 10, 10
);

-- Select all records from the 'airbnb_listing' table
SELECT * FROM raw_schema.airbnb_listing_all;

-- Delete all records from the 'airbnb_listing' table in the 'raw_schema'
DELETE FROM raw_schema.airbnb_listing_all;
