-- Drop the 'NSW_LGA_SUBURB' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.NSW_LGA_SUBURB;

-- Create the 'NSW_LGA_SUBURB' table in the 'raw_schema'
CREATE TABLE raw_schema.NSW_LGA_SUBURB (
    LGA_NAME VARCHAR(255),
    SUBURB_NAME VARCHAR(255)
);

-- Insert values into the 'NSW_LGA_SUBURB' table
INSERT INTO raw_schema.NSW_LGA_SUBURB (LGA_NAME, SUBURB_NAME) VALUES ('MID-WESTERN REGIONAL', 'AARONS PASS');

-- Select all records from the 'NSW_LGA_SUBURB' table
SELECT * FROM raw_schema.NSW_LGA_SUBURB;

DELETE FROM raw_schema.NSW_LGA_SUBURB;