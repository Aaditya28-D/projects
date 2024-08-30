-- Drop the 'NSW_LGA_CODE' table in the 'raw_schema' if it exists
DROP TABLE IF EXISTS raw_schema.NSW_LGA_CODE;

-- Create the 'NSW_LGA_CODE' table in the 'raw_schema'
CREATE TABLE raw_schema.NSW_LGA_CODE (
    LGA_CODE INT,
    LGA_NAME VARCHAR(255)
);

-- Insert the values into the 'NSW_LGA_CODE' table
INSERT INTO raw_schema.NSW_LGA_CODE (LGA_CODE, LGA_NAME) VALUES (10050, 'Albury');

-- Select all records from the 'NSW_LGA_CODE' table
SELECT * FROM raw_schema.NSW_LGA_CODE;

-- Delete all records from the 'NSW_LGA_CODE' table in the 'raw_schema'
DELETE FROM raw_schema.NSW_LGA_CODE;

