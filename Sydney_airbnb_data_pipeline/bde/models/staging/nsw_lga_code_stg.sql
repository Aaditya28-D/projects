SELECT 
    lga_code,
    LOWER(lga_name) AS lga_name
 FROM
     raw_schema.nsw_lga_code