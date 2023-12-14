
-- TASK 1 cast the columns of orders_table 
-- Change the data types to correspond to those seen in the table below.
-- 
-- +------------------+--------------------+--------------------+
-- |   orders_table   | current data type  | required data type |
-- +------------------+--------------------+--------------------+
-- | date_uuid        | TEXT               | UUID               |
-- | user_uuid        | TEXT               | UUID               |
-- | card_number      | TEXT               | VARCHAR(?)         |
-- | store_code       | TEXT               | VARCHAR(?)         |
-- | product_code     | TEXT               | VARCHAR(?)         |
-- | product_quantity | BIGINT             | SMALLINT           |
-- +------------------+--------------------+--------------------+
ALTER TABLE "orders_table" ALTER COLUMN "date_uuid"        TYPE uuid        USING "date_uuid"::uuid;
ALTER TABLE "orders_table" ALTER COLUMN "user_uuid"        TYPE uuid        USING "user_uuid"::uuid;
ALTER TABLE "orders_table" ALTER COLUMN "card_number"      TYPE varchar(19) USING "card_number"::varchar;
ALTER TABLE "orders_table" ALTER COLUMN "product_quantity" TYPE smallint    USING "product_quantity"::smallint;

SELECT LENGTH(store_code), COUNT(*)
FROM orders_table
GROUP BY LENGTH(store_code);

ALTER TABLE "orders_table" ALTER COLUMN "store_code" TYPE varchar(12) USING "store_code"::varchar;

SELECT store_code
FROM orders_table

SELECT LENGTH(product_code), COUNT(*)
FROM orders_table
GROUP BY LENGTH(product_code);

ALTER TABLE "orders_table" ALTER COLUMN "product_code" TYPE varchar(11) USING "product_code"::varchar;

-- Task 2 cast the columns of dim_user_table
-- The columns required to be changed in the users table are as follows:
-- 
-- +----------------+--------------------+--------------------+
-- | dim_user_table | current data type  | required data type |
-- +----------------+--------------------+--------------------+
-- | first_name     | TEXT               | VARCHAR(255)       |
-- | last_name      | TEXT               | VARCHAR(255)       |
-- | date_of_birth  | TEXT               | DATE               |
-- | country_code   | TEXT               | VARCHAR(?)         |
-- | user_uuid      | TEXT               | UUID               |
-- | join_date      | TEXT               | DATE               |
-- +----------------+--------------------+--------------------+

SELECT product_code
FROM orders_table

SELECT LENGTH(country_code), COUNT(*)
FROM dim_users
GROUP BY LENGTH(country_code);

ALTER TABLE "dim_users" ALTER COLUMN "country_code"  TYPE varchar(2)   USING "country_code"::varchar;
ALTER TABLE "dim_users" ALTER COLUMN "first_name"    TYPE varchar(255) USING "first_name"::varchar;
ALTER TABLE "dim_users" ALTER COLUMN "last_name"     TYPE varchar(255) USING "last_name"::varchar;
ALTER TABLE "dim_users" ALTER COLUMN "user_uuid"     TYPE uuid         USING "user_uuid"::uuid
ALTER TABLE "dim_users" ALTER COLUMN "join_date"     TYPE date         USING "join_date"::date;
ALTER TABLE "dim_users" ALTER COLUMN "date_of_birth" TYPE date         USING "date_of_birth"::date;

-- TASK 3 alter the dim_store_details table
-- There are two latitude columns in the store details table. Using SQL, merge one of the columns into the other so you have one latitude column.
-- 
-- Then set the data types for each column as shown below:
-- 
-- +---------------------+-------------------+------------------------+
-- | store_details_table | current data type |   required data type   |
-- +---------------------+-------------------+------------------------+
-- | longitude           | TEXT              | FLOAT                  |
-- | locality            | TEXT              | VARCHAR(255)           |
-- | store_code          | TEXT              | VARCHAR(?)             |
-- | staff_numbers       | TEXT              | SMALLINT               |
-- | opening_date        | TEXT              | DATE                   |
-- | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
-- | latitude            | TEXT              | FLOAT                  |
-- | country_code        | TEXT              | VARCHAR(?)             |
-- | continent           | TEXT              | VARCHAR(255)           |
-- +---------------------+-------------------+------------------------+
-- There is a row that represents the business's website change the location column values where they're null to N/A.

SELECT LENGTH(store_code), COUNT(*)
FROM dim_store_details
GROUP BY LENGTH(store_code);

SELECT "store_code", *
FROM dim_store_details
WHERE store_code LIKE 'WEB%'

SELECT store_code, ISNULL(latitude, 'N/A') AS latitude 
FROM dim_store_details
WHERE store_code LIKE 'WEB%'

ALTER TABLE "dim_store_details" ALTER      COLUMN "store_code"    TYPE varchar(12)  USING "store_code"::varchar;
ALTER TABLE "dim_store_details" ALTER      COLUMN "longitude"     TYPE FLOAT        USING "longitude"::FLOAT        ;
ALTER TABLE "dim_store_details" ALTER      COLUMN "locality"      TYPE VARCHAR(255) USING "locality"::VARCHAR(255)  ;
-- Issue with characters in the staff_numbers column so run 
-- Delete the staff numbers which contain characters (5 in the database)
SELECT staff_numbers, store_code
FROM dim_store_details
WHERE NOT staff_numbers ~ '[a-zA-Z]';

SELECT staff_numbers, store_code
FROM dim_store_details
WHERE NOT staff_numbers ~ '^[0-9]+$';

DELETE FROM dim_store_details
WHERE NOT staff_numbers ~ '^[0-9]+$';

SELECT LENGTH(staff_numbers), COUNT(*)
FROM dim_store_details
GROUP BY LENGTH(staff_numbers);

ALTER TABLE "dim_store_details" ALTER      COLUMN "staff_numbers" TYPE SMALLINT     USING "staff_numbers"::SMALLINT ;
ALTER TABLE "dim_store_details" ALTER      COLUMN "opening_date"  TYPE DATE         USING "opening_date"::DATE      ;
-- Issue with nullable
ALTER TABLE "dim_store_details" 
    ALTER COLUMN "store_type"  TYPE VARCHAR(255) USING "store_type"::VARCHAR(255),
    ALTER COLUMN "store_type"  DROP NOT NULL;
ALTER TABLE "dim_store_details" ALTER COLUMN "latitude"     TYPE FLOAT        USING "latitude"::FLOAT         ;
ALTER TABLE "dim_store_details" ALTER COLUMN "country_code" TYPE VARCHAR(2)   USING "country_code"::VARCHAR   ;
ALTER TABLE "dim_store_details" ALTER COLUMN "continent"    TYPE VARCHAR(255) USING "continent"::VARCHAR(255) ;


SELECT staff_numbers, store_type
FROM dim_store_details 

-- TASK 4 
-- You will need to do some work on the products table before casting the data types correctly.
-- The product_price column has a £ character which you need to remove using SQL.
-- The team that handles the deliveries would like a new human-readable column added for the weight 
--   so they can quickly make decisions on delivery weights.
-- Add a new column weight_class which will contain human-readable values based on the weight range of the product.
-- +--------------------------+-------------------+
-- | weight_class VARCHAR(?)  | weight range(kg)  |
-- +--------------------------+-------------------+
-- | Light                    | < 2               |
-- | Mid_Sized                | >= 2 - < 40       |
-- | Heavy                    | >= 40 - < 140     |
-- | Truck_Required           | => 140            |
-- +----------------------------+-----------------+
SELECT product_price
FROM dim_products

-- Remove the £ from the price
UPDATE dim_products
SET product_price = replace(product_price, '£', '');

-- Alter the table to add the "weight_class" extra column
ALTER TABLE dim_products
ADD COLUMN weight_class varchar(14);
UPDATE dim_products
SET weight_class = 
    CASE WHEN weight_kg < 2    THEN 'Light' 
         WHEN weight_kg >= 2   AND weight_kg < 40   THEN 'Mid_Sized' 
         WHEN weight_kg >= 40  AND weight_kg < 140  THEN 'Heavy' 
         WHEN weight_kg >= 140 THEN 'Truck_Required'  
    END;

SELECT weight_kg,
    CASE WHEN weight_kg < 2  THEN 'Light' 
         WHEN weight_kg >= 2  AND weight_kg < 40   THEN 'Mid_Sized' 
         WHEN weight_kg >= 40 AND weight_kg < 140  THEN 'Heavy' 
         WHEN weight_kg >= 140 THEN 'Truck_Required'  
    END weight_class
FROM dim_products;

-- TASK 5 
-- After all the columns are created and cleaned, change the data types of the products table.
-- You will want to rename the removed column to still_available before changing its data type.
-- Make the changes to the columns to cast them to the following data types:
-- 
-- +-----------------+--------------------+--------------------+
-- |  dim_products   | current data type  | required data type |
-- +-----------------+--------------------+--------------------+
-- | product_price   | TEXT               | FLOAT              |
-- | weight          | TEXT               | FLOAT              |
-- | EAN             | TEXT               | VARCHAR(?)         |
-- | product_code    | TEXT               | VARCHAR(?)         |
-- | date_added      | TEXT               | DATE               |
-- | uuid            | TEXT               | UUID               |
-- | still_available | TEXT               | BOOL               |
-- | weight_class    | TEXT               | VARCHAR(?)         |
-- +-----------------+--------------------+--------------------+

ALTER TABLE "dim_products" ALTER COLUMN "product_price" TYPE FLOAT       USING "product_price" ::FLOAT    ; 
ALTER TABLE "dim_products" ALTER COLUMN "weight_kg"     TYPE FLOAT       USING "weight_kg"     ::FLOAT    ; 

SELECT LENGTH("product_code"), COUNT(*)
FROM dim_products
GROUP BY LENGTH("product_code");
ALTER TABLE "dim_products" ALTER COLUMN "product_code"  TYPE VARCHAR(11) USING "product_code"  ::VARCHAR; 

SELECT LENGTH("EAN"), COUNT(*)
FROM dim_products
GROUP BY LENGTH("EAN");
ALTER TABLE "dim_products" ALTER COLUMN "EAN"           TYPE VARCHAR(17) USING "EAN"           ::VARCHAR; 
ALTER TABLE "dim_products" ALTER COLUMN "date_added"    TYPE DATE        USING "date_added"    ::DATE     ; 
ALTER TABLE "dim_products" ALTER COLUMN "uuid"          TYPE UUID        USING "uuid"          ::UUID     ; 
ALTER TABLE "dim_products" RENAME COLUMN "removed" TO "still_available";
ALTER TABLE "dim_products" ALTER COLUMN "still_available" TYPE BOOL USING CASE WHEN "still_available"='Still_available' THEN TRUE ELSE FALSE END; 
--ALTER TABLE "dim_products" ALTER COLUMN "weight_class"    TYPE VARCHAR(?) USING  "weight_class"   ::VARCHAR; 

SELECT "still_available", "uuid"
FROM dim_products;

-- TASK 6 
-- update the dim_date_times table with the correct types:
-- +-----------------+-------------------+--------------------+
-- | dim_date_times  | current data type | required data type |
-- +-----------------+-------------------+--------------------+
-- | month           | TEXT              | VARCHAR(?)         |
-- | year            | TEXT              | VARCHAR(?)         |
-- | day             | TEXT              | VARCHAR(?)         |
-- | time_period     | TEXT              | VARCHAR(?)         |
-- | date_uuid       | TEXT              | UUID               |
-- +-----------------+-------------------+--------------------+

SELECT LENGTH("month"),       COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("month");
SELECT LENGTH("year"),        COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("year");
SELECT LENGTH("day"),         COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("day");
SELECT LENGTH("time_period"), COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("time_period");

ALTER TABLE "dim_date_times" ALTER COLUMN "month"       TYPE VARCHAR(2)  USING  "month"      ::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "year"        TYPE VARCHAR(4)  USING  "year"       ::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "day"         TYPE VARCHAR(2)  USING  "day"        ::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "time_period" TYPE VARCHAR(10) USING  "time_period"::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "date_uuid"   TYPE UUID        USING  "date_uuid"  ::UUID   ;

SELECT "time_period" FROM "dim_date_times" 


-- TASK 7 
-- +------------------------+-------------------+--------------------+
-- |    dim_card_details    | current data type | required data type |
-- +------------------------+-------------------+--------------------+
-- | card_number            | TEXT              | VARCHAR(?)         |
-- | expiry_date            | TEXT              | VARCHAR(?)         |
-- | date_payment_confirmed | TEXT              | DATE               |
-- +------------------------+-------------------+--------------------+
SELECT LENGTH("card_number"), COUNT(*) FROM "dim_card_details" GROUP BY LENGTH("card_number");
SELECT LENGTH("expiry_date"), COUNT(*) FROM "dim_card_details" GROUP BY LENGTH("expiry_date");

ALTER TABLE "dim_card_details" ALTER COLUMN "card_number"            TYPE VARCHAR(19) USING "card_number"                  ::VARCHAR;
ALTER TABLE "dim_card_details" ALTER COLUMN "expiry_date"            TYPE VARCHAR(5)  USING "expiry_date"                  ::VARCHAR;
ALTER TABLE "dim_card_details" ALTER COLUMN "date_payment_confirmed" TYPE DATE        USING "date_payment_confirmed"::DATE ;

-- TASK 8
-- Now that the tables have the appropriate data types we can begin adding the primary keys to each of the tables prefixed with dim.
-- Each table will serve the orders_table which will be the single source of truth for our orders.
-- Check the column header of the orders_table you will see all but one of the columns exist in one of our tables prefixed with dim.
-- We need to update the columns in the dim tables with a primary key that matches the same column in the orders_table.
-- Using SQL, update the respective columns as primary key columns.

ALTER TABLE "dim_users"         ADD PRIMARY KEY ("user_uuid");
ALTER TABLE "dim_store_details" ADD PRIMARY KEY ("store_code");
ALTER TABLE "dim_products"      ADD PRIMARY KEY ("product_code"); 
ALTER TABLE "dim_date_times"    ADD PRIMARY KEY ("date_uuid");
ALTER TABLE "dim_card_details"  ADD PRIMARY KEY ("card_number");

-- TASK 9 
-- With the primary keys created in the tables prefixed with dim we can now create the foreign keys 
--   in the orders_table to reference the primary keys in the other tables.
-- Use SQL to create those foreign key constraints that reference the primary keys of the other table.
-- This makes the star-based database schema complete.

ALTER TABLE "orders_table"
    ADD CONSTRAINT "user_uuid"    FOREIGN KEY ("user_uuid") 
        REFERENCES dim_users("user_uuid"),
    ADD CONSTRAINT "date_uuid"    FOREIGN KEY ("date_uuid")
        REFERENCES dim_date_times("date_uuid"),
    ADD CONSTRAINT "card_number" FOREIGN KEY ("card_number")
        REFERENCES dim_card_details("card_number"),
    ADD CONSTRAINT "store_code"   FOREIGN KEY ("store_code")
        REFERENCES dim_store_details("store_code"),      
    ADD CONSTRAINT "product_code" FOREIGN KEY ("product_code")
        REFERENCES dim_products("product_code");


--ALTER TABLE "orders_table"
    --DROP CONSTRAINT "user_uuid"


-- Queries to check the tables
SELECT  *
FROM    "orders_table"
FULL JOIN
        "dim_store_details"
ON      orders_table.store_code = dim_store_details.store_code
WHERE   orders_table.store_code IS NULL OR dim_store_details.store_code IS NULL

SELECT store_code 
FROM   "orders_table" l 
WHERE  NOT EXISTS (
   SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
   FROM   "dim_store_details"
   WHERE  store_code = l.store_code
   )
   GROUP BY store_code;

SELECT store_code 
FROM   "dim_store_details" l 
WHERE  NOT EXISTS (
   SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
   FROM   "orders_table"
   WHERE  store_code = l.store_code
   )
   GROUP BY store_code;

SELECT product_code 
FROM   "orders_table" l 
WHERE  NOT EXISTS (
   SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
   FROM   "dim_products"
   WHERE  product_code = l.product_code
   );   

SELECT product_code 
FROM   "dim_products" l 
WHERE  NOT EXISTS (
   SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
   FROM   "orders_table"
   WHERE  product_code = l.product_code
   );   

SELECT user_uuid
FROM   "orders_table" l 
WHERE  NOT EXISTS (
   SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
   FROM   "dim_users"
   WHERE  user_uuid = l.user_uuid
   );   

SELECT LENGTH("card_number"), COUNT(*) FROM "orders_table" GROUP BY LENGTH("card_number");

SELECT card_number 
FROM   "orders_table" l 
WHERE  NOT EXISTS (
   SELECT  card_number 
   FROM   "dim_card_details"
   WHERE  card_number = l.card_number
   );   

SELECT card_number 
FROM   "dim_card_details" l 
WHERE  NOT EXISTS (
   SELECT  card_number 
   FROM   "orders_table"
   WHERE  card_number = l.card_number
   );   

