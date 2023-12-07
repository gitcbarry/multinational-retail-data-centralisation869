

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


SELECT LENGTH(store_code), COUNT(*)
FROM dim_store_details
GROUP BY LENGTH(store_code);

SELECT LENGTH(country_code), COUNT(*)
FROM dim_store_details
GROUP BY LENGTH(country_code);

ALTER TABLE "dim_store_details" ALTER      COLUMN "store_code"    TYPE varchar(11)  USING "store_code"::varchar;
ALTER TABLE "dim_store_details" ALTER      COLUMN "longitude"     TYPE FLOAT        USING "longitude"::FLOAT        ;
ALTER TABLE "dim_store_details" ALTER      COLUMN "locality"      TYPE VARCHAR(255) USING "locality"::VARCHAR(255)  ;
--    Issue with                characters in     the             column
ALTER TABLE "dim_store_details" ALTER      COLUMN "staff_numbers" TYPE SMALLINT     USING "staff_numbers"::SMALLINT ;
ALTER TABLE "dim_store_details" ALTER      COLUMN "opening_date"  TYPE DATE         USING "opening_date"::DATE      ;
-- Issue with nullable
ALTER TABLE "dim_store_details" 
ALTER COLUMN "store_type"  TYPE VARCHAR(255) USING "store_type"::VARCHAR(255),
ALTER COLUMN "store_type"  DROP NOT NULL;
ALTER TABLE "dim_store_details" ALTER COLUMN "latitude"     TYPE FLOAT        USING "latitude"::FLOAT         ;
ALTER TABLE "dim_store_details" ALTER COLUMN "country_code" TYPE VARCHAR(2)   USING "country_code"::VARCHAR   ;
ALTER TABLE "dim_store_details" ALTER COLUMN "continent"    TYPE VARCHAR(255) USING "continent"::VARCHAR(255) ;


SELECT LENGTH(staff_numbers), COUNT(*)
FROM dim_store_details
GROUP BY LENGTH(staff_numbers);

SELECT staff_numbers, store_code
FROM dim_store_details
WHERE NOT staff_numbers ~ '[a-zA-Z]';

SELECT staff_numbers, store_code
FROM dim_store_details
WHERE NOT staff_numbers ~ '^[0-9]+$';

-- Delete the staff numbers which contain characters (5 in the database)
DELETE FROM dim_store_details
WHERE NOT staff_numbers ~ '^[0-9]+$';

SELECT staff_numbers, store_type
FROM dim_store_details 

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
FROM dim_products


ALTER TABLE "dim_products" ALTER COLUMN "product_price" TYPE FLOAT       USING "product_price" ::FLOAT    ; 
ALTER TABLE "dim_products" ALTER COLUMN "weight_kg"     TYPE FLOAT       USING "weight_kg"     ::FLOAT    ; 

ALTER TABLE "dim_products" ALTER COLUMN "product_code"  TYPE VARCHAR(11) USING "product_code"  ::VARCHAR; 
ALTER TABLE "dim_products" ALTER COLUMN "EAN"           TYPE VARCHAR(17) USING "EAN"           ::VARCHAR; 
ALTER TABLE "dim_products" ALTER COLUMN "date_added"    TYPE DATE        USING "date_added"    ::DATE     ; 
ALTER TABLE "dim_products" ALTER COLUMN "uuid"          TYPE UUID        USING "uuid"          ::UUID     ; 
ALTER TABLE "dim_products" RENAME COLUMN "removed" TO "still_available";
ALTER TABLE "dim_products" ALTER COLUMN "still_available" TYPE BOOL USING CASE WHEN "still_available"='Still_available' THEN TRUE ELSE FALSE END; 
--ALTER TABLE "dim_products" ALTER COLUMN "weight_class"    TYPE VARCHAR(?) USING  "weight_class"   ::VARCHAR; 


SELECT weight_kg, weight_class
FROM dim_products

SELECT LENGTH("EAN"), COUNT(*)
FROM dim_products
GROUP BY LENGTH("EAN");

SELECT LENGTH("product_code"), COUNT(*)
FROM dim_products
GROUP BY LENGTH("product_code");

SELECT "still_available", "uuid"
FROM dim_products;

ALTER TABLE "dim_date_times" ALTER COLUMN "month"       TYPE VARCHAR(2)  USING  "month"      ::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "year"        TYPE VARCHAR(4)  USING  "year"       ::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "day"         TYPE VARCHAR(2)  USING  "day"        ::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "time_period" TYPE VARCHAR(10) USING  "time_period"::VARCHAR   ;     
ALTER TABLE "dim_date_times" ALTER COLUMN "date_uuid"   TYPE UUID        USING  "date_uuid"  ::UUID   ;

SELECT LENGTH("month"),       COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("month");
SELECT LENGTH("year"),        COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("year");
SELECT LENGTH("day"),         COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("day");
SELECT LENGTH("time_period"), COUNT(*) FROM "dim_date_times" GROUP BY LENGTH("time_period");
SELECT "time_period" FROM "dim_date_times" 

SELECT LENGTH("card_number"), COUNT(*) FROM "dim_card_details" GROUP BY LENGTH("card_number");
SELECT LENGTH("expiry_date"), COUNT(*) FROM "dim_card_details" GROUP BY LENGTH("expiry_date");

ALTER TABLE "dim_card_details" ALTER COLUMN "card_number"            TYPE VARCHAR(20) USING "card_number"                  ::VARCHAR;
ALTER TABLE "dim_card_details" ALTER COLUMN "expiry_date"            TYPE VARCHAR(5)  USING "expiry_date"                  ::VARCHAR;
ALTER TABLE "dim_card_details" ALTER COLUMN "date_payment_confirmed" TYPE DATE        USING "date_payment_confirmed"::DATE ;


ALTER TABLE "dim_users"         ADD PRIMARY KEY ("user_uuid");
ALTER TABLE "dim_store_details" ADD PRIMARY KEY ("store_code");
ALTER TABLE "dim_products"      ADD PRIMARY KEY ("product_code"); 
ALTER TABLE "dim_date_times"    ADD PRIMARY KEY ("date_uuid");
ALTER TABLE "dim_card_details"  ADD PRIMARY KEY ("card_number");

