-- Task 1 -- DONE Incorrect values by a one (one GB is a web store)
-- The Operations team would like to know which countries we currently operate in and which country now has the most stores. 
-- Perform a query on the database to get the information, it should return the following information:
-- +----------+-----------------+
-- | country  | total_no_stores |  country_code | total_no_stores
-- +----------+-----------------+ --------------+----------------
-- | GB       |             265 |  GB           |             266
-- | DE       |             141 |  DE           |             141
-- | US       |              34 |  US           |              34
-- +----------+-----------------+

SELECT "country_code", COUNT(*) AS "total_no_stores"
FROM "dim_store_details"
WHERE "store_code" NOT LIKE 'WEB%'
GROUP BY "country_code"
ORDER BY "total_no_stores" desc; 
--SELECT "country", COUNT(*) AS "total_no_stores"  FROM "dim_users" GROUP BY "country" ORDER BY "total_no_stores" desc; 

-- Task 2 -- DONE
-- Which locations currently have the most stores.
-- +-------------------+-----------------+
-- |     locality      | total_no_stores |
-- +-------------------+-----------------+
-- | Chapletown        |              14 |
-- | Belper            |              13 |
-- | Bushley           |              12 |
-- | Exeter            |              11 |
-- | High Wycombe      |              10 |
-- | Arbroath          |              10 |
-- | Rutherglen        |              10 |
-- +-------------------+-----------------+
SELECT "locality", COUNT(*) AS "total_no_stores" 
FROM "dim_store_details" 
GROUP BY "locality" 
ORDER BY "total_no_stores" desc
FETCH FIRST 7 ROWS ONLY; 


-- Task 3 -- DONE (values not quite right -- fixed tables values correct )
-- Which months have produced the most sales.
-- +-------------+-------+
-- | total_sales | month |  total_sale_price | month 
-- +-------------+-------+ ------------------+-------
-- |   673295.68 |     8 |         673295.68 | 8     
-- |   668041.45 |     1 |         668041.45 | 1     
-- |   657335.84 |    10 |         657335.84 | 10    
-- |   650321.43 |     5 |         650321.43 | 5     
-- |   645741.70 |     7 |         645741.70 | 7     
-- |   645463.00 |     3 |         645463.00 | 3     
-- +-------------+-------+

SELECT  ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS "total_sale_price",
        dim_date_times.month
FROM orders_table
INNER JOIN 
    dim_products ON dim_products.product_code = orders_table.product_code 
INNER JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY
       dim_date_times.month
ORDER BY "total_sale_price" DESC
FETCH FIRST 6 ROWS ONLY; 

-- Task 4 --  HMM not sure how to get that table format
-- DONE not sure that this is the most efficient way
-- The company is looking to increase its online sales.
-- They want to know how many sales are happening online vs offline.
-- Calculate how many products were sold and the amount of sales made for online and offline purchases.
-- You should get the following information:
--
-- +------------------+-------------------------+----------+
-- | numbers_of_sales | product_quantity_count  | location |
-- +------------------+-------------------------+----------+
-- |            26957 |                  107739 | Web      |
-- |            93166 |                  374047 | Offline  |
-- +------------------+-------------------------+----------+

WITH web_product_num 
AS (
    SELECT 
        COUNT(store_code) FILTER (WHERE store_code LIKE 'WEB%') AS web_total_sales,
        SUM(product_quantity) FILTER (WHERE store_code LIKE 'WEB%') AS web_total_product,
        'Web' AS location
    FROM orders_table
),
offline_product_num
AS (
    SELECT 
        COUNT(store_code) FILTER (WHERE store_code NOT LIKE 'WEB%') AS offline_total_sales,
        SUM(product_quantity) FILTER (WHERE store_code NOT LIKE 'WEB%') AS offline_total_product,
        'Offline' AS location
    FROM orders_table
    )
SELECT web_total_sales AS numbers_of_sales, web_total_product AS product_quantity_count, location 
FROM web_product_num
UNION
SELECT offline_total_sales AS numbers_of_sales, offline_total_product AS product_quantity_count, location
FROM offline_product_num;

-- Task 5 -- DONE (what a nightmare) numbers slightly off (percentages incorrect on table -- numbers match)
-- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
-- +-------------+-------------+---------------------+
-- | store_type  | total_sales | percentage_total(%) |  store_type  |   round    | percent
-- +-------------+-------------+---------------------+ -------------+------------+--------
-- | Local       |  3440896.52 |               44.87 |  Local       | 3440896.52 |   44.56
-- | Web portal  |  1726547.05 |               22.44 |  Web Portal  | 1726547.05 |   22.36
-- | Super Store |  1224293.65 |               15.63 |  Super Store | 1224293.65 |   15.85
-- | Mall Kiosk  |   698791.61 |                8.96 |  Mall Kiosk  |  698791.61 |    9.05
-- | Outlet      |   631804.81 |                8.10 |  Outlet      |  631804.81 |    8.18
-- +-------------+-------------+---------------------+

WITH 
store_sales 
AS ( SELECT 
    "store_type", sum(orders_table.product_quantity * dim_products.product_price) AS total_sales
    FROM dim_store_details
    INNER JOIN 
        orders_table ON orders_table.store_code = dim_store_details.store_code
    INNER JOIN 
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY "store_type"
    ),
sales_table 
AS ( 
    SELECT store_sales."store_type" AS store_type, total_sales AS total_sales
    FROM store_sales
    )
SELECT store_type, ROUND(total_sales::numeric,2), 
        ROUND((100 * total_sales / SUM(total_sales) OVER ())::numeric,2) AS percent
FROM sales_table
ORDER BY percent DESC;


-- Task 6 -- DONE (something not quite right with the data possibly -- fixed)
-- Which month in which year produced the highest sales
-- +-------------+------+-------+  
-- | total_sales | year | month |   total_sales | year | month
-- +-------------+------+-------+  -------------+------+------
-- |    27936.77 | 1994 |     3 |      27936.77 | 1994 | 3
-- |    27356.14 | 2019 |     1 |      27356.14 | 2019 | 1
-- |    27091.67 | 2009 |     8 |      27091.67 | 2009 | 8
-- |    26679.98 | 1997 |    11 |      26679.98 | 1997 | 11
-- |    26310.97 | 2018 |    12 |      26310.97 | 2018 | 12
-- |    26277.72 | 2019 |     8 |      26277.72 | 2019 | 8
-- |    26236.67 | 2017 |     9 |      26236.67 | 2017 | 9
-- |    25798.12 | 2010 |     5 |      25798.12 | 2010 | 5
-- |    25648.29 | 1996 |     8 |      25648.29 | 1996 | 8
-- |    25614.54 | 2000 |     1 |      25614.54 | 2000 | 1
-- +-------------+------+-------+  

SELECT  ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS "total_sales",
        dim_date_times.year,
        dim_date_times.month
FROM orders_table
INNER JOIN 
    dim_products ON dim_products.product_code = orders_table.product_code 
INNER JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY
      dim_date_times.year, dim_date_times.month
ORDER BY "total_sales" DESC
FETCH FIRST 10 ROWS ONLY; 

-- Task 7 -- DONE but data not right?
-- What is our staff headcount
-- +---------------------+--------------+
-- | total_staff_numbers | country_code |  total_staff_numbers | country_code
-- +---------------------+--------------+ ---------------------+--------------
-- |               13307 | GB           |                13307 | GB
-- |                6123 | DE           |                 6123 | DE
-- |                1384 | US           |                 1384 | US
-- +---------------------+--------------+

SELECT sum(staff_numbers) AS "total_staff_numbers", "country_code"
FROM dim_store_details
GROUP BY "country_code"
ORDER BY "total_staff_numbers" DESC;

-- Task 8 -- DONE but data not right (numbers don't match) (Now matching)
-- Which German store type is selling the most
-- +--------------+-------------+--------------+
-- | total_sales  | store_type  | country_code |  total_sales | store_type  | country_code
-- +--------------+-------------+--------------+--------------+-------------+--------------
-- |   198373.57  | Outlet      | DE           |    198373.57 | Outlet      | DE
-- |   247634.20  | Mall Kiosk  | DE           |    247634.20 | Mall Kiosk  | DE
-- |   384625.03  | Super Store | DE           |    384625.03 | Super Store | DE
-- |  1109909.59  | Local       | DE           |   1109909.59 | Local       | DE
-- +--------------+-------------+--------------+

SELECT  ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS "total_sales",
        dim_store_details.store_type,
        dim_store_details.country_code
FROM orders_table
INNER JOIN 
    dim_products ON dim_products.product_code = orders_table.product_code 
INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY
    dim_store_details.store_type,
    dim_store_details.country_code
ORDER BY "total_sales" ;    

-- Task 9
-- DONE
-- Sales would like the get an accurate metric for how quickly the company is making sales.
-- Determine the average time taken between each sale grouped by year, 
-- the query should return the following information:
--  +------+-------------------------------------------------------+
--  | year |                           actual_time_taken           |  year | actual_time_taken 
--  +------+-------------------------------------------------------+ ------+-------------------
--  | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |  2013 | 02:17:12.300182   
--  | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |  1993 | 02:15:35.857327   
--  | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... |  2002 | 02:13:50.412529    
--  | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |  2022 | 02:13:06.313993   
--  | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |  2008 | 02:13:02.80308    
--  +------+-------------------------------------------------------+

WITH time_diff AS(
SELECT 
     (MAKE_DATE("year"::int, "month"::int,"day"::INT) + "timestamp"::time) 
        -LAG(MAKE_DATE("year"::int, "month"::int,"day"::INT) + "timestamp"::time)
        OVER (ORDER BY (MAKE_DATE("year"::int, "month"::int,"day"::INT) + "timestamp"::time)) AS time_taken, 
    year
FROM
    dim_date_times
)
SELECT year, AVG(time_taken) AS actual_time_taken
FROM time_diff
GROUP BY "year"
ORDER BY actual_time_taken DESC
FETCH FIRST 5 ROWS ONLY;


