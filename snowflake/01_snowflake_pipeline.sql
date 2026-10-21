USE ROLE ACCOUNTADMIN;

USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS RETAIL_SALES_DB;
CREATE SCHEMA IF NOT EXISTS RETAIL_SALES_DB.RETAIL_SALES_STAGING;

USE ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON SCHEMA RETAIL_SALES_DB.RETAIL_SALES_STAGING TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

CREATE OR REPLACE STAGE adls_stage
URL = 'azure://storageaccountname***.blob.core.windows.net/containername***/processed/'
CREDENTIALS = (AZURE_SAS_TOKEN = '********');

LIST @adls_stage;


-- ---------------------------------------------------------
-- CUSTOMER
-- ---------------------------------------------------------
DROP TABLE IF EXISTS CUSTOMER;
 
CREATE TABLE IF NOT EXISTS CUSTOMER (
    customer_accountNumber VARCHAR, customerId INT, modifiedDate DATETIME,
    details_accountNumber VARCHAR, personId INT, territoryId INT,
    ingest_timestamp DATETIME, source_file VARCHAR
);

COPY INTO CUSTOMER
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*customer/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- PERSON
-- ---------------------------------------------------------
DROP TABLE IF EXISTS PERSON;
 
CREATE TABLE IF NOT EXISTS PERSON (
    BusinessEntityID INT, PersonType VARCHAR, NameStyle INT, Title VARCHAR,
    FirstName VARCHAR, MiddleName VARCHAR, LastName VARCHAR, Suffix VARCHAR,
    EmailPromotion INT, ModifiedDate DATETIME,
    ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO PERSON
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*person/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- ADDRESS
-- ---------------------------------------------------------
DROP TABLE IF EXISTS ADDRESS;
 
CREATE TABLE IF NOT EXISTS ADDRESS (
    AddressID INT, AddressLine1 VARCHAR, AddressLine2 VARCHAR, City VARCHAR,
    StateProvinceID INT, PostalCode VARCHAR, ModifiedDate DATETIME,
    ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO ADDRESS
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*address/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- STATE_PROVINCE
-- ---------------------------------------------------------
DROP TABLE IF EXISTS STATE_PROVINCE;
 
CREATE TABLE IF NOT EXISTS STATE_PROVINCE (
    StateProvinceID INT, StateProvinceCode VARCHAR, CountryRegionCode VARCHAR,
    IsOnlyStateProvinceFlag INT, Name VARCHAR, TerritoryID INT,
    ModifiedDate DATETIME, ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO STATE_PROVINCE
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*stateprovince/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- COUNTRY_REGION
-- ---------------------------------------------------------
DROP TABLE IF EXISTS COUNTRY_REGION;
 
CREATE TABLE IF NOT EXISTS COUNTRY_REGION (
    CountryRegionCode VARCHAR, Name VARCHAR, ModifiedDate DATETIME,
    ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO COUNTRY_REGION
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*countryregion/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- PRODUCT
-- ---------------------------------------------------------
DROP TABLE IF EXISTS PRODUCT;
 
CREATE TABLE IF NOT EXISTS PRODUCT (
    ProductID INT, Name VARCHAR, ProductNumber VARCHAR, Color VARCHAR,
    SafetyStockLevel INT, ReorderPoint INT, StandardCost DECIMAL(20,2), ListPrice DECIMAL(20,2),
    Size VARCHAR, SizeUnitMeasureCode VARCHAR, WeightUnitMeasureCode VARCHAR,
    Weight DECIMAL(20,2), ProductLine VARCHAR, Class VARCHAR, Style VARCHAR,
    ProductSubCategoryID INT, ProductModelID INT,
    SellStartDate DATETIME, SellEndDate DATETIME, DiscontinuedDate DATETIME,
    ModifiedDate DATETIME, ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO PRODUCT
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*product/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- PRODUCT_CATEGORY
-- ---------------------------------------------------------
DROP TABLE IF EXISTS PRODUCT_CATEGORY;
 
CREATE TABLE IF NOT EXISTS PRODUCT_CATEGORY (
    ProductCategoryID INT, Name VARCHAR, ModifiedDate DATETIME,
    ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO PRODUCT_CATEGORY
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*productcategory/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- PRODUCT_SUBCATEGORY
-- ---------------------------------------------------------
DROP TABLE IF EXISTS PRODUCT_SUBCATEGORY;
 
CREATE TABLE IF NOT EXISTS PRODUCT_SUBCATEGORY (
    ProductSubCategoryID INT, ProductCategoryID INT, Name VARCHAR,
    ModifiedDate DATETIME, ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO PRODUCT_SUBCATEGORY
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*productsubcategory/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- SALES_ORDER_HEADER
-- ---------------------------------------------------------
DROP TABLE IF EXISTS SALES_ORDER_HEADER;
 
CREATE TABLE IF NOT EXISTS SALES_ORDER_HEADER (
    SalesOrderID INT, RevisionNumber INT, OrderDate DATETIME,
    DueDate DATETIME, ShipDate DATETIME, Status VARCHAR, OnlineOrderFlag INT,
    SalesOrderNumber VARCHAR, PurchaseOrderNumber VARCHAR, AccountNumber VARCHAR,
    CustomerID INT, SalesPersonID INT, TerritoryID INT, BillToAddressID INT,
    ShipToAddressID INT, ShipMethodID INT, CreditCardID INT,
    CreditCardApprovalCode VARCHAR, CurrencyRateID INT,
    SubTotal DECIMAL(20,4), TaxAmt DECIMAL(20,4), Freight DECIMAL(20,4), TotalDue DECIMAL(20,4),
    ModifiedDate DATETIME, ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO SALES_ORDER_HEADER
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*salesorderheader/.+\.snappy\.parquet$';
 
-- ---------------------------------------------------------
-- SALES_ORDER_DETAIL
-- ---------------------------------------------------------
DROP TABLE IF EXISTS SALES_ORDER_DETAIL;
 
CREATE TABLE IF NOT EXISTS SALES_ORDER_DETAIL (
    salesOrderId INT, specialOfferId INT, carrierTrackingNumber VARCHAR,
    modifiedDate DATETIME, salesOrderDetailId INT, orderQty INT,
    lineTotal DECIMAL(20,6), productId INT, unitPrice DECIMAL(20,4), unitPriceDiscount DECIMAL(20,4),
    ingest_timestamp DATETIME, source_file VARCHAR
);
 
COPY INTO SALES_ORDER_DETAIL
FROM @adls_stage
FILE_FORMAT = (TYPE = 'Parquet' COMPRESSION = 'SNAPPY')
MATCH_BY_COLUMN_NAME = case_insensitive
PATTERN = '.*salesorderdetail/.+\.snappy\.parquet$';


SELECT 'CUSTOMER' AS tbl, COUNT(*) AS n FROM CUSTOMER
UNION ALL SELECT 'PERSON', COUNT(*) FROM PERSON
UNION ALL SELECT 'ADDRESS', COUNT(*) FROM ADDRESS
UNION ALL SELECT 'STATE_PROVINCE', COUNT(*) FROM STATE_PROVINCE
UNION ALL SELECT 'COUNTRY_REGION', COUNT(*) FROM COUNTRY_REGION
UNION ALL SELECT 'PRODUCT', COUNT(*) FROM PRODUCT
UNION ALL SELECT 'PRODUCT_CATEGORY', COUNT(*) FROM PRODUCT_CATEGORY
UNION ALL SELECT 'PRODUCT_SUBCATEGORY', COUNT(*) FROM PRODUCT_SUBCATEGORY
UNION ALL SELECT 'SALES_ORDER_HEADER', COUNT(*) FROM SALES_ORDER_HEADER
UNION ALL SELECT 'SALES_ORDER_DETAIL', COUNT(*) FROM SALES_ORDER_DETAIL;


-- =========================================================================
-- Step 6: Fakt- und Dimensionstabellen im Schema RETAIL_SALES_CORE
-- =========================================================================
 
-- Schema anlegen (mit ACCOUNTADMIN, da SYSADMIN evtl. kein CREATE SCHEMA hat)
USE ROLE ACCOUNTADMIN;
CREATE SCHEMA IF NOT EXISTS RETAIL_SALES_DB.RETAIL_SALES_CORE;
GRANT ALL PRIVILEGES ON SCHEMA RETAIL_SALES_DB.RETAIL_SALES_CORE TO ROLE SYSADMIN;
 
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE RETAIL_SALES_DB;
USE SCHEMA RETAIL_SALES_CORE;
 

-- =========================================================================
-- fact_sales_per_order_line
-- SalesOrderHeader + SalesOrderDetail (eine Zeile pro Bestellposition)
-- =========================================================================

CREATE OR REPLACE TABLE fact_sales_per_order_line AS
SELECT
    d.salesOrderDetailId    AS sales_order_detail_id,
    d.salesOrderId          AS sales_order_id,
    h.CustomerID            AS customer_id,
    h.SalesPersonID         AS sales_person_id,
    h.TerritoryID           AS territory_id,
    d.productId             AS product_id,
    d.orderQty              AS order_qty,
    d.unitPrice             AS unit_price_usd,
    d.unitPriceDiscount     AS unit_price_discount_usd,
    d.lineTotal             AS line_total_usd,
    h.SubTotal              AS sub_total_usd,
    h.TaxAmt                AS tax_amount_usd,
    h.Freight               AS freight_charge_usd,
    h.TotalDue              AS total_due_usd,
    h.Status                AS order_status,
    h.OrderDate::DATE       AS order_date,
    h.DueDate::DATE         AS due_date,
    h.ShipDate::DATE        AS ship_date,
    h.ModifiedDate          AS modified_at
FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.SALES_ORDER_DETAIL d
INNER JOIN RETAIL_SALES_DB.RETAIL_SALES_STAGING.SALES_ORDER_HEADER h
    ON d.salesOrderId = h.SalesOrderID;
-- =========================================================================
-- dim_product
-- Product + ProductSubCategory + ProductCategory
-- =========================================================================

CREATE OR REPLACE TABLE dim_product AS
SELECT
    p.ProductID                 AS product_id,
    p.Name                      AS name,
    p.ProductNumber             AS product_number,
    p.ProductModelID            AS product_model_id,
    s.ProductSubCategoryID      AS product_subcategory_id,
    s.Name                      AS product_subcategory,
    c.ProductCategoryID         AS product_category_id,
    c.Name                      AS product_category,
    p.Color                     AS color,
    p.SafetyStockLevel          AS safety_stock_level,
    p.ReorderPoint              AS reorder_point,
    p.StandardCost              AS standard_cost_usd,
    p.ListPrice                 AS list_price_usd,
    p.Size                      AS size,
    p.Weight                    AS weight,
    p.ProductLine               AS product_line,
    p.Class                     AS class,
    p.Style                     AS style,
    p.SellStartDate::DATE       AS sell_start_date,
    p.SellEndDate::DATE         AS sell_end_date,
    p.DiscontinuedDate::DATE    AS discontinued_date,
    p.ModifiedDate              AS modified_at
FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.PRODUCT p
LEFT JOIN RETAIL_SALES_DB.RETAIL_SALES_STAGING.PRODUCT_SUBCATEGORY s
    ON p.ProductSubCategoryID = s.ProductSubCategoryID
LEFT JOIN RETAIL_SALES_DB.RETAIL_SALES_STAGING.PRODUCT_CATEGORY c
    ON s.ProductCategoryID = c.ProductCategoryID;
 
 
-- =========================================================================
-- dim_customer
-- Customer + Person (LEFT JOIN: nicht jeder Kunde hat eine Person)
-- =========================================================================
CREATE OR REPLACE TABLE dim_customer AS
SELECT
    c.customerId                AS customer_id,
    c.customer_accountNumber    AS account_number,
    c.personId                  AS person_id,
    p.Title                     AS title,
    TO_VARCHAR(p.NameStyle)     AS name_style,
    p.FirstName                 AS first_name,
    p.MiddleName                AS middle_name,
    p.LastName                  AS last_name,
    p.Suffix                    AS suffix,
    CAST(NULL AS INT)           AS store_id,
    c.territoryId               AS territory_id,
    c.modifiedDate              AS modified_at
FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.CUSTOMER c
LEFT JOIN RETAIL_SALES_DB.RETAIL_SALES_STAGING.PERSON p
    ON c.personId = p.BusinessEntityID;
 
 
-- =========================================================================
-- dim_location
-- Address + StateProvince + CountryRegion
-- =========================================================================
CREATE OR REPLACE TABLE dim_location AS
SELECT
    a.AddressID                 AS address_id,
    a.AddressLine1              AS address_line_1,
    a.AddressLine2              AS address_line_2,
    a.City                      AS city,
    sp.StateProvinceCode        AS state_province_code,
    sp.Name                     AS state,
    sp.TerritoryID              AS territory_id,
    a.PostalCode                AS postal_code,
    cr.CountryRegionCode        AS country_region_code,
    cr.Name                     AS country,
    a.ModifiedDate              AS modified_at
FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.ADDRESS a
LEFT JOIN RETAIL_SALES_DB.RETAIL_SALES_STAGING.STATE_PROVINCE sp
    ON a.StateProvinceID = sp.StateProvinceID
LEFT JOIN RETAIL_SALES_DB.RETAIL_SALES_STAGING.COUNTRY_REGION cr
    ON sp.CountryRegionCode = cr.CountryRegionCode;

--===========================================================
 
-- Zeilenanzahl: fact = Anzahl SalesOrderDetail, dim_product = Anzahl Product, usw.
SELECT 'fact_sales_per_order_line' AS tbl, COUNT(*) AS n FROM fact_sales_per_order_line
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_location', COUNT(*) FROM dim_location;
 
-- Zum Vergleich: Quelltabellen
SELECT 'SALES_ORDER_DETAIL' AS tbl, COUNT(*) AS n FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.SALES_ORDER_DETAIL
UNION ALL SELECT 'PRODUCT', COUNT(*) FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.PRODUCT
UNION ALL SELECT 'CUSTOMER', COUNT(*) FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.CUSTOMER
UNION ALL SELECT 'ADDRESS', COUNT(*) FROM RETAIL_SALES_DB.RETAIL_SALES_STAGING.ADDRESS;



-- Fakt-Zeilen ohne passendes Produkt / Kunde (sollte 0 sein)
SELECT COUNT(*) AS ohne_produkt
FROM fact_sales_per_order_line f
LEFT JOIN dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;
 
SELECT COUNT(*) AS ohne_kunde
FROM fact_sales_per_order_line f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
 
SELECT * FROM fact_sales_per_order_line LIMIT 10;

SHOW TABLES IN SCHEMA RETAIL_SALES_DB.RETAIL_SALES_CORE;


-- =========================================================================
--Step 7 
-- =========================================================================
USE ROLE ACCOUNTADMIN;
CREATE SCHEMA IF NOT EXISTS RETAIL_SALES_DB.RETAIL_SALES_BI;
GRANT ALL PRIVILEGES ON SCHEMA RETAIL_SALES_DB.RETAIL_SALES_BI TO ROLE SYSADMIN;
 
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE RETAIL_SALES_DB;
USE SCHEMA RETAIL_SALES_BI;


-- =========================================================================
-- rep_monthly_sales: eine Zeile pro Monat
-- =========================================================================

CREATE OR REPLACE TABLE rep_monthly_sales AS
WITH order_level AS (
    SELECT DISTINCT
        sales_order_id,
        order_date,
        tax_amount_usd,
        freight_charge_usd
    FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line
    WHERE order_status = 'Shipped'
),
order_agg AS (
    SELECT
        DATE_TRUNC('month', order_date)::DATE       AS month_start_date,
        COUNT(*)                                    AS total_orders,
        ROUND(SUM(tax_amount_usd), 2)               AS total_tax_usd,
        ROUND(SUM(freight_charge_usd), 2)           AS total_freight_usd
    FROM order_level
    GROUP BY DATE_TRUNC('month', order_date)
),
line_agg AS (
    SELECT
        DATE_TRUNC('month', order_date)::DATE       AS month_start_date,
        SUM(order_qty)                              AS total_quantity_sold,
        ROUND(SUM(line_total_usd), 2)               AS total_sales_usd
    FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line
    WHERE order_status = 'Shipped'
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
    o.month_start_date                              AS month_start_date,
    TO_VARCHAR(YEAR(o.month_start_date))            AS sales_year,
    TO_VARCHAR(MONTH(o.month_start_date))           AS sales_month,
    o.total_orders                                  AS total_orders,
    l.total_sales_usd                               AS total_sales_usd,
    l.total_quantity_sold                           AS total_quantity_sold,
    o.total_tax_usd                                 AS total_tax_usd,
    o.total_freight_usd                             AS total_freight_usd
FROM order_agg o
INNER JOIN line_agg l
    ON o.month_start_date = l.month_start_date
ORDER BY o.month_start_date;



-- =========================================================================
-- Kontrolle
-- =========================================================================
SELECT * FROM rep_monthly_sales ORDER BY month_start_date;
 
-- Gesamtumsatz muss mit der Faktentabelle übereinstimmen
SELECT
    (SELECT ROUND(SUM(total_sales_usd), 2) FROM rep_monthly_sales) AS summe_report,
    (SELECT ROUND(SUM(line_total_usd), 2)
     FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line
     WHERE order_status = 'Shipped') AS summe_fact;
-- =========================================================================
-- rep_regional_sales: eine Zeile pro Land und Bundesstaat/Provinz
-- =========================================================================

/* 
Version 1 (auskommentiert):
Die territory_id wird aufgrund unterschiedlicher Bundesstaaten mehrfach angezeigt, wodurch die Gesamtumsätze mehrfach gezählt werden können
CREATE OR REPLACE TABLE rep_regional_sales AS
WITH loc AS (
    SELECT DISTINCT
        territory_id,
        country AS countryregionname,
        state_province_code AS stateprovincecode,
        state AS stateprovincename
    FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.dim_location
    WHERE territory_id IS NOT NULL
)
SELECT
    loc.territory_id AS territoryid,
    loc.countryregionname,
    loc.stateprovincecode,
    loc.stateprovincename,
    COUNT(DISTINCT fct.sales_order_id) AS no_of_orders,
    SUM(fct.line_total_usd)            AS total_sum,
    SUM(fct.order_qty)                 AS total_qty
FROM loc, RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line fct
WHERE loc.territory_id = fct.territory_id
    AND fct.order_status = 'Shipped'
GROUP BY 1,2,3,4;*/



CREATE OR REPLACE TABLE rep_regional_sales AS
WITH loc AS (
    SELECT territory_id,
           MIN(country) AS countryregionname,
           LISTAGG(DISTINCT state_province_code, ', ') AS stateprovincecode,
           LISTAGG(DISTINCT state, ', ')               AS stateprovincename
    FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.dim_location
    WHERE territory_id IS NOT NULL
    GROUP BY territory_id
)
SELECT loc.territory_id AS territoryid,
       loc.countryregionname, loc.stateprovincecode, loc.stateprovincename,
       COUNT(DISTINCT fct.sales_order_id) AS no_of_orders,
       SUM(fct.line_total_usd)            AS total_sum,
       SUM(fct.order_qty)                 AS total_qty
FROM loc
JOIN RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line fct
  ON loc.territory_id = fct.territory_id
WHERE fct.order_status = 'Shipped'
GROUP BY 1,2,3,4;

-- Kontrolle:
SELECT 
  (SELECT SUM(total_sum) FROM rep_regional_sales) AS summe_regional,
  (SELECT SUM(line_total_usd) 
   FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line 
   WHERE order_status = 'Shipped') AS summe_fact;

-- =========================================================================
-- 1) rep_top_products: eine Zeile pro Produkt
-- =========================================================================
CREATE OR REPLACE TABLE rep_top_products AS
SELECT pro.product_id,
       pro.name                 AS productname,
       pro.product_subcategory  AS productsubcategoryname,
       pro.product_category     AS productcategoryname,
       SUM(fct.line_total_usd)            AS total_sum,
       COUNT(DISTINCT fct.sales_order_id) AS no_of_orders,
       SUM(fct.order_qty)                 AS total_qty
FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.dim_product pro,
     RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line fct
WHERE fct.product_id = pro.product_id
  AND fct.order_status = 'Shipped'
GROUP BY 1,2,3,4;
 
-- Kontrolle
SELECT
    (SELECT ROUND(SUM(total_sum), 2) FROM rep_top_products) AS summe_report,
    (SELECT ROUND(SUM(line_total_usd), 2)
     FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line
     WHERE order_status = 'Shipped') AS summe_fact;
-- =========================================================================
-- 1) Top-10-Kundschaft nach Umsatz
-- =========================================================================

 CREATE OR REPLACE TABLE rep_top_customers AS
SELECT cus.customer_id, cus.last_name AS lastname, cus.first_name AS firstname,
       SUM(fct.line_total_usd) AS total_sum
FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.dim_customer cus,
     RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line fct
WHERE cus.customer_id = fct.customer_id
  AND fct.order_status = 'Shipped'
GROUP BY 1,2,3;

SELECT
    (SELECT ROUND(SUM(total_sum), 2) FROM rep_top_customers) AS summe_report,
    (SELECT ROUND(SUM(line_total_usd), 2)
     FROM RETAIL_SALES_DB.RETAIL_SALES_CORE.fact_sales_per_order_line
     WHERE order_status = 'Shipped') AS summe_fact;

-- Top 10 Kunde
SELECT * FROM rep_top_customers ORDER BY total_sum DESC LIMIT 10;
 


 
SELECT productname, total_qty FROM rep_top_products ORDER BY total_qty DESC LIMIT 3;
SELECT productname, total_sum FROM rep_top_products ORDER BY total_sum DESC LIMIT 5;
