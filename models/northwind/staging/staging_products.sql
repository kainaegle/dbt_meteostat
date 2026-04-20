-- models/northwind/staging_products.sql
WITH source_data AS (
    SELECT *
    FROM {{ source('northwind_data', 'products') }}
)
SELECT
    productid AS product_id,
    productname AS product_name,
    supplierid AS supplier_id,
    categoryid AS category_id,
    unitprice::NUMERIC AS unit_price
FROM source_data