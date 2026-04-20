# Northwind Sales Insights with dbt

## Business Problem
Northwind’s raw data not directly usable for analytics due to inconsistent column names, repeated joins, and unclear revenue calculations. After processing we have a clean and standardized data pipeline for sales analysis.
So step-by-step:
- we had unstandardised 
- too many joins
- unconsistent and wrong revenue calculations

## Models Built
- staging_orders: cleaned and standardized order data  
- staging_order_details: cleaned transaction-level sales data  
- staging_products: cleaned product information  
- staging_categories: cleaned category information  
- prep_sales: joined all staging models and calculated revenue and time features  
- mart_sales_performance: aggregated sales KPIs by year, month, and category  

## Insights
The mart provides clear monthly sales performance by category, including:
- total revenue  
- number of orders  
- average revenue per order  

This enables fast and consistent reporting without complex joins.

## Biggest Learning
The key learning was understanding how dbt structures data transformation into layers (staging → prep → mart), making pipelines modular, reusable, and much easier to maintain and scale.