# Data Transformation in Azure Synapse

1. Open the Azure Synapse Studio and go to the "SQL Scripts" section.
2. Write and execute SQL queries to transform the raw data:
   ```sql
   -- Example SQL query to calculate total sales and average order value
   SELECT 
     product_id, 
     SUM(quantity_sold) AS total_sales, 
     AVG(order_value) AS average_order_value
   FROM raw_sales_data
   GROUP BY product_id;
