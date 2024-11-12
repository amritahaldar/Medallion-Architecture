# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

tables_df = spark.sql("SHOW TABLES IN amrita_haldar_databricks_npmentorskool_onmicrosoft_com.silver_ecom")
display(tables_df)

for row in tables_df.collect():
    table_name = row['tableName']
    df_name = f"{table_name}_df"
    globals()[df_name] = spark.table(f"amrita_haldar_databricks_npmentorskool_onmicrosoft_com.silver_ecom.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.customer_dimension (
# MAGIC     customer_sk_key STRING,
# MAGIC     Customer_ID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Email STRING,
# MAGIC     PhoneNumber LONG,
# MAGIC     DateOfBirth DATE,
# MAGIC     RegistrationDate DATE
# MAGIC ) 
# MAGIC USING DELTA;

# COMMAND ----------

# Add a surrogate key (UUID) to the DataFrame
customers_df = customers_df.withColumn("customer_sk_key", expr("uuid()"))

# COMMAND ----------

# Append data from customers_silver_df to the gold_ecom.customers table
customers_df.write.option("mergeSchema", "true").mode("append").saveAsTable("amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.customer_dimension")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.order_dimension (
# MAGIC     order_sk_key STRING,
# MAGIC     OrderID STRING,
# MAGIC     CustomerID STRING,
# MAGIC     OrderDate DATE,
# MAGIC     ShippingTierID STRING,
# MAGIC     SupplierID STRING,
# MAGIC     OrderChannel STRING
# MAGIC ) 
# MAGIC USING DELTA;

# COMMAND ----------

orders_df = orders_df.withColumn("order_sk_key", expr("uuid()"))

# COMMAND ----------

orders_df.write.option("mergeSchema", "true").mode("append").saveAsTable("amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.order_dimension")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.payments_dimension (
# MAGIC     payment_sk_key STRING,
# MAGIC     PaymentID STRING,
# MAGIC     OrderID STRING,
# MAGIC     PaymentDate TIMESTAMP,
# MAGIC     PaymentMethodID STRING
# MAGIC ) 
# MAGIC USING DELTA;

# COMMAND ----------

payments_df = payments_df.withColumn("payment_sk_key", expr("uuid()"))

# COMMAND ----------

payments_df.printSchema()

# COMMAND ----------

payments_df.write.option("mergeSchema", "true").mode("append").saveAsTable("amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.payments_dimension")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.addresses_dimension (
# MAGIC     addresses_sk_key STRING,
# MAGIC     AddressID STRING,
# MAGIC     CustomerID STRING,
# MAGIC     AddressLine1 STRING,
# MAGIC     City STRING,
# MAGIC     State STRING,
# MAGIC     PinCode FLOAT,
# MAGIC     AddressType STRING
# MAGIC ) 
# MAGIC USING DELTA;

# COMMAND ----------

addresses_df = addresses_df.withColumn("addresses_sk_key", expr("uuid()"))

# COMMAND ----------

addresses_df.write.option("mergeSchema", "true").mode("append").saveAsTable("amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.addresses_dimension")

# COMMAND ----------

display(customers_df)

# COMMAND ----------

# Join the tables to create the fact table
fact_sales_df = orders_df.alias("o") \
    .join(order_items_df.alias("oi"), col("o.OrderID") == col("oi.OrderID")) \
    .join(products_df.alias("p"), col("oi.ProductID") == col("p.Product_ID")) \
    .join(customers_df.alias("c"), col("o.CustomerID") == col("c.CustomerID")) \
    .join(payments_df.alias("pay"), col("o.OrderID") == col("pay.OrderID")) \
    .join(addresses_df.alias("a"), col("o.CustomerID") == col("a.CustomerID")) \
    .select(
        # Generate a unique surrogate key for each row in the fact table
        monotonically_increasing_id().alias("fact_sales_sk_key"),
        col("pay.PaymentID").alias("Payment_ID"),
        col("c.CustomerID").alias("Customer_ID"),
        col("p.Product_ID").alias("Product_ID"),
        col("o.OrderID").alias("Order_ID"),
        col("a.AddressID").alias("Address_ID"),
        (col("oi.Quantity") * col("p.Discounted_Price")).alias("Sales_amount"),
        col("oi.Quantity").alias("Quantity_purchased"),
        col("p.Actual_Price").alias("Actual_price"),  
        col("p.Discounted_Price").alias("Discounted_price") 
    )

# COMMAND ----------

display(fact_sales_df)

# COMMAND ----------

# Write the fact_sales DataFrame to the gold layer
fact_sales_df.write.option("mergeSchema", "true").format("delta").mode("overwrite").saveAsTable("amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.fact_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.vw_total_sales_by_city AS
# MAGIC SELECT
# MAGIC     a.City AS city,
# MAGIC     SUM(f.Sales_amount) AS total_sales
# MAGIC FROM
# MAGIC     amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.fact_sales f
# MAGIC JOIN
# MAGIC     amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.addresses_dimension a ON f.Address_ID = a.AddressID
# MAGIC GROUP BY
# MAGIC     a.City;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.vw_total_sales_by_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.vw_sales_by_payment_method AS
# MAGIC SELECT
# MAGIC     pm.PaymentMethodID AS payment_method,
# MAGIC     COUNT(f.fact_sales_sk_key) AS number_of_transactions,
# MAGIC     SUM(f.Sales_amount) AS total_sales
# MAGIC FROM
# MAGIC     amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.fact_sales f
# MAGIC JOIN
# MAGIC     amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.payments_dimension pm ON f.Payment_ID = pm.PaymentID
# MAGIC GROUP BY
# MAGIC     pm.PaymentMethodID
# MAGIC ORDER BY
# MAGIC     total_sales DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amrita_haldar_databricks_npmentorskool_onmicrosoft_com.gold_ecom.vw_sales_by_payment_method;
