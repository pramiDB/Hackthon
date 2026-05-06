# Databricks notebook source
# Import PySpark functions for data transformation

from pyspark.sql.functions import col, coalesce, to_date, when
from pyspark.sql.functions import col, coalesce, expr
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, round
from pyspark.sql.functions import concat
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC **Setup & Config**

# COMMAND ----------

catalog = "usecase_catalog"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# Base paths
base_path = "/Volumes/usecase_catalog/default/retail_dev"
raw_path = f"{base_path}/raw"
checkpoint_path = f"{base_path}/checkpoints"
schema_path = f"{base_path}/schema"

# COMMAND ----------

# MAGIC %md
# MAGIC **Bronze Layer – Incremental Ingestion (Auto Loader)**

# COMMAND ----------

# Ingest Sales Order data into Bronze layer
def clean_columns(df):
    return df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

sales_order_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")   
    .option("cloudFiles.schemaLocation", f"{schema_path}/sales_order")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{raw_path}/sales_order")
)
cleaned_df = clean_columns(sales_order_bronze)


cleaned_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{checkpoint_path}/sales_order") \
    .trigger(availableNow=True) \
    .toTable(f"{catalog}.{bronze_schema}.sales_order")

# COMMAND ----------

#Ingest SalesOrderLine data into Bronze layer
sales_line_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{schema_path}/sales_order_line")
    .load(f"{raw_path}/sales_order_line")
)

(sales_line_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/sales_order_line")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{bronze_schema}.sales_order_line")
)

# COMMAND ----------

#Ingest Product data into Bronze layer

product_bronze = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("cloudFiles.schemaLocation", f"{schema_path}/product") \
    .load(f"{raw_path}/product")

import re

def clean_columns(df):
    return df.toDF(*[
        re.sub(r'[^a-zA-Z0-9]', '_', c.strip().lower())
        for c in df.columns
    ])

cleaned_df = clean_columns(product_bronze)

(cleaned_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/product")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{bronze_schema}.product")
)

# COMMAND ----------

#Ingest CardRefund data into Bronze layer

refund_bronze = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("cloudFiles.schemaLocation", f"{schema_path}/refund") \
    .load(f"{raw_path}/refund")

(refund_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/refund")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{bronze_schema}.card_refund")
)

# COMMAND ----------

#Ingest Customer data into Bronze layer

customer_bronze = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("cloudFiles.schemaLocation", f"{schema_path}/customer") \
    .load(f"{raw_path}/customer")

import re

def clean_columns(df):
    return df.toDF(*[
        re.sub(r'[^a-zA-Z0-9]', '_', c.strip().lower())
        for c in df.columns
    ])

cleaned_df = clean_columns(customer_bronze)

(cleaned_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/customer")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{bronze_schema}.customer")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Layer – Cleaning & Standardization**

# COMMAND ----------

# SalesOrder Cleaning

sales_order = spark.read.table(f"{catalog}.{bronze_schema}.sales_order")


sales_order_silver = sales_order \
    .dropDuplicates(["SalesOrderId"]) \
    .filter(col("SalesOrderId").isNotNull()) \
    .filter(col("customerId").isNotNull()) \
    .withColumn(
        "creationDate",
        coalesce(
            expr("try_to_date(creationDate, 'dd-MM-yyyy')"),
            expr("try_to_date(creationDate, 'MM/dd/yyyy')"),
           )
    )

sales_order_silver.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog}.{silver_schema}.sales_order")

# COMMAND ----------

# SalesOrderLine Cleaning

sales_line = spark.read.table(f"{catalog}.{bronze_schema}.sales_order_line")

sales_line_silver = sales_line \
    .dropDuplicates(["salesOrderLineId"]) \
    .filter(col("salesOrderId").isNotNull()) \
    .filter(col("ProductId").isNotNull()) \
    .filter(col("quantity") > 0)

sales_line_silver.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.sales_order_line")

# COMMAND ----------

# Product Cleaning

product = spark.read.table(f"{catalog}.{bronze_schema}.product")

product_silver = product \
    .dropDuplicates(["product_id"]) \
    .withColumnRenamed("product_id", "ProductId") \
    .withColumnRenamed("product_name", "ProductName")

product_silver.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.product")

# COMMAND ----------

# CardRefund Cleaning

refund = spark.read.table(f"{catalog}.{bronze_schema}.card_refund")

refund_silver = refund \
    .dropDuplicates(["CardRefundID"]) \
    .filter(col("ReturnOrderID").isNotNull()) \
    .withColumn(
        "RefundDate",
        coalesce(
            expr("try_to_date(RefundDate, 'dd-MM-yyyy')"),
            expr("try_to_date(RefundDate, 'MM/dd/yyyy')"),
            expr("try_to_date(RefundDate, 'yyyy-MM-dd')")
        )
    ) \
    .filter(col("RefundDate").isNotNull())

refund_silver.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.card_refund")

# COMMAND ----------

# customer Cleaning

customer = spark.read.table(f"{catalog}.{bronze_schema}.customer")

customer_silver = customer \
    .dropDuplicates(["customer_id"]) \
    .withColumnRenamed("customer_id", "customerId")

customer_silver.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.customer")

# COMMAND ----------

# MAGIC %md
# MAGIC **Gold Layer – KPI 4 (Orders vs Returns)**

# COMMAND ----------

sales_order = spark.read.table(f"{catalog}.{silver_schema}.sales_order")
sales_line = spark.read.table(f"{catalog}.{silver_schema}.sales_order_line")
refund = spark.read.table(f"{catalog}.{silver_schema}.card_refund")
product = spark.read.table(f"{catalog}.{silver_schema}.product")
customer = spark.read.table(f"{catalog}.{silver_schema}.customer")

# COMMAND ----------

# Build Fact Table

fact_df = sales_order.alias("so") \
    .join(sales_line.alias("sl"), "salesorderid") \
    .join(refund.alias("rf"),
          col("so.returnorderid") == col("rf.returnorderid"),
          "left") \
    .select(
        col("so.customerid"),
        col("sl.productid"),
        col("sl.quantity"),
        when(col("rf.returnorderid").isNotNull(), 1).otherwise(0).alias("is_return")
    )

# COMMAND ----------

#top records based on the requirement

top5 = customer_product_kpi.orderBy(col("total_orders").desc()).limit(5)

# COMMAND ----------

# Join the required dataframes

t = top5.alias("t")
p = product.alias("p")
c = customer.alias("c")

final_df = t \
    .join(
        p.select("ProductId", "ProductName"),
        col("t.productid") == col("p.ProductId"),
        "left"
    ) \
    .select(
        col("t.*"),
        col("p.ProductName")
    ) \
    .join(
        c.select("customerid", "first_name", "last_name"),
        col("t.customerid") == col("c.customerid"),
        "left"
    ) \
    .select(
        col("t.*"),                
        col("p.ProductName"),
        col("c.first_name"),
        col("c.last_name")
    )

# COMMAND ----------

# Required columns

final_df = final_df.select("customerid", concat(col("first_name"), lit(" "), col("last_name")).alias("customer_name"), "productid", "ProductName", "total_orders", "total_returns")

# COMMAND ----------

#Required KPI result

final_df.display()

# COMMAND ----------

# move the final df to gold

final_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.top5_products_returns")