# Task 5: Gold Layer, Auto Loader, Streaming and Schema Evolution

## Objective

In this task, I built the Gold layer and implemented incremental file ingestion using Databricks Auto Loader.

The main focus was Fact and Dimension tables, business aggregations, Auto Loader, Structured Streaming, checkpointing, incremental file processing, and schema evolution.

---

## Architecture

```text
Silver Layer
     ↓
Gold Layer
     ↓
Fact and Dimension Tables
     ↓
Business Reporting
```

Streaming flow:

```text
New Files in ADLS
       ↓
Auto Loader
       ↓
Structured Streaming
       ↓
Checkpoint
       ↓
Bronze Streaming Table
```

---

## 1. Customer Dimension

A customer dimension was created from trusted Silver customer data.

Important attributes included:

```text
customer_id
customer_name
email
city
province
country
is_active
```

The table was saved as:

```text
retail_lakehouse.gold.dim_customers
```

A Dimension table contains descriptive information about a business entity.

---

## 2. Product Dimension

A product dimension was created containing information such as:

```text
product_id
product_name
category
brand
price
active_flag
```

The table was saved as:

```text
retail_lakehouse.gold.dim_products
```

---

## 3. Fact Sales

Orders, Order Items, and Products were joined to create a Fact Sales table.

The Fact table contained transactional information including:

```text
order_id
customer_id
product_id
order_date
quantity
unit_price
discount_pct
revenue
```

Revenue was calculated using:

```text
Quantity × Unit Price × (1 − Discount)
```

The table was saved as:

```text
retail_lakehouse.gold.fact_sales
```

---

## 4. Daily Sales

Daily business metrics were created using aggregation.

Metrics included:

```text
Total Revenue
Total Orders
Units Sold
```

The result was stored in:

```text
retail_lakehouse.gold.daily_sales
```

---

## 5. Monthly Sales

Sales data was grouped by month.

Metrics included:

```text
Monthly Revenue
Monthly Orders
```

The result was stored in:

```text
retail_lakehouse.gold.monthly_sales
```

---

## 6. Product Performance

Product level metrics were created to understand business performance.

Metrics included:

```text
Total Revenue
Units Sold
Total Orders
```

The result was stored in:

```text
retail_lakehouse.gold.product_performance
```

---

## 7. Customer Performance

Customer level metrics were created.

Metrics included:

```text
Total Spend
Total Orders
```

The result was stored in:

```text
retail_lakehouse.gold.customer_performance
```

---

## 8. Gold Layer Strategy

```text
Bronze
Raw Data

Silver
Clean and Trusted Data

Gold
Business Ready Data
```

The Gold layer provides optimized datasets for reporting, analytics, dashboards, and business users.

---

## 9. Auto Loader

Databricks Auto Loader was used to automatically process newly arriving files from ADLS Gen2.

Example:

```python
orders_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option(
        "cloudFiles.format",
        "csv"
    )
    .option(
        "cloudFiles.schemaLocation",
        schema_path
    )
    .option(
        "cloudFiles.inferColumnTypes",
        "true"
    )
    .option(
        "header",
        "true"
    )
    .load(streaming_path)
)
```

`cloudFiles` enables Databricks Auto Loader.

Auto Loader detects and processes newly arriving files without repeatedly processing the entire directory.

---

## 10. Streaming Metadata

Metadata was added to streaming records.

```text
source_file
ingestion_timestamp
```

This provides data lineage and helps identify where each record came from.

---

## 11. Streaming Write

Streaming data was written using:

```python
.writeStream
```

The target table was:

```text
retail_lakehouse.bronze.orders_stream_raw
```

The trigger used was:

```python
.trigger(availableNow=True)
```

This processes all currently available files and then stops.

---

## 12. Checkpointing

A checkpoint location was configured for the streaming pipeline.

Checkpointing remembers processing progress.

Example:

```text
First Run
File 1 processed

Second Run
File 1 skipped
File 2 processed
```

This prevents previously processed files from being processed again after a restart.

Checkpointing improves:

```text
Reliability
Fault tolerance
Restart capability
Duplicate processing prevention
```

---

## 13. Incremental Streaming Test

Streaming files were uploaded one by one.

After the first file:

```text
40 rows
```

After the second file:

```text
80 rows
```

After all five files:

```text
200 rows
```

This demonstrated that Auto Loader processed only newly arriving files.

---

## 14. Schema Evolution

A new customer file arrived with additional columns:

```text
loyalty_tier
phone_number
```

Schema Evolution was used so the Delta table could accept the new structure.

Example:

```python
.option(
    "mergeSchema",
    "true"
)
```

The new columns were added without rebuilding the entire table.

---

## Key Concepts Practiced

```text
Gold Layer
Fact Table
Dimension Table
Star Schema
Business Metrics
Aggregations
Revenue Calculation
Auto Loader
cloudFiles
Structured Streaming
readStream
writeStream
Checkpointing
availableNow
Incremental File Processing
Schema Evolution
mergeSchema
Data Lineage
```

---

## Practical Evidence

```text
01_task5_notebook.png
02_gold_tables.png
03_top_products.png
04_first_streaming_file.png
05_autoloader_incremental.png
06_schema_evolution.png
07_task5_final_validation.png
```

---

## Final Result

Successfully built the Gold reporting layer and implemented incremental streaming ingestion.

The final solution supported:

```text
Trusted Silver Data
        ↓
Gold Business Tables
        ↓
Fact and Dimension Model
        ↓
Business Metrics

New ADLS Files
        ↓
Auto Loader
        ↓
Checkpointing
        ↓
Incremental Processing
        ↓
Schema Evolution
```

This task demonstrated how Azure Databricks can support both analytics ready Gold datasets and reliable incremental data ingestion.
