# Task 2: Spark Basics, Reading Data & Bronze Layer

## Objective

In this task, I built the Bronze ingestion layer in Azure Databricks.

The goal was to read raw CSV and JSON files from Azure Data Lake Storage Gen2 using PySpark, add metadata for traceability, store the data as Delta tables, and validate the ingestion.

---

## Architecture

```text
ADLS Gen2
   ↓
Azure Databricks
   ↓
PySpark DataFrames
   ↓
Bronze Delta Tables
```

---

## Source Files

The following source files were loaded from ADLS Gen2:

- `customers.csv`
- `products.csv`
- `orders.csv`
- `order_items.csv`
- `payments.json`

Storage container:

```text
retail-source
```

---

## 1. Select Catalog and Bronze Schema

```python
spark.sql("USE CATALOG retail_lakehouse")
spark.sql("USE SCHEMA bronze")
```

This selected the Unity Catalog and Bronze schema used for raw data ingestion.

---

## 2. Read CSV Data from ADLS Gen2

Example:

```python
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path + "customers.csv")
)
```

The same approach was used for:

```text
customers
products
orders
order_items
```

---

## 3. Read JSON Data

```python
payments_df = spark.read.json(
    source_path + "payments.json"
)
```

---

## 4. Explore and Validate DataFrames

Used common PySpark operations such as:

```python
select()
filter()
show()
display()
count()
```

These operations helped inspect the source data before writing it into Bronze.

---

## 5. Add Metadata Columns

Metadata was added to improve lineage and traceability.

```python
from pyspark.sql.functions import current_timestamp, col

customers_df = customers_df.select(
    "*",
    col("_metadata.file_name").alias("source_file"),
    current_timestamp().alias("ingestion_timestamp")
)
```

### Metadata Purpose

`source_file`

Shows which source file produced the record.

`ingestion_timestamp`

Shows when the record entered the Bronze layer.

This makes it easier to trace bad records back to their original source.

---

## 6. Write Bronze Delta Tables

Example:

```python
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_lakehouse.bronze.customers_raw"
    )
```

The following Bronze Delta tables were created:

```text
customers_raw
products_raw
orders_raw
order_items_raw
payments_raw
```

---

## 7. Validate Bronze Tables

```sql
SHOW TABLES IN retail_lakehouse.bronze;
```

Example validation:

```sql
SELECT *
FROM retail_lakehouse.bronze.customers_raw
LIMIT 10;
```

Row counts were also checked to confirm that all source data was loaded successfully.

---

## Bronze Layer Strategy

The Bronze layer stores raw source data with minimal transformation.

```text
Source Data
   ↓
Bronze
Raw Data + Metadata
```

Cleaning and validation are not performed in Bronze.

This allows the raw data to remain available for:

- Auditing
- Troubleshooting
- Reprocessing
- Data lineage

---

## Key Concepts Practiced

- SparkSession
- PySpark DataFrames
- Reading CSV files
- Reading JSON files
- ADLS Gen2 integration
- `header`
- `inferSchema`
- `select()`
- `filter()`
- `show()`
- `display()`
- Bronze Layer
- Metadata Columns
- Delta Format
- `overwrite`
- `saveAsTable`
- Unity Catalog
- Row Count Validation
- Data Lineage

---

## Practical Evidence

Screenshots included in this task demonstrate:

- Customer DataFrame
- Source Row Counts
- Bronze Metadata
- Bronze Tables Created
- SQL Validation
- Unity Catalog Bronze Tables
- Customer Bronze Table
- Final Bronze Validation

---

## Final Result

Successfully built the Bronze ingestion layer for the Retail Lakehouse project.

```text
ADLS Gen2
   ↓
PySpark
   ↓
Raw DataFrames
   ↓
Metadata Added
   ↓
Delta Format
   ↓
Bronze Tables
```

All five source datasets were successfully read from Azure Data Lake Storage Gen2, converted into PySpark DataFrames, enriched with metadata, stored as Delta tables, and validated in Azure Databricks.
