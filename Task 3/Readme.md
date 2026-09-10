
# Task 3: Silver Layer, Data Quality and PySpark Transformations

## Objective

In this task, I transformed raw Bronze data into clean and trusted Silver tables using PySpark.

The main focus was data cleaning, data quality validation, duplicate handling, quarantine processing, relationship validation, joins, aggregations, and Window Functions.

---

## Architecture

```text
Bronze Raw Tables
        ↓
Data Cleaning
        ↓
Data Quality Validation
        ↓
Good Records → Silver
Bad Records  → Quarantine
        ↓
Relationship Validation
        ↓
Joined Silver Dataset
```

---

## Source Tables

The following Bronze tables were used:

```text
customers_raw
products_raw
orders_raw
order_items_raw
payments_raw
```

---

## 1. Read Bronze Tables

```python
customers_df = spark.table(
    "retail_lakehouse.bronze.customers_raw"
)

products_df = spark.table(
    "retail_lakehouse.bronze.products_raw"
)

orders_df = spark.table(
    "retail_lakehouse.bronze.orders_raw"
)

order_items_df = spark.table(
    "retail_lakehouse.bronze.order_items_raw"
)

payments_df = spark.table(
    "retail_lakehouse.bronze.payments_raw"
)
```

The Bronze Delta tables were loaded as PySpark DataFrames for Silver processing.

---

## 2. Customer Data Cleaning

Customer data was standardized using PySpark functions.

```python
customers_clean_df = (
    customers_df
    .withColumn(
        "customer_name",
        initcap(trim(col("customer_name")))
    )
    .withColumn(
        "email",
        lower(trim(col("email")))
    )
    .withColumn(
        "city",
        initcap(trim(col("city")))
    )
    .withColumn(
        "province",
        upper(trim(col("province")))
    )
    .withColumn(
        "country",
        initcap(trim(col("country")))
    )
    .withColumn(
        "is_active",
        upper(trim(col("is_active")))
    )
    .withColumn(
        "signup_date",
        to_date(col("signup_date"))
    )
    .dropDuplicates(["customer_id"])
)
```

Cleaning included:

```text
Removing unnecessary spaces
Standardizing text case
Converting dates
Removing duplicate customer IDs
Normalizing email addresses
```

---

## 3. Customer Data Quality Rules

Data quality rules were applied to identify invalid records.

```python
customers_checked_df = customers_clean_df.withColumn(
    "dq_reason",
    when(
        col("customer_id").isNull() |
        (trim(col("customer_id")) == ""),
        "missing_customer_id"
    )
    .when(
        (col("email").isNotNull()) &
        (trim(col("email")) != "") &
        (
            ~col("email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
        ),
        "invalid_email"
    )
)
```

Examples of validation rules:

```text
Missing customer ID
Invalid email format
Duplicate customer ID
Invalid data types
Invalid numeric values
Invalid dates
```

---

## 4. Good Records and Quarantine Records

Good records were separated from bad records.

```python
customers_silver_df = customers_checked_df.filter(
    col("dq_reason").isNull()
)

customers_quarantine_df = customers_checked_df.filter(
    col("dq_reason").isNotNull()
)
```

Good records were written to Silver.

Bad records were written to Quarantine for investigation and future reprocessing.

---

## 5. Quarantine Strategy

The quarantine layer stores records that fail data quality rules.

Instead of deleting bad records, they are kept separately so the issue can be investigated and corrected later.

Important information can include:

```text
dq_reason
source_file
ingestion_timestamp
```

This helps with troubleshooting and data lineage.

---

## 6. Product Cleaning and Validation

Product data was cleaned and numeric columns were converted to correct data types.

Example:

```python
.withColumn(
    "price",
    col("price").cast("decimal(10,2)")
)
```

Validation included:

```text
Missing product ID
Invalid price
Negative price
Invalid cost
Negative cost
Duplicate product ID
```

---

## 7. Order Cleaning and Validation

Order data was standardized and validated.

Examples:

```text
Order date converted to date
Currency converted to uppercase
City standardized
Order status standardized
Duplicate order IDs removed
```

Validation included:

```text
Missing order ID
Missing customer ID
Invalid order date
Invalid order status
```

---

## 8. Relationship Validation

Orders were checked against the Customer table.

### Left Anti Join

```python
orders_orphan_df = orders_clean_df.join(
    customers_silver_df.select("customer_id"),
    "customer_id",
    "left_anti"
)
```

`left_anti` identifies orders whose customer ID does not exist in the Customer table.

These records can be treated as invalid or moved to Quarantine.

### Left Semi Join

```python
orders_silver_df = orders_clean_df.join(
    customers_silver_df.select("customer_id"),
    "customer_id",
    "left_semi"
)
```

`left_semi` keeps only orders that have a valid matching customer.

---

## 9. Order Item Cleaning and Validation

Order item columns were converted to correct data types.

```python
line_number → integer
quantity → integer
unit_price → decimal
discount_pct → decimal
line_total → decimal
```

Duplicates were removed using:

```text
order_id + line_number
```

Validation included:

```text
Missing order ID
Missing product ID
Quantity greater than zero
Unit price not negative
Discount between valid limits
```

Relationship checks were also performed against Orders and Products.

---

## 10. Payment Cleaning and Validation

Payment data was cleaned and validated.

Examples:

```text
payment_amount converted to decimal
payment_date converted to date
payment_status standardized
duplicate payment IDs removed
```

Validation included:

```text
Missing payment ID
Missing order ID
Invalid payment amount
Invalid payment date
```

Payments were also checked against valid Orders.

---

## 11. Payment Aggregation

Payment amounts were summarized by order.

```python
payment_summary_df = (
    payments_silver_df
    .groupBy("order_id")
    .agg(
        sum("payment_amount").alias("paid_amount")
    )
)
```

This created the total amount paid for each order.

---

## 12. Join Silver Tables

The clean Silver datasets were joined together.

The relationship was:

```text
Orders
   ↓
Order Items
   ↓
Customers
   ↓
Products
   ↓
Payments
```

Inner joins were used when matching records were required.

A Left Join was used for Payments because some valid orders may not have a payment yet.

---

## 13. Calculate Revenue

Revenue was calculated using:

```text
Quantity × Unit Price × (1 − Discount)
```

Example:

```python
round(
    col("quantity")
    * col("unit_price")
    * (lit(1) - col("discount_pct")),
    2
)
```

The result created a business ready revenue value for each order item.

---

## 14. Window Function

A Window Function was used to rank orders for each customer.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

customer_order_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("order_date").desc())
)

orders_ranked_df = orders_silver_df.withColumn(
    "order_rank",
    row_number().over(customer_order_window)
)
```

This allows orders to be ranked within each customer while keeping the original rows.

Unlike `groupBy()`, Window Functions do not collapse the original records.

---

## 15. Silver Tables Created

The following Silver tables were created:

```text
customers
products
orders
order_items
payments
order_details
```

---

## Quarantine Tables

Invalid records were stored separately in Quarantine tables.

This allows bad data to be investigated without affecting trusted Silver data.

---

## Silver Layer Strategy

```text
Bronze
Raw Data
   ↓
Cleaning
   ↓
Validation
   ↓
Relationship Checks
   ↓
Good Data → Silver
Bad Data  → Quarantine
```

The Silver layer contains cleaned, standardized, validated, and trusted data that can be used for downstream transformations and reporting.

---

## Key Concepts Practiced

```text
PySpark DataFrames
withColumn
trim
lower
upper
initcap
to_date
cast
dropDuplicates
NULL validation
Data quality rules
when
rlike
Quarantine
Data lineage
Left Anti Join
Left Semi Join
Inner Join
Left Join
Relationship validation
groupBy
Aggregation
Window Functions
row_number
Revenue calculation
Silver Delta Tables
```

---

## Practical Evidence

The following screenshots document the practical implementation:

```text
01_silver_notebook.png
02_customer_quarantine.png
03_silver_joined_data.png
04_silver_tables.png
05_quarantine_tables.png
```

---

## Final Result

Successfully transformed raw Bronze data into clean and trusted Silver datasets.

The pipeline now:

```text
Reads Bronze Data
        ↓
Cleans and Standardizes Data
        ↓
Applies Data Quality Rules
        ↓
Separates Invalid Records
        ↓
Validates Relationships
        ↓
Joins Trusted Data
        ↓
Creates Silver Delta Tables
```

This task established a reliable Silver layer with data quality controls, quarantine handling, relationship validation, aggregations, and PySpark transformations.
