# Task 4: Delta Lake, Incremental Loading, MERGE and SCD

## Objective

In this task, I implemented incremental data processing using Delta Lake in Azure Databricks.

The main focus was Delta table history, UPDATE, DELETE, Time Travel, RESTORE, MERGE, UPSERT, SCD Type 1, and SCD Type 2.

---

## Architecture

```text
Existing Silver Data
        +
Incremental Files
        ↓
Data Cleaning
        ↓
Delta MERGE
        ↓
Existing Records → Update
New Records      → Insert
        ↓
SCD Type 1 / SCD Type 2
```

---

## Incremental Source Files

The following incremental files were processed:

```text
customers_2026_09_08.csv
orders_2026_09_08.csv
order_items_2026_09_08.csv
payments_2026_09_08.json
```

These files represented newly arrived daily data.

---

## 1. Delta Table History

Delta history was inspected using:

```sql
DESCRIBE HISTORY retail_lakehouse.silver.customers;
```

Delta Lake keeps transaction history for table operations such as:

```text
CREATE
UPDATE
DELETE
MERGE
RESTORE
```

---

## 2. UPDATE and DELETE

A demo Delta table was created so changes could be tested safely.

Example update:

```sql
UPDATE retail_lakehouse.silver.customers_delta_demo
SET city = 'DemoCity'
WHERE customer_id = 'C00001';
```

Example delete:

```sql
DELETE FROM retail_lakehouse.silver.customers_delta_demo
WHERE customer_id = 'C00002';
```

The table history was checked again to verify the operations.

---

## 3. Delta Time Travel

An older Delta table version was accessed using:

```sql
SELECT *
FROM retail_lakehouse.silver.customers_delta_demo
VERSION AS OF 0;
```

Time Travel allows previous versions of a Delta table to be inspected.

This is useful for:

```text
Troubleshooting
Auditing
Comparing old and new data
Recovering from incorrect updates or deletes
```

---

## 4. RESTORE

A previous Delta version was restored using:

```sql
RESTORE TABLE retail_lakehouse.silver.customers_delta_demo
TO VERSION AS OF 0;
```

This demonstrates how Delta Lake can recover from accidental data changes.

---

## 5. Read Incremental Data

New customer data was read from ADLS Gen2.

```python
customers_incremental_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        incremental_path +
        "customers_2026_09_08.csv"
    )
)
```

The incoming data was cleaned using the same Silver layer rules.

---

## 6. Delta MERGE

Delta MERGE was used to process both existing and new customers.

```python
customers_target.alias("target") \
    .merge(
        customers_incremental_clean_df.alias("source"),
        "target.customer_id = source.customer_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

The logic was:

```text
Customer already exists
        ↓
UPDATE

Customer does not exist
        ↓
INSERT
```

This combination is known as an UPSERT.

---

## 7. SCD Type 1

SCD stands for Slowly Changing Dimension.

SCD Type 1 keeps only the latest value.

Example:

```text
Old City
Toronto

New City
Calgary

Final Record
Calgary
```

The previous value is overwritten and history is not preserved.

Delta MERGE can be used to implement SCD Type 1.

---

## 8. SCD Type 2

SCD Type 2 keeps historical versions of changed records.

The customer history table included:

```text
effective_from
effective_to
is_current
```

Example:

```text
Customer C101 | Toronto     | is_current = false
Customer C101 | Mississauga | is_current = true
```

The old version remains available while the latest version becomes current.

---

## 9. Find Changed Customers

Incoming records were compared with current customer history.

Important customer attributes such as:

```text
city
email
is_active
```

were compared to identify changed records.

These records were then used to expire the old version and create a new current version.

---

## 10. Find Brand New Customers

A Left Anti Join was used to identify customers that did not previously exist.

```python
new_customers_df = (
    customers_incremental_clean_df
    .join(
        history_current_df.select("customer_id"),
        "customer_id",
        "left_anti"
    )
)
```

These customers were inserted into the history table as new current records.

---

## 11. Incremental Orders

New orders were cleaned and merged into the existing Silver Orders table.

The MERGE key was:

```text
order_id
```

Existing orders were updated and new orders were inserted.

---

## 12. Incremental Order Items

Order items were merged using:

```text
order_id + line_number
```

This combination uniquely identifies an order item.

Numeric columns such as quantity, price, discount, and line total were converted to appropriate data types before MERGE.

---

## 13. Incremental Payments

New payment records were processed using Delta MERGE.

The matching key was:

```text
payment_id
```

Existing payments were updated and new payments were inserted.

---

## 14. Validation

Row counts were checked after incremental processing.

Delta history was also inspected to confirm that MERGE operations were successfully executed.

---

## Key Concepts Practiced

```text
Delta Lake
Delta Table History
UPDATE
DELETE
Time Travel
VERSION AS OF
RESTORE
Incremental Loading
MERGE
UPSERT
SCD Type 1
SCD Type 2
effective_from
effective_to
is_current
Left Anti Join
Historical Records
Current Records
```

---

## Practical Evidence

```text
01_delta_merge_notebook.png
02_delta_table_history.png
03_delta_update_delete_history.png
04_incremental_source_files.png
05_customer_merge_result.png
06_scd_type2_history.png
07_incremental_merge_history.png
08_task4_silver_tables.png
```

---

## Final Result

Successfully implemented incremental processing using Delta Lake.

The final flow was:

```text
New Daily Files
      ↓
Read and Clean
      ↓
Compare with Existing Data
      ↓
Delta MERGE
      ↓
Update Existing Records
Insert New Records
      ↓
SCD Type 1
SCD Type 2
      ↓
Maintain Current and Historical Data
```

This task demonstrated how Delta Lake supports reliable incremental processing, historical tracking, recovery, and changing dimension management.
