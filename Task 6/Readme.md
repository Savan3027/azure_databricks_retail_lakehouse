# Task 6: Spark Performance, Optimization, Jobs and Monitoring

## Objective

In this task, I focused on Spark performance optimization and production pipeline operations in Azure Databricks.

The main areas included execution plans, partitions, shuffle, Broadcast Join, data skew, Delta OPTIMIZE, Databricks Jobs, scheduling, dependencies, retries, failure handling, and monitoring.

---

## Architecture

```text
Data Processing
      ↓
Spark Performance Analysis
      ↓
Optimization
      ↓
Databricks Jobs
      ↓
Scheduling
      ↓
Failure Handling
      ↓
Monitoring
```

---

## 1. Spark Execution Plan

Spark execution plans were inspected using:

```python
orders_df.explain()
```

An execution plan shows how Spark plans to execute a query.

It helps investigate:

```text
Table Scans
Join Strategy
Shuffle
Exchange Operations
Query Execution
```

This is useful when a query becomes slow after adding transformations or joins.

---

## 2. Spark Architecture

The following Spark components were reviewed:

```text
Driver
Worker
Executor
Job
Stage
Task
Partition
```

Basic flow:

```text
Driver
   ↓
Creates Work
   ↓
Executors
   ↓
Tasks
   ↓
Partitions
```

The Driver coordinates the Spark application while Executors perform the actual data processing.

---

## 3. Partitions

Spark divides data into partitions so multiple tasks can process data in parallel.

Too few partitions can result in:

```text
Low Parallelism
Idle Executors
Slower Processing
```

Partitions can be changed using:

```python
repartition()
```

---

## 4. Repartition and Coalesce

`repartition()` redistributes data into a new number of partitions.

It can increase or decrease partitions and generally causes shuffle.

`coalesce()` is mainly used to reduce the number of partitions with less data movement.

Example:

```text
500 Partitions
      ↓
coalesce(20)
      ↓
20 Partitions
```

This can also help control the number of output files.

---

## 5. Shuffle

Shuffle happens when Spark moves data between partitions.

Operations such as:

```text
JOIN
GROUP BY
REPARTITION
ORDER BY
```

may cause shuffle.

Large shuffle operations can increase network communication and slow Spark jobs.

---

## 6. Broadcast Join

A large table was joined with a small Products table using Broadcast Join.

```python
from pyspark.sql.functions import broadcast

broadcast_join_df = (
    items_df
    .join(
        broadcast(products_df),
        "product_id",
        "inner"
    )
)
```

With Broadcast Join, Spark sends the small table to executors.

This avoids heavily shuffling the large table.

The execution plan was inspected for:

```text
BroadcastHashJoin
```

---

## 7. Data Skew

Data skew occurs when data is not distributed evenly between partitions.

Example:

```text
Customer A
50,000,000 transactions

Most Customers
100 transactions
```

One Spark task may receive much more data than other tasks and therefore take significantly longer.

Data skew can be investigated using:

```text
Spark UI
Task Duration
Partition Size
Record Distribution
```

Possible approaches include:

```text
Repartitioning
Salting
Broadcast Join when appropriate
Adaptive Query Execution
```

---

## 8. Small File Problem

A Delta table containing many tiny files can become slower to query because Spark must manage and read many individual files.

Databricks provides:

```sql
OPTIMIZE table_name;
```

OPTIMIZE combines smaller Delta files into fewer larger files.

This improves file organization and can improve query performance.

---

## 9. VACUUM

VACUUM can remove old Delta files that are no longer required after the configured retention period.

Example:

```sql
VACUUM table_name
RETAIN 168 HOURS;
```

Retention should be handled carefully because older files may still be required for Time Travel and recovery.

---

## 10. Databricks Job Notebook

A validation notebook was created to test automated execution.

Example:

```python
print("Retail pipeline started")

count = spark.table(
    "retail_lakehouse.gold.fact_sales"
).count()

print(
    "Fact sales rows:",
    count
)

print("Retail pipeline completed")
```

This notebook was executed automatically through a Databricks Job.

---

## 11. Databricks Jobs

A Databricks Job was created for the Retail Lakehouse project.

The Job executed notebook tasks automatically instead of requiring manual notebook execution.

A successful Job run confirmed that the Gold table validation could run automatically.

---

## 12. Task Dependencies

Dependencies control the order in which Job tasks execute.

Example:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Validation
```

Gold can be configured to depend on Silver.

If Silver fails, Gold should not continue.

This prevents downstream tasks from using incomplete or incorrect data.

---

## 13. Scheduling

The Databricks Job was configured with a schedule.

Scheduling allows pipelines to execute automatically at required times.

Example:

```text
Daily Pipeline
Every Morning
Automatic Execution
```

---

## 14. Failure Testing

A failure was intentionally created using:

```python
raise Exception(
    "Demo pipeline failure"
)
```

This allowed Job failure behavior and monitoring to be tested practically.

---

## 15. Retry Strategy

Retries can be configured for temporary failures such as:

```text
Network Problems
Temporary Service Issues
Transient Errors
```

Retries should be limited so recurring production problems are not hidden.

Repeated failures should be investigated through logs and monitoring.

---

## 16. Task Logs

Task logs provide information about what happened during a Job run.

Logs help identify:

```text
Which task failed
Where the failure occurred
Error message
Execution details
Task output
```

They are an important part of production troubleshooting.

---

## 17. Monitoring

Databricks Job monitoring was used to inspect:

```text
Run Status
Task Status
Start Time
Duration
Task Output
Failure Details
Run History
```

Monitoring helps identify both performance problems and pipeline failures.

---

## 18. Production Performance Investigation

A slow Spark pipeline can be investigated in a structured way:

```text
Check Task Logs
      ↓
Check Spark Execution Plan
      ↓
Check Table Scans
      ↓
Check Shuffle
      ↓
Check Join Strategy
      ↓
Check Partitions
      ↓
Check Data Skew
      ↓
Check Small Files
      ↓
Apply Optimization
      ↓
Validate Performance
```

---

## 19. Production Scenario

A production pipeline may contain several problems at the same time:

```text
Large Sales Table
Small Products Table
Thousands of Tiny Delta Files
One Customer Creating Data Skew
Scheduled Daily Pipeline
Occasional Failures
```

Possible solutions include:

```text
Broadcast Join for Small Tables
OPTIMIZE for Small Files
Spark UI for Data Skew Investigation
Repartitioning or Salting for Skew
Task Dependencies for Correct Execution Order
Limited Retries for Temporary Failures
Scheduling for Automation
Task Logs and Monitoring for Troubleshooting
```

---

## Serverless Compute Note

Manual DataFrame caching was reviewed conceptually.

The Serverless compute environment used in this project did not support the manual cache operation attempted during the practical exercise.

The caching concept was still studied as a Spark performance technique for repeatedly reused DataFrames.

---

## Key Concepts Practiced

```text
Spark Architecture
Driver
Worker
Executor
Job
Stage
Task
Partition
Parallelism
Execution Plan
Scan
Shuffle
Repartition
Coalesce
Broadcast Join
BroadcastHashJoin
Data Skew
Spark UI
Salting
OPTIMIZE
VACUUM
Databricks Jobs
Task Dependencies
Scheduling
Retries
Task Logs
Failure Handling
Monitoring
Production Troubleshooting
```

---

## Practical Evidence

```text
01_task6_notebook.png
02_broadcast_join_plan.png
03_optimize_result.png
04_job_notebook.png
06_job_success.png
07_job_schedule.png
08_job_failure.png
09_job_monitoring.png
10_task6_final_job.png
```

---

## Final Result

Successfully practiced Spark performance optimization and production pipeline operations in Azure Databricks.

The final operational flow was:

```text
Spark Processing
      ↓
Execution Plan Analysis
      ↓
Performance Optimization
      ↓
Databricks Job
      ↓
Task Dependencies
      ↓
Scheduling
      ↓
Retries
      ↓
Monitoring
      ↓
Production Troubleshooting
```

This task completed the production focused portion of the Retail Lakehouse project by combining Spark performance analysis, Delta optimization, workflow automation, failure handling, and monitoring.
