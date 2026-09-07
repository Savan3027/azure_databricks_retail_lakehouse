# Task 1: Azure Databricks Setup, ADLS, and Unity Catalog

## Objective

Set up the foundation for an end to end Azure Databricks Retail Lakehouse project.

## What I Completed

Created an Azure Databricks workspace:

`retail-databricks-project`

Created a Databricks project folder:

`Retail_Lakehouse_Project`

Created the first notebook:

`01_Project_Setup`

Created an Azure Data Lake Storage Gen2 account:

`retaildbxdata2026`

Created a source container:

`retail-source`

Created source folders for:

`initial`

`incremental`

`bad-data`

`schema-evolution`

`streaming`

Uploaded the initial retail dataset containing:

`customers.csv`

`products.csv`

`orders.csv`

`order_items.csv`

`payments.json`

Created an Azure Databricks Access Connector:

`ac-retail-databricks`

Configured Managed Identity and Azure Storage permissions so Databricks can securely access ADLS.

Created a Unity Catalog Storage Credential.

Created an External Location connected to the ADLS source container.

Created the Unity Catalog catalog:

`retail_lakehouse`

Created the following schemas:

`landing`

`bronze`

`silver`

`gold`

`quarantine`

## Architecture

```text
Retail Source Files
        ↓
ADLS Gen2
        ↓
Access Connector
        ↓
Managed Identity
        ↓
Storage Credential
        ↓
External Location
        ↓
Unity Catalog
        ↓
retail_lakehouse
        ↓
landing
bronze
silver
gold
quarantine
