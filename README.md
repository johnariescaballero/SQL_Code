# Nashville Housing: End-to-End SQL Project

### 🛠️ Key SQL Skills Applied
* **Data Cleaning & ETL:** Self-Joins, `UPDATE` with `JOIN` logic, Staging table creation.
* **Advanced Transformations:** Window Functions (`ROW_NUMBER`), CTEs.
* **String Manipulation:** `SUBSTRING_INDEX`, `LOCATE`, `TRIM`, `REPLACE`.
* **Schema Design:** DDL (`ALTER`, `DROP`), Data type standardization, Primary Key management.

## 1. Project Overview
This project focuses on the cleaning and exploratory analysis of a Nashville Housing dataset. The goal is to transform a raw, unorganized dataset into a structured format for business intelligence and market research.

## 2. Database Schema (ERD)
* **Below is the visual map of the database. I transformed the raw housing_data into a refined staging1_data table, adding engineered columns for addresses and standardized dates.

## 3. Data Cleaning Highlights
* **Self-Joins: Populated missing property addresses by matching Parcel IDs.
* **String Parsing: Split PropertyAddress and OwnerAddress into atomic columns (Address, City, State).
* **De-duplication: Identified and removed duplicates using ROW_NUMBER().

## 4. Repository Structure
* **Scripts/Nashville_Housing_Data_Cleaning_Project.sql: Full SQL transformation script.
* **Scripts/02_eda_queries.sql: (Coming Soon) Market trend analysis.
* **Visuals/: Contains the schema diagram.

<img width="346" height="535" alt="Nashville_Housing_ERD" src="https://github.com/user-attachments/assets/5d5fd6c6-795a-4156-aa7a-a6545a1bc78b" />
