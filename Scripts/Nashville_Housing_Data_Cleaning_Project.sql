/*
Nashville Housing Data Cleaning Project
Source: Kaggle (Nashville Housing Dataset)
Author: John Aries Caballero
Objective: To clean and transform raw housing data into a format suitable for analysis.
Techniques: Joins, CTEs, Window Functions, String Parsing, and Schema Standardization.
*/


-- ----------------------------------------------------------------------------------------------------------------------


-- 1. Setup Environment & Import Data
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';


-- Load Data from csv to database housing_data
-- Note: Update the path below to your local file location
LOAD DATA LOCAL INFILE "C:/Users/jarie/OneDrive/Documents/Data Analyst/mySQL/Nashville Housing Data for Data Cleaning.csv"
INTO TABLE housing_data
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- ----------------------------------------------------------------------------------------------------------------------


-- 2. Creating a staging table
-- We use a staging table to preserve the raw data while performing transformations.
CREATE TABLE `staging1_data` (
  `ï»¿UniqueID` int DEFAULT NULL,
  `ParcelID` text,
  `LandUse` text,
  `PropertyAddress` text,
  `SaleDate` text,
  `SalePrice` int DEFAULT NULL,
  `LegalReference` text,
  `SoldAsVacant` text,
  `OwnerName` text,
  `OwnerAddress` text,
  `Acreage` double DEFAULT NULL,
  `TaxDistrict` text,
  `LandValue` int DEFAULT NULL,
  `BuildingValue` int DEFAULT NULL,
  `TotalValue` int DEFAULT NULL,
  `YearBuilt` int DEFAULT NULL,
  `Bedrooms` int DEFAULT NULL,
  `FullBath` int DEFAULT NULL,
  `HalfBath` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Identifying and flagging duplicates using ROW_NUMBER()
ALTER TABLE staging1_data
ADD COLUMN row_num INT;

INSERT INTO staging1_data
SELECT *,
ROW_NUMBER() OVER(PARTITION BY "ï»¿UniqueID", ParcelID, LandUse, PropertyAddress, SaleDate, SalePrice, LegalReference, SoldAsVacant, OwnerName, OwnerAddress, Acreage, TaxDistrict, LandValue, BuildingValue, TotalValue, YearBuilt, Bedrooms, FullBath, HalfBath ORDER BY "ï»¿UniqueID") AS row_num
FROM housing_data;


-- Deleting duplicates where row_num > 1
-- Deleting Duplicate Rows
DELETE
FROM staging1_data
WHERE row_num > 1;


-- ----------------------------------------------------------------------------------------------------------------------
-- 1. COLUMN & DATA TYPE STANDARDIZATION
-- ----------------------------------------------------------------------------------------------------------------------


-- Standardizing column header 'ï»¿UniqueID' format to ensure clean column headers.
-- Fix the BOM encoding issue on the UniqueID column
ALTER TABLE staging1_data
RENAME COLUMN `ï»¿UniqueID` TO `UniqueID`;

-- Convert SaleDate from text string to actual DATE format
UPDATE staging1_data
SET SaleDate = str_to_date(SaleDate, "%M %d, %Y");

ALTER TABLE staging1_data
MODIFY COLUMN SaleDate DATE; 


-- ----------------------------------------------------------------------------------------------------------------------
-- 2. POPULATING MISSING DATA
-- ----------------------------------------------------------------------------------------------------------------------


-- Updating blank values to NULL
UPDATE staging1_data
SET PropertyAddress = NULL
WHERE PropertyAddress = "";

UPDATE staging1_data
SET OwnerName = NULL
WHERE OwnerName = "";


-- Populate PropertyAddress by matching ParcelID from other rows
SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress
FROM staging1_data a
JOIN staging1_data b ON a.ParcelID = b.ParcelID
WHERE a.PropertyAddress IS NULL and b.PropertyAddress IS NOT NULL;

UPDATE staging1_data a
JOIN staging1_data b ON a.ParcelID = b.ParcelID
SET a.PropertyAddress = b.PropertyAddress
WHERE a.PropertyAddress IS NULL AND b.PropertyAddress IS NOT NULL;


-- ----------------------------------------------------------------------------------------------------------------------
-- 3. PARSING & SPLITTING COLUMNS (Property & Owner Addresses)
-- ----------------------------------------------------------------------------------------------------------------------


-- Splitting PropertyAddress into (Address, City)
SELECT PropertyAddress,
	CASE
		WHEN LOCATE(",", PropertyAddress)>0
        THEN TRIM(SUBSTRING(PropertyAddress, 1, LOCATE(",", PropertyAddress)-1))
        ELSE PropertyAddress
	END AS Address,
    CASE
		WHEN LOCATE(",", PropertyAddress)>0
        THEN TRIM(SUBSTRING(PropertyAddress, LOCATE(",", PropertyAddress)+1))
        ELSE NULL
	END AS City
FROM staging1_data;

-- Updating Address data
ALTER TABLE staging1_data
ADD COLUMN Address VARCHAR(255);

UPDATE staging1_data
SET Address = CASE 
	WHEN LOCATE(",", PropertyAddress) > 0
    THEN TRIM(SUBSTRING(PropertyAddress, 1, LOCATE(",", PropertyAddress)-1))
    ELSE PropertyAddress
    END;

-- Updating City data
ALTER TABLE staging1_data
ADD COLUMN City VARCHAR(255);

UPDATE staging1_data
SET City = CASE
	WHEN LOCATE(",", PropertyAddress) > 0
    THEN TRIM(SUBSTRING(PropertyAddress, LOCATE(",", PropertyAddress)+1))
    ELSE null
    END;


-- Splitting OwnerAddress into (Address, City, State) using SUBSTRING_INDEX
SELECT OwnerAddress,
	CASE
	WHEN LOCATE(",", OwnerAddress) > 0
    THEN TRIM(SUBSTRING_INDEX(OwnerAddress, ",", 1))
    ELSE OwnerAddress
    END AS OwnerSplitAddress,
    CASE
    WHEN (LENGTH(OwnerAddress) - LENGTH(REPLACE(OwnerAddress, ",", ""))) >= 2
    THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ",", 2), ",", -1))
    ELSE NULL
    END AS OwnerCity,
    CASE
    WHEN LOCATE(",", OwnerAddress) > 0
    THEN TRIM(SUBSTRING_INDEX(OwnerAddress, ",", -1))
    ELSE NULL
    END AS OwnerState
 FROM staging1_data;

ALTER TABLE staging1_data
ADD COLUMN OwnerSplitAddress VARCHAR(255);

UPDATE staging1_data
SET OwnerSplitAddress = CASE
	WHEN LOCATE(",", OwnerAddress) > 0
    THEN TRIM(SUBSTRING_INDEX(OwnerAddress, ",", 1))
    ELSE OwnerAddress
    END;
 
 ALTER TABLE staging1_data
 ADD COLUMN OwnerCity VARCHAR(255);
 
 UPDATE staging1_data
 SET OwnerCity = CASE
    WHEN (LENGTH(OwnerAddress) - LENGTH(REPLACE(OwnerAddress, ",", ""))) >= 2
    THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ",", 2), ",", -1))
    ELSE NULL
    END;
    
ALTER TABLE staging1_data
ADD COLUMN OwnerState VARCHAR(255);

UPDATE staging1_data
SET OwnerState = CASE
    WHEN LOCATE(",", OwnerAddress) > 0
    THEN TRIM(SUBSTRING_INDEX(OwnerAddress, ",", -1))
    ELSE NULL
    END;
     
SELECT *
FROM staging1_data;


-- ----------------------------------------------------------------------------------------------------------------------
-- 4. DATA NORMALIZATION (Standardizing Values)
-- ----------------------------------------------------------------------------------------------------------------------


-- Standardize 'SoldAsVacant' field to 'Yes' and 'No' for consistency
SELECT ParcelID, SoldAsVacant
FROM staging1_data
WHERE SoldAsVacant = "y";

UPDATE staging1_data
SET SoldAsVacant = CASE
	WHEN UPPER(SoldAsVacant) IN ("Y", "YES") THEN "Yes"
    WHEN UPPER(SoldAsVacant) IN ("N", "NO") THEN "No"
    ELSE SoldAsVacant
    END;

SELECT SoldAsVacant, COUNT(SoldAsVacant)
FROM staging1_data
GROUP BY SoldAsVacant;


-- ----------------------------------------------------------------------------------------------------------------------
-- 5. REMOVING UNUSED COLUMNS
-- ----------------------------------------------------------------------------------------------------------------------


ALTER TABLE staging1_data
DROP COLUMN PropertyAddress,
DROP COLUMN OwnerAddress,
DROP COLUMN row_num,
DROP COLUMN TaxDistrict;


-- Final View of Cleaned Data
SELECT *
FROM staging1_data
LIMIT 100;