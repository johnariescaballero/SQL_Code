-- DATA CLEANING

-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZING DATA
-- 3. CHECK NULL VALUES
-- 4. DROP UNNECESSARY COLUMNS

-- STEP 1: REMOVING DUPLICATES
SELECT *
FROM layoffs;

-- CREATING STAGING1 TO START CLEANING layoffs TABLE
CREATE TABLE layoffs_staging1
LIKE layoffs;

-- INSERTING ALL DATA FROM layoffs TO layoffs_staging1
INSERT INTO layoffs_staging1
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging1;

-- FLAG DUPLICATE ROW
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging1;

-- SEE DUPLICATE ROWS IN TEMPORARY TABLE
WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging1
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- CREATING STAGING 2 TO START REMOVING DUPLICATES
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- INSERTING DATA FROM layoffs_staging1 TO layoffs_staging2
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging1;

-- CHECK TABLE
SELECT *
FROM layoffs_staging2;

-- DELETE DUPLICATES
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- VERIFY NO DUPLICATES
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- STANDARDIZING DATA
-- CHECK company FORMAT
SELECT *
FROM layoffs_staging2;

SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- CHECK location COLUMN
SELECT DISTINCT location
FROM layoffs_staging2;

-- CHECK industry COLUMN
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1 ASC;

-- CHECH CRYPTO DEFAULT VALUE
SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

-- UPDATING ALL CRYPTO LIKE TO CRYPTO
UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%"; -- UPDATE SUCCESSFUL

-- CHECK country FORMAT
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1 ASC;

-- UPDATING UNITED STATES TRAILING TO UNITED STATES
SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging2
WHERE country LIKE "United States%";

UPDATE layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";

-- CHANGE DATE FORMAT
SELECT `date`,
str_to_date(`date`, "%m/%d/%Y")
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, "%m/%d/%Y");

-- CHANGE DATA TYPE OF COLUMN `date`
ALTER TABLE layoffs_Staging2
MODIFY COLUMN `date` DATE;

-- SUCCESSFULL STANDARDIZED DATA

-- CHECK NULL VALUES
SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally%";

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

SELECT t1.company, t1.industry, t2.industry, t2.company
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL OR t1.industry = ""
AND t2.industry IS NOT NULL;

-- UPDATING NULL VALUES
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- DELETING Bally's Interactive company SINCE THIS HAS NULL industry AND total_laid_off
DELETE
FROM layoffs_staging2
WHERE company LIKE "Bally%";

-- SUCCESSFULLY UPDATED AND CHECKED NULL VALUES
SELECT *
FROM layoffs_staging2;

-- DROP UNNECESSARY COLUMN row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;