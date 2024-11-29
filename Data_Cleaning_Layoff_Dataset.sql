-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. REMOVING DUPLICATES

-- Creating a new table 'layoffs_staging' so that the raw data will not change and can be used any time
CREATE TABLE layoffs_staging
like layoffs;

INSERT INTO layoffs_staging
SELECT * FROM
layoffs;

SELECT * FROM
layoffs_staging;

-- For removing duplicates it will be very easy if we have Id or any other primary key
-- But here we don't and so we use row_number to create unique values
SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Oda';

-- While we use only few columns for partition, we could see that unique values also get row number as 2
-- So we use all the column to partition so that we can get only duplicate rows
WITH duplicate_cte AS 
(
SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

-- We can't update or Delete the values in CTE 
-- So we creare a new table and intert the data with row number column
CREATE TABLE `layoffs_staging1` (
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


SELECT * 
FROM layoffs_staging1;

INSERT INTO layoffs_staging1
SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

-- Now we Delete the rows which has row number Greater than 1
DELETE
FROM layoffs_staging1
WHERE row_num>1;

SELECT *
FROM layoffs_staging1
WHERE row_num>1;


-- 2. STANDARDIZING DATA

-- In company column there are some extra space available before the name
-- That is being removed using Trim
SELECT company, TRIM(company)
FROM layoffs_staging1;

UPDATE layoffs_staging1
SET company = TRIM(company);

-- There are some industries where the name is repaeated in different form
-- That is being changes and updated 
SELECT DISTINCT industry
FROM layoffs_staging1
ORDER BY 1 ASC;

UPDATE layoffs_staging1
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging1
ORDER BY 1;

-- Now we look into country and see some changes
-- We see United State is repeated twicw with a full stop
SELECT DISTINCT country
FROM layoffs_staging1
ORDER BY 1;

UPDATE layoffs_staging1
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Another way that can be used to remove the point is by using trim
UPDATE layoffs_staging1
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- The date column is in text data type. This format is inappropriate for Time Series analysis and other analysis
-- So, we are going to change the format
SELECT `date`, 
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging1;

UPDATE layoffs_staging1
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Now change the text column to date column
ALTER TABLE layoffs_staging1
MODIFY COLUMN `date` DATE;

-- 3. REMOVING NULL VALUES
-- Now we are looking for blank and Null values of column Industry
SELECT *
FROM layoffs_staging1
WHERE industry IS NULL
OR industry = '';

-- We first convert all the Blank values into Null values.
-- Then it will be easier for us to convert the Null values into actual values
UPDATE layoffs_staging1
SET industry = NULL
WHERE industry = '';

-- Checking if we have more than one layoffs so we can find the industry
SELECT *
FROM layoffs_staging1
WHERE company = 'Airbnb';

-- Create a Self join in such a way that t1 table retrieve null values 
-- whereas t2 table retrieve not null values 
SELECT t1.industry, t2.industry
FROM layoffs_staging1 t1
JOIN layoffs_staging1 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- Now update the t1 table with the not null values of t2 table for the clumn Industry
UPDATE layoffs_staging1 t1
JOIN layoffs_staging1 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '' )
AND t2.industry IS NOT NULL;

-- Only one company Null values is not changed and it don't have Multiple layoffs. 
-- So we leave them undisturbed
SELECT *
FROM layoffs_staging1
WHERE company = "Bally's Interactive";

-- We are seeing the null values of two columns(total_laid_off, percentage_laid_off)
SELECT * 
FROM layoffs_staging1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- As two main Colum are not present I believe that these rows are not needed and deleting them.
DELETE 
FROM layoffs_staging1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- As we completed the Data Cleaning we remove the Row_num column which is not required
ALTER TABLE layoffs_staging1
DROP COLUMN row_num;
