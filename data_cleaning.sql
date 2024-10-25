-- Data Cleaning

select * from layoffs;

-- 1. remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 
`date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as 
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 
`date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num>1;


CREATE TABLE `layoffs_staging_2` (
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

select * from layoffs_staging_2;  

insert into layoffs_staging_2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 
`date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging_2
where row_num>1;

delete from layoffs_staging_2
where row_num>1;

select * from layoffs_staging_2
where row_num>1;

-- standardizing the data


select distinct company from layoffs_staging_2;

select company,trim(company) from layoffs_staging_2;

select distinct industry from layoffs_staging_2
order by 1;

update layoffs_staging_2
set industry='crypto'
where industry like 'crypto%';

select distinct industry from layoffs_staging_2
order by 1;

select country, trim(Trailing '.' from country)
from layoffs_staging_2
order by 1;

update layoffs_staging_2
set country= trim(trailing '.' from country)
where country like 'United States%';

select * from layoffs_staging_2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging_2;

update layoffs_staging_2
set `date`=str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging_2
modify column `date` date;

-- Removing NULL values

select * from layoffs_staging_2
where total_laid_off is NULL
and percentage_laid_off is NULL;

select * from layoffs_staging_2
where industry is null
or industry ='';

select * from layoffs_staging_2
where company='Airbnb';

select * from layoffs_staging_2 t1
join layoffs_staging_2 t2 
on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;
 
update layoffs_staging_2
set industry = null
where industry = '';

update layoffs_staging_2 t1
join layoffs_staging_2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

-- delete rows and column

select * from layoffs_staging_2
where total_laid_off is NULL
and percentage_laid_off is NULL;

Delete from layoffs_staging_2
where total_laid_off is NULL
and percentage_laid_off is NULL;

select * from layoffs_staging_2;

alter table layoffs_staging_2
drop column row_num;








