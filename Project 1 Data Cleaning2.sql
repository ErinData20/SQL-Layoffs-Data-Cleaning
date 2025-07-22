drop table if exists layoffs_staging;
drop table if exists layoffs_staging2;
-- 1 Drop Duplicates --
create table layoffs_staging
like layoffs;

insert layoffs_staging
select *
from layoffs;

select *
from layoffs_staging;

select * ,
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num
from layoffs_staging;

with duplicate_cte as 
(
select * ,
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num
from layoffs_staging)

select *
from duplicate_cte
where row_num = 1;

drop table if exists layoffs_staging2;

create table layoffs_staging2 as
with duplicate_cte as 
(
select * ,
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num
from layoffs_staging)

select company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
funds_raised_millions
from duplicate_cte
where row_num = 1;

select *
from layoffs_staging2;

-- 2 Standardizing Data -- finding issues -- trim get rid of space

select company, (trim(company))
from layoffs_staging2;
-- update the table --
update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';

select distinct country
from layoffs_staging2
order by 1;

-- FORMATTING THE DATE --

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2;

-- 3 null and blank values --

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'bally%';

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company = st2.company
    and st1.location = st2.location
where st1.industry is null or st1.industry = ''
and st2.industry is not null;

update layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company = st2.company
set st1.industry = st2.industry
where st1.industry is null
and st2.industry is not null;


select *
from layoffs_staging2;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


















