--The below queries were created in Google BigQuery to answer the following questions on a data set called "life-expectancy.csv”

--1.	What is the average life expectancy at birth in Europe?

SELECT RegionDisplay, ROUND(AVG(Numeric), 2) AS avg_life_exp_birth
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at birth (years)"
GROUP BY RegionDisplay
HAVING TRIM(RegionDisplay)="Europe";

--Alternative query to get only the value of the average life expectancy at birth in Europe

SELECT ROUND(avg(Numeric), 2) as ALEB_EU
FROM `life-expectancy`
where RegionDisplay = 'Europe' and MetricObserved = 'Life expectancy at birth (years)';

--2.	Is Europe the region with the highest life expectancy at birth? Which region has the lowest life expectancy at birth? 

SELECT RegionDisplay, ROUND(AVG(Numeric), 2) AS avg_life_exp_birth
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at birth (years)"
GROUP BY RegionDisplay
ORDER BY ROUND(AVG(Numeric), 2) DESC;

--3.	Does Europe also have the highest life expectancy at age 60? Which country has the highest life expectancy after 60?

SELECT RegionDisplay, ROUND(AVG(Numeric), 2) AS avg_life_exp_60
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at age 60 (years)"
GROUP BY RegionDisplay
ORDER BY ROUND(AVG(Numeric), 2) DESC;

--4.	Create a pivot table that shows the average life expectancy for all three types of MetricObserved (in three separate columns) by each region (each in a separate row)

SELECT RegionDisplay, 
ROUND(AVG(CASE WHEN(MetricObserved = "Life expectancy at birth (years)") THEN Numeric END), 2) AS LEAB,
ROUND(AVG(CASE WHEN(MetricObserved = "Life expectancy at age 60 (years)") THEN Numeric END), 2) AS LE60,
ROUND(AVG(CASE WHEN(MetricObserved = "Healthy life expectancy (HALE) at birth (years)") THEN Numeric END), 2) AS HLEAB
FROM `life-expectancyy`
GROUP BY RegionDisplay;

--5.	Which countries have the highest average life expectancy at birth?

SELECT CountryDisplay, ROUND(AVG(Numeric), 2) AS avg_life_exp_birth
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at birth (years)"
GROUP BY CountryDisplay
ORDER BY ROUND(AVG(Numeric), 2) DESC;

--6.	Using three separate queries, check out which are the top 10 countries that consume the highest quantities of beer, wine and spirits respectively.

--Query 1
SELECT 
CountryDisplay, 
ROUND(AVG(wine_servings), 2) AS tot_wine
FROM `life-expectancy`
GROUP BY CountryDisplay
ORDER BY ROUND(AVG(wine_servings), 2) DESC
LIMIT 10;

--Query 2
SELECT 
CountryDisplay, 
ROUND(AVG(beer_servings), 2) AS tot_beer
FROM `life-expectancy`
GROUP BY CountryDisplay
ORDER BY ROUND(AVG(beer_servings), 2) DESC
LIMIT 10;

--Query 3
SELECT 
CountryDisplay, 
ROUND(AVG(spirit_servings), 2) AS tot_spirits
FROM `life-expectancy`
GROUP BY CountryDisplay
ORDER BY ROUND(AVG(spirit_servings), 2) DESC
LIMIT 10;

--7.	Now create a new variable that sums the average servings of beer + wine + spirit and call it “avg_alcohol_servings”; what are the top 10 countries that consume the highest quantities of alcohol overall? 
SELECT 
CountryDisplay,
ROUND(AVG(wine_servings + spirit_servings + beer_servings), 2) AS avg_alcohol_servings
FROM `life-expectancy`
GROUP BY CountryDisplay
ORDER BY ROUND(AVG(wine_servings + spirit_servings + beer_servings), 2) DESC
LIMIT 10;

--8.	Which Country in Europe has the lowest life expectancy at birth? 
SELECT CountryDisplay, ROUND(AVG(Numeric), 2) as avg_life_exp
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at birth (years)" AND RegionDisplay="Europe"
GROUP BY CountryDisplay
ORDER BY ROUND(AVG(Numeric), 2);

--9.	Create a new variable that shows the difference between females and males life expectancy at birth and call it avg_LE_delta_gender; which country has the highest gap (in terms of years) between females and males? Are there any countries where men live more than women?

SELECT CountryDisplay, 
ROUND((AVG(CASE WHEN SexDisplay = "Female" THEN Numeric END) - AVG(CASE WHEN SexDisplay = "Male" THEN Numeric END)), 2) AS avg_LE_delta_gender
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at birth (years)"
GROUP BY CountryDisplay
ORDER BY avg_LE_delta_gender DESC;

SELECT CountryDisplay, 
ROUND((AVG(CASE WHEN SexDisplay = "Female" THEN Numeric END) - AVG(CASE WHEN SexDisplay = "Male" THEN Numeric END)), 2) AS avg_LE_delta_gender
FROM `life-expectancy`
WHERE MetricObserved="Life expectancy at birth (years)"
GROUP BY CountryDisplay
HAVING ROUND((AVG(CASE WHEN SexDisplay = "Female" THEN Numeric END) - AVG(CASE WHEN SexDisplay = "Male" THEN Numeric END)), 2) < 0
ORDER BY avg_LE_delta_gender;
