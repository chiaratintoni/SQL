-- This list of queries was used to work with two sets of data collected for 2 power plants (source: https://www.kaggle.com/datasets/anikannal/solar-power-generation-data?select=Plant_1_Weather_Sensor_Data.csv): 
-- the “Generation Data” and the ”Weather Sensor Data”.
-- 1.The data was uploaded in Google BigQuery in a new data set named “SolarPower”. Below the query to perform this task:

CREATE TABLE your-project.Solar_Power.Generation_Data AS 
SELECT *  FROM `your-project.Solar_Power.Plant_1_Generation_Data`
UNION ALL 
SELECT * FROM `your-project.Solar_Power.Plant_2_Generation_Data`;

create TABLE your-project.Solar_Power.Weather_Sensor_Data AS 
SELECT * FROM `your-project.Solar_Power.Plant_1_Weather_Sensor_Data`
UNION ALL 
SELECT * FROM `your-project.Solar_Power.Plant_2_Weather_Sensor_Data`;

-- 2. The following query was used to count the inverters in eah plant
SELECT PLANT_ID, COUNT(DISTINCT SOURCE_KEY) AS cnt_inverter
FROM `boolean-class-2.Solar_Power.Generation_Data`
GROUP BY PLANT_ID;

-- 3. The following query was used to calculate how many day of observation were present for each plant.
SELECT PLANT_ID, COUNT(DISTINCT EXTRACT(DATE FROM date_time)) AS CNT_DAYS
FROM `boolean-class-2.Solar_Power.Generation_Data`
GROUP BY PLANT_ID;


-- 4. The following query was used to find which inverter generated the highest total yield and the plant which it belonged to.
SELECT PLANT_ID, SOURCE_KEY, MAX(TOTAL_YIELD) AS MAX_YIELD
FROM `boolean-class-2.Solar_Power.Generation_Data`
GROUP BY PLANT_ID, SOURCE_KEY
ORDER BY MAX_YIELD DESC;

-- 5. The following query shows the average AC and DC Power generated, grouped by each power plant.
SELECT 
PLANT_ID, 
ROUND(AVG(DC_POWER), 2) AS avg_DC, 
ROUND(AVG(AC_POWER), 2) AS avg_AC
FROM `boolean-class-2.Solar_Power.Generation_Data`
GROUP BY PLANT_ID
ORDER BY PLANT_ID DESC, avg_DC DESC, avg_DC DESC;

-- 6. The following query was used to calculate the overall average inverter efficiency for each Plant.
SELECT 
PLANT_ID, 
avg_DC, avg_AC, ROUND((avg_AC/avg_DC)*100, 2) AS avg_efficiency
FROM (
	SELECT 
	PLANT_ID, 
	ROUND(AVG(DC_POWER), 2) AS avg_DC, 
	ROUND(AVG(AC_POWER), 2) AS avg_AC
	FROM `boolean-class-2.Solar_Power.Generation_Data`
	GROUP BY PLANT_ID
);

-- 7. Focusing on plant_id = 4136001, the following query shows the average DA and AC Power as well as the average inverter efficiency for each hour of the day.
SELECT *, 
  CASE WHEN avg_AC = 0 THEN 0 ELSE ROUND((avg_AC/avg_DC)*100, 2) END AS avg_efficiency
FROM (SELECT 
  PLANT_ID,
  EXTRACT(HOUR FROM DATE_TIME) AS HOUR, 
  AVG(DC_POWER) AS avg_DC, 
  AVG(AC_POWER) AS avg_AC, 
FROM `boolean-class-2.Solar_Power.Generation_Data_Clean`
WHERE PLANT_ID = 4136001
GROUP BY HOUR
ORDER BY HOUR);

-- 8. The following query was used to calculate how many inverters (source_key) there are in the Generation_Data table and in the Weather_Sensor_Data table.
WITH 
  a AS (SELECT DISTINCT(SOURCE_KEY) FROM `boolean-class-2.Solar_Power.Generation_Data`),
  b AS (SELECT DISTINCT(SOURCE_KEY) FROM `boolean-class-2.Solar_Power.Weather_Sensor_Data`)
SELECT "Generation_Data" AS DataSet, COUNT(*) AS inv_cnt FROM a
UNION ALL
SELECT "Weather_Sensor_Data" AS DataSet,COUNT(*) AS inv_cnt FROM b;


-- 9. The following subquery was used to check if there are any source keys in the Weather_Sensor_Data table that are also present in the Generation_Data table.
SELECT * 
FROM `boolean-class-2.Solar_Power.Weather_Sensor_Data`
WHERE SOURCE_KEY IN (SELECT SOURCE_KEY FROM `boolean-class-2.Solar_Power.Generation_Data`);

-- 10. As for Plant 4135001 seems to have efficiency was recorded incorrectly (it was foudn to be 10 times lower than the expected one), the Generation_Data table was re-createed via a 
-- UNION statement and was called Generation_Data_Clean. The below query was used for this tasks as well as for: fixing the DC Power problem in the Plant1 table, adding a new string column 
-- in the final table called Plant_nr where it is manually specified whether that data is relative to “plant_1” or “plant_2”.

CREATE TABLE boolean-class-2.Solar_Power.Generation_Data_Clean AS 
SELECT DATE_TIME, PLANT_ID, SOURCE_KEY, DC_POWER/10 AS DC_POWER, AC_POWER, DAILY_YIELD, TOTAL_YIELD, 1 AS PLANT_NR FROM `boolean-class-2.Solar_Power.Plant_1_Generation_Data`
UNION ALL
SELECT *, 2 AS PLANT_NR FROM `boolean-class-2.Solar_Power.Plant_2_Generation_Data`);

-- 11. The Weather_Sensor_Data table stores records of the average ambient (outdoor temp) and module (photovoltaic panel temp) temperatures as well as irradiation levels (the amount of the 
-- sun's power detected by a sensor). The following query was used to calculate the average ambient temperature, module temperature and irradiation by hour of day.
SELECT
  PLANT_ID,
  EXTRACT(HOUR FROM DATE_TIME) AS HOUR, 
  ROUND(AVG(AMBIENT_TEMPERATURE), 2) AS avg_amb_T, 
  ROUND(AVG(MODULE_TEMPERATURE), 2) AS avg_mod_T,
  ROUND(AVG(IRRADIATION), 2) AS avg_irr
FROM `boolean-class-2.Solar_Power.Weather_Sensor_Data`
GROUP BY PLANT_ID, HOUR
ORDER BY PLANT_ID, HOUR;

-- 12. The below query was used to create a new table called Hourly_Generation_Weather_Plant.
CREATE TABLE boolean-class-2.Solar_Power.Hourly_Generation_Weather_Plant AS
SELECT a.*, b.* EXCEPT(PLANT_ID, HOUR),
  CASE WHEN avg_AC = 0 THEN 0 ELSE ROUND((avg_AC/avg_DC)*100, 2) END AS avg_efficiency
FROM (
(SELECT 
  PLANT_ID,
  EXTRACT(HOUR FROM DATE_TIME) AS HOUR, 
  AVG(DC_POWER) AS avg_DC, 
  AVG(AC_POWER) AS avg_AC, 
FROM `boolean-class-2.Solar_Power.Generation_Data_Clean`
GROUP BY PLANT_ID, HOUR
) AS a
FULL OUTER JOIN 
(SELECT
  PLANT_ID,
  EXTRACT(HOUR FROM DATE_TIME) AS HOUR, 
  ROUND(AVG(AMBIENT_TEMPERATURE), 2) AS avg_amb_T, 
  ROUND(AVG(MODULE_TEMPERATURE), 2) AS avg_mod_T,
  ROUND(AVG(IRRADIATION), 2) AS avg_irr
FROM `boolean-class-2.Solar_Power.Weather_Sensor_Data`
GROUP BY PLANT_ID, HOUR
) AS b
ON a.PLANT_ID = b.PLANT_ID AND a.HOUR = b.HOUR);

