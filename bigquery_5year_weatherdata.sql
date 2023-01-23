--BigQuery for aggregating weather data over past 5 years
-- VIEW names are structured project_name.dataset_name.view_name

CREATE VIEW `root-cortex-366919.weather_proj.y18` AS
SELECT id, date, EXTRACT(MONTH FROM date) month,
IF (year.element = 'PRCP', year.value/10, NULL) AS prcp,
IF (year.element = 'TMIN', year.value/10, NULL) AS tmin,
IF (year.element = 'TMAX', year.value/10, NULL) AS tmax
FROM (
  SELECT id, date, element, value FROM `bigquery-public-data.ghcn_d.ghcnd_2018`
  WHERE element='TMIN' OR element='TMAX' OR element='PRCP'
) year

CREATE VIEW `root-cortex-366919.weather_proj.y19` AS
SELECT id, date, EXTRACT(MONTH FROM date) month,
IF (year.element = 'PRCP', year.value/10, NULL) AS prcp,
IF (year.element = 'TMIN', year.value/10, NULL) AS tmin,
IF (year.element = 'TMAX', year.value/10, NULL) AS tmax
FROM (
  SELECT id, date, element, value FROM `bigquery-public-data.ghcn_d.ghcnd_2019`
  WHERE element='TMIN' OR element='TMAX' OR element='PRCP'
) year

CREATE VIEW `root-cortex-366919.weather_proj.y20` AS
SELECT id, date, EXTRACT(MONTH FROM date) month,
IF (year.element = 'PRCP', year.value/10, NULL) AS prcp,
IF (year.element = 'TMIN', year.value/10, NULL) AS tmin,
IF (year.element = 'TMAX', year.value/10, NULL) AS tmax
FROM (
  SELECT id, date, element, value FROM `bigquery-public-data.ghcn_d.ghcnd_2020`
  WHERE element='TMIN' OR element='TMAX' OR element='PRCP'
) year

CREATE VIEW `root-cortex-366919.weather_proj.y21` AS
SELECT id, date, EXTRACT(MONTH FROM date) month,
IF (year.element = 'PRCP', year.value/10, NULL) AS prcp,
IF (year.element = 'TMIN', year.value/10, NULL) AS tmin,
IF (year.element = 'TMAX', year.value/10, NULL) AS tmax
FROM (
  SELECT id, date, element, value FROM `bigquery-public-data.ghcn_d.ghcnd_2021`
  WHERE element='TMIN' OR element='TMAX' OR element='PRCP'
) year


CREATE VIEW `root-cortex-366919.weather_proj.y22` AS
SELECT id, date, EXTRACT(MONTH FROM date) month,
IF (year.element = 'PRCP', year.value/10, NULL) AS prcp,
IF (year.element = 'TMIN', year.value/10, NULL) AS tmin,
IF (year.element = 'TMAX', year.value/10, NULL) AS tmax
FROM (
  SELECT id, date, element, value FROM `bigquery-public-data.ghcn_d.ghcnd_2022`
  WHERE element='TMIN' OR element='TMAX' OR element='PRCP'
) year


CREATE VIEW `root-cortex-366919.weather_proj.5year_TMIN_TMAX_PRCP`
SELECT * FROM
	`root-cortex-366919.weather_proj.y18`
UNION DISTINCT

SELECT * FROM
	`root-cortex-366919.weather_proj.y19`
UNION DISTINCT

SELECT * FROM
	`root-cortex-366919.weather_proj.y20`
UNION DISTINCT

SELECT * FROM
	`root-cortex-366919.weather_proj.y21`
UNION DISTINCT

SELECT * FROM
	`root-cortex-366919.weather_proj.y22`
	
	
CREATE VIEW `root-cortex-366919.weather_proj.monthly_5y_avg` AS
SELECT id, month, AVG(prcp) as avg_prcp, AVG(tmin) as avg_tmin, AVG(tmax) as avg_tmax
FROM 
`root-cortex-366919.weather_proj.5year_TMIN_TMAX_PRCP`
GROUP BY id, month