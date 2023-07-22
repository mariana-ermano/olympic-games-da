-- Databricks notebook source
-- DBTITLE 1,Olympic Games Data Analysis
-- MAGIC %md
-- MAGIC I want to analyze if there are any specific factors that may contribute to winning medals in the Olympic 
-- MAGIC Games. 
-- MAGIC These insights may be helpful to try and predict the winning probability for a specific country/team.
-- MAGIC This project may also appeal to sport enthusiasts looking for interesting facts about the Olympics.
-- MAGIC The analysis will be focused on whether there is an impact on the number of athletes and medals won by 
-- MAGIC a country when hosting the Olympics, and the relation between these two factors.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Data import and tables creation 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Making sure the delta table locations are empty
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/olympic_games", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/top5_athletes", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/top5_medals", True)
-- MAGIC

-- COMMAND ----------

-- Imported data from two .csv files and and created the respective tables

CREATE DATABASE IF NOT EXISTS default;
USE default;

DROP TABLE IF EXISTS athlete_events;

CREATE TABLE athlete_events 
USING csv
OPTIONS (
  path "/FileStore/tables/athlete_events.csv", 
  header "true"
  );

SELECT 
  *
FROM 
  athlete_events;



-- COMMAND ----------

DROP TABLE IF EXISTS noc;

CREATE TABLE noc 
USING csv
OPTIONS (
  path "/FileStore/tables/noc_regions.csv", 
  header "true"
  );

SELECT 
  *
FROM 
  noc;


-- COMMAND ----------

-- Checking if table data types are correct

DESCRIBE athlete_events;

-- COMMAND ----------

-- Merging both tables

DROP TABLE IF EXISTS games;

CREATE OR REPLACE TABLE 
  games
AS
  (SELECT 
    CAST(a.ID AS INT) AS id,
    a.Name,
    a.Sex,
    CAST(a.Age as INT) AS age,
    CAST(a.Height AS INT) AS height ,
    CAST(a.Weight AS INT) AS weight,
    a.Team,
    a.NOC,
    n.region,
    a.Games,
    CAST(a.Year AS INT) AS year,
    a.Season,
    a.City,
    a.Sport,
    a.Event,
    a.Medal
FROM
  athlete_events a
INNER JOIN
  noc n
ON
  a.NOC = n.NOC);

SELECT 
  *
FROM
  games
LIMIT 10;


-- COMMAND ----------

-- Creating a new table to correct the data types of numeric fields

DROP TABLE IF EXISTS olympic_games;

CREATE OR REPLACE TABLE 
  olympic_games (
            id INT,
            name STRING,
            sex STRING,
            age INT,
            height INT,
            weight INT,
            team STRING,
            noc STRING,
            region STRING,
            games STRING,
            year INT,
            season STRING,
            city STRING,
            sport STRING,
            event STRING,
            medal STRING
            );



-- COMMAND ----------

-- Inserting the merged tables here

INSERT INTO 
  olympic_games
SELECT 
  *
FROM
  games;
  
  
SELECT 
  *
FROM
  olympic_games;  
  


-- COMMAND ----------

-- Correcting values for this region

UPDATE 
  olympic_games
SET 
  region = "Yugoslavia" 
WHERE
  noc = "YUG"   

-- COMMAND ----------

-- Ensuring that there aren't any repeated names

SELECT
  COUNT(id) - COUNT(name)
FROM
  olympic_games;



-- COMMAND ----------

-- Checking if there are any null values on these categories

SELECT
  *
FROM
  olympic_games
WHERE   
  noc IS NULL
  OR sport IS NULL
  OR event IS NULL
  OR sex IS NULL;

-- COMMAND ----------

-- DBTITLE 1,Data exploration and analysis
-- MAGIC %md
-- MAGIC Medals and athletes by sex
-- MAGIC

-- COMMAND ----------

-- Initial exploration: number of athletes by sex

SELECT
  sex,
  SUM(athletes) AS athletes_total
FROM
  (SELECT 
    games,  
    sex,
    COUNT(DISTINCT id) AS athletes
  FROM
    olympic_games
  GROUP BY  
    games,  
    sex)   
GROUP BY    
  sex;

-- COMMAND ----------

-- Initial exploration: number of medals by sex

SELECT
  sex,
  SUM(medals) AS medals_total
FROM
  (SELECT 
    games,  
    sex,
    COUNT(id) AS medals
  FROM
    olympic_games
  WHERE  
    medal IN ('Gold', 'Silver', 'Bronze')       
  GROUP BY  
    games,  
    sex
  )     
GROUP BY    
  sex;

-- COMMAND ----------

-- Athletes distribution over the years, Summer games

SELECT 
  year, 
  sex,
  COUNT(DISTINCT id) AS athletes
FROM
  olympic_games
WHERE
  season = 'Summer'  
GROUP BY  
  year,
  sex

-- COMMAND ----------

-- Athletes distribution over the years, Winter games

SELECT 
  year, 
  sex,
  COUNT(DISTINCT id) AS athletes
FROM
  olympic_games
WHERE
  season = 'Winter'  
GROUP BY  
  year,
  sex

-- COMMAND ----------

-- Athletes by mixed event

SELECT 
  sport,
  ROUND(AVG(DISTINCT id),2) AS athletes
FROM
  olympic_games
WHERE     
  LOWER(event) LIKE '%mixed%'
GROUP BY  
  sport
ORDER BY
  athletes DESC;

-- COMMAND ----------

-- Athletes by sex and event type

SELECT 
  event_type,
  ROUND(AVG(athletes),2) AS avg_athletes
FROM    
  (SELECT 
    games,  
    CASE
      WHEN LOWER(event) LIKE '%women%' THEN 'Women'
      WHEN LOWER(event) LIKE '%mixed%' THEN 'Mixed'
      WHEN LOWER(event) LIKE '% men%' THEN 'Men'
    END AS event_type,  
    COUNT(DISTINCT id) AS athletes
  FROM
    olympic_games
  GROUP BY  
    event_type,  
    games)
GROUP BY
  event_type;  


-- COMMAND ----------

-- Medal-athlete ratio by sex on mixed events

CREATE OR REPLACE VIEW  
  athletes_mixed_events 
AS
SELECT
  sex,
  ROUND(SUM(athletes),2) AS athletes_tot
FROM  
  (SELECT 
    games,  
    sex,
    COUNT(DISTINCT id) AS athletes
  FROM
    olympic_games
  WHERE  
    LOWER(event) LIKE '%mixed%'       
  GROUP BY  
    games,  
    sex)
GROUP BY  
    sex;    


CREATE OR REPLACE VIEW  
  medals_mixed_events 
AS
SELECT
  sex,
  ROUND(SUM(medals),2) AS medals_tot
FROM  
  (SELECT 
    games,  
    sex,
    COUNT(id) AS medals
  FROM
    olympic_games
  WHERE  
    medal IN ('Gold', 'Silver', 'Bronze')
  AND 
    LOWER(event) LIKE '%mixed%'       
  GROUP BY  
    games,  
    sex)
GROUP BY  
   sex;    


CREATE OR REPLACE VIEW  
  gold_medals_mixed_events 
AS
SELECT
  sex,
  ROUND(SUM(gold_medals),2) AS gold_medals_tot
FROM  
  (SELECT 
    games,  
    sex,
    COUNT(id) AS gold_medals
  FROM
    olympic_games
  WHERE  
    medal = 'Gold'
  AND 
    LOWER(event) LIKE '%mixed%'       
  GROUP BY  
    games,  
    sex)
GROUP BY  
   sex;  

SELECT 
  athletes_mixed_events.sex,
  medals_tot,
  athletes_tot,
  ROUND((medals_tot/athletes_tot),2) AS medal_athlete_ratio
FROM
  athletes_mixed_events
JOIN  
  medals_mixed_events
ON
  athletes_mixed_events.sex = medals_mixed_events.sex; 

 

-- COMMAND ----------

-- Gold medal-athlete ratio by sex on mixed events

SELECT 
  athletes_mixed_events.sex,
  gold_medals_tot,
  athletes_tot,
  ROUND((gold_medals_tot/athletes_tot),2) AS gold_medal_athlete_ratio
FROM
  athletes_mixed_events
JOIN  
  gold_medals_mixed_events
ON
  athletes_mixed_events.sex = gold_medals_mixed_events.sex; 

-- COMMAND ----------

-- Creating a score to better compare medal-athlete average ratio by sex on mixed events. Gold = 3 points, Silver = 2 points, Bronze = 1 point

CREATE OR REPLACE VIEW  
  score_mixed_events 
AS
SELECT
  sex,
  ROUND(SUM(score),2) AS score_tot
FROM  
  (SELECT 
    games,  
    sex,
    SUM(
      CASE
          WHEN medal = 'Gold' THEN 3
          WHEN medal = 'Silver' THEN 2
          WHEN medal = 'Bronze' THEN 1
          ELSE 0 
      END) AS score
  FROM
    olympic_games
  WHERE  
    LOWER(event) LIKE '%mixed%'       
  GROUP BY  
    games,  
    sex)
GROUP BY  
   sex;  

SELECT 
  athletes_mixed_events.sex,
  score_tot,
  athletes_tot,
  ROUND((score_tot/athletes_tot),2) AS score_athlete_ratio
FROM
  athletes_mixed_events
JOIN  
  score_mixed_events
ON
  athletes_mixed_events.sex = score_mixed_events.sex; 

-- COMMAND ----------

-- DBTITLE 1,Data exploration and analysis
-- MAGIC %md
-- MAGIC Medals and athletes by country when hosting the Olympics vs. when not hosting
-- MAGIC

-- COMMAND ----------

-- Initial exploration: Medals won by country, choosing a sample from Olympic games

SELECT
    region,
    games,
    COUNT(*) AS medals
  FROM
    olympic_games
  WHERE  
    Medal IN ('Gold', 'Silver', 'Bronze')
  AND
    games IN ('1996 Summer', '1908 Summer', '1924 Summer', '2010 Winter')  
  GROUP BY
    region,
    games
  ORDER BY  
    medals DESC;

-- COMMAND ----------

-- Athletes per country, choosing the same sample and listing only the host countries

SELECT
    region,
    games,
    COUNT(*) AS athletes
  FROM
    olympic_games
  WHERE  
    games IN ('1996 Summer', '1908 Summer', '1924 Summer', '2010 Winter') 
  AND 
    region IN ('USA', 'UK', 'France', 'Canada')   
  GROUP BY
    region,
    games
  ORDER BY  
    athletes DESC;

-- COMMAND ----------

-- Medals won by country, choosing the same sample and listing only the host countries

SELECT
    region,
    games,
    COUNT(*) AS medals
  FROM
    olympic_games
  WHERE  
    Medal IN ('Gold', 'Silver', 'Bronze')
  AND
    games IN ('1996 Summer', '1908 Summer', '1924 Summer', '2010 Winter') 
  AND 
    region IN ('USA', 'UK', 'France', 'Canada')   
  GROUP BY
    region,
    games
  ORDER BY  
    medals DESC;


-- COMMAND ----------

-- Creating a table from a csv file with the list of all the games and the host cities and the respective countries

DROP TABLE IF EXISTS 
  hosts;

CREATE TABLE 
  hosts 
USING 
  csv
OPTIONS (
  path "/FileStore/tables/hosts.csv", 
  header "true", 
  inferSchema "true"
  );

SELECT 
  *
FROM 
  hosts;

-- COMMAND ----------

-- Creating a table that lists the top 5 countries with higher number of athletes by game, and the host country of that game

DROP TABLE IF EXISTS 
  top5_athletes;

CREATE TABLE 
  top5_athletes 
USING DELTA
AS  
  (SELECT 
    country,
    games,  
    host,
    ranking,
    athletes
  FROM  
    (SELECT
      region AS country,
      games,
      host_country AS host,
      ROW_NUMBER () OVER (PARTITION BY games ORDER BY COUNT(DISTINCT id) DESC) AS ranking,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    JOIN
      hosts
    ON
      olympic_games.games = hosts.game   
    GROUP BY
      country,
      games,
      host)
  WHERE
    ranking <= 5
  ORDER BY
    ranking);

SELECT
  *
FROM
  top5_athletes;

-- COMMAND ----------

-- Creating a table that lists the top 5 countries winning more medals by game, and the host country of that game

DROP TABLE IF EXISTS 
  top5_medals;

CREATE TABLE 
  top5_medals 
USING DELTA
AS  
  (SELECT 
    country,
    games,  
    host,
    ranking,
    medals
  FROM  
    (SELECT
      region AS country,
      games,
      host_country AS host,
      ROW_NUMBER () OVER (PARTITION BY games ORDER BY COUNT(*) DESC) AS ranking,
      COUNT(*) AS medals
    FROM
      olympic_games
    JOIN
      hosts
    ON
      olympic_games.games = hosts.game   
    WHERE  
      medal IN ('Gold', 'Silver', 'Bronze') 
    GROUP BY
      country,
      games,
      host)
  WHERE
    ranking <= 5
  ORDER BY
    ranking);

SELECT
  *
FROM
  top5_medals;      

-- COMMAND ----------

-- No. of times that a host country was the one with most athletes competing (out of 25 host countries and 52 Olympic Games)

SELECT
  COUNT(*) AS host_with_most_athletes_count 
FROM
  top5_athletes
WHERE 
  country = host
AND 
  ranking = 1;

-- COMMAND ----------

-- No. of times that a host country was in the top 5 (athletes) - out of 25 host countries and 52 Olympic Games

SELECT
  COUNT(*) AS host_in_the_top5 
FROM
  top5_athletes
WHERE 
  country = host;

-- COMMAND ----------

-- No. of times that a host country was the one with most athletes competing, by country 

SELECT
  country,
  COUNT(*) AS host_with_most_athletes_count 
FROM
  top5_athletes
WHERE 
  ranking = 1
AND
  country = host
GROUP BY  
  country
ORDER BY
  host_with_most_athletes_count DESC;

-- COMMAND ----------

-- No. of times that a host country was in the top 5 of most athletes competing, by country

SELECT
  country,
  COUNT(*) AS in_top5 
FROM
  top5_athletes
WHERE 
  country = host
GROUP BY  
  country
ORDER BY
  in_top5 DESC;

-- COMMAND ----------

-- Comparing average number of athletes of a country when being the host vs when not being host

CREATE OR REPLACE VIEW  
  athletes_avg_comparison 
AS
SELECT
  host.region AS country,  
  athletes_avg_host,
  athletes_avg_no_host,
  ROUND((((athletes_avg_host - athletes_avg_no_host)/athletes_avg_no_host) * 100), 2) AS impact_of_hosting 
FROM  
 (SELECT
    region,
    ROUND(AVG(athletes),2) AS athletes_avg_host 
  FROM
    (SELECT
      region,
      games,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    JOIN
      hosts
    ON
      olympic_games.games = hosts.game
    WHERE 
      olympic_games.region = hosts.host_country  
    GROUP BY
      region,
      games)   
  GROUP BY  
    region
  ) AS host
LEFT JOIN   
  (SELECT
    region,
    ROUND(AVG(athletes),2) AS athletes_avg_no_host 
  FROM
    (SELECT
      region,
      games,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    JOIN
      hosts
    ON
      olympic_games.games = hosts.game
    WHERE 
      olympic_games.region <> hosts.host_country  
    GROUP BY
      region,
      games)   
  GROUP BY  
    region
  ) AS no_host 
 ON
   host.region = no_host.region;

SELECT
  *
FROM
  athletes_avg_comparison;     

-- COMMAND ----------

-- Looking at the stats of the impact

SELECT
  MIN(impact_of_hosting) AS min_impact,
  ROUND(AVG(impact_of_hosting),2) AS avg_impact,
  MAX(impact_of_hosting) AS max_impact
FROM  
  athletes_avg_comparison;

-- COMMAND ----------

-- No. of times that a host country was the winner of most medals (out of 25 host countries and 52 Olympic Games)

SELECT
  COUNT(*) AS host_winning_most_medals_count 
FROM
  top5_medals
WHERE 
  country = host
AND 
  ranking = 1;

-- COMMAND ----------

-- No. of times that a host country was in the top 5 of medal winners (out of 25 host countries and 52 Olympic Games)

SELECT
  COUNT(*) AS host_in_the_top5 
FROM
  top5_medals
WHERE 
  country = host;

-- COMMAND ----------

-- No. of times that a host country was the no. 1 medal winner, by country

SELECT
  country,
  COUNT(*) AS host_winning_most_medals_count 
FROM
  top5_medals
WHERE 
  ranking = 1
AND
  country = host
GROUP BY  
  country
ORDER BY
  host_winning_most_medals_count DESC;

-- COMMAND ----------

-- No. of times that a host country was in the top 5 medal winners, by country

SELECT
  country,
  COUNT(*) AS in_top5 
FROM
  top5_medals
WHERE 
  country = host
GROUP BY  
  country
ORDER BY
  in_top5 DESC;

-- COMMAND ----------

-- Comparing average number of medals of a country when being the host vs. when not being host

CREATE OR REPLACE VIEW  
  medals_avg_comparison 
AS
SELECT
  host.region AS country,  
  medals_avg_host,
  medals_avg_no_host,
  ROUND((((medals_avg_host - medals_avg_no_host)/medals_avg_no_host) * 100), 2) AS impact_of_hosting 
FROM  
 (SELECT
    region,
    ROUND(AVG(medals),2) AS medals_avg_host 
  FROM
    (SELECT
      region,
      games,
      COUNT(*) AS medals
    FROM
      olympic_games
    JOIN
      hosts
    ON
      olympic_games.games = hosts.game
    WHERE 
      medal IN ('Gold', 'Silver', 'Bronze')
    AND  
      olympic_games.region = hosts.host_country  
    GROUP BY
      region,
      games)   
  GROUP BY  
    region
  ) AS host
LEFT JOIN   
  (SELECT
    region,
    ROUND(AVG(medals),2) AS medals_avg_no_host 
  FROM
    (SELECT
      region,
      games,
      COUNT(*) AS medals
    FROM
      olympic_games
    JOIN
      hosts
    ON
      olympic_games.games = hosts.game
    WHERE 
      medal IN ('Gold', 'Silver', 'Bronze')
    AND 
      olympic_games.region <> hosts.host_country  
    GROUP BY
      region,
      games)   
  GROUP BY  
    region
  ) AS no_host 
 ON
   host.region = no_host.region;

SELECT 
  *
FROM
  medals_avg_comparison;     

-- COMMAND ----------

-- Looking at the stats of the impact

SELECT
  MIN(impact_of_hosting) AS min_impact,
  ROUND(AVG(impact_of_hosting),2) AS avg_impact,
  MAX(impact_of_hosting) AS max_impact
FROM  
  medals_avg_comparison;

-- COMMAND ----------

SELECT
  athletes_avg_comparison.country AS country,
  athletes_avg_comparison.impact_of_hosting AS athletes_impact,
  medals_avg_comparison.impact_of_hosting AS medals_impact 
FROM
  athletes_avg_comparison
JOIN
  medals_avg_comparison    
ON
  athletes_avg_comparison.country = medals_avg_comparison.country
ORDER BY
  athletes_impact DESC;


-- COMMAND ----------

-- Checking if there's a correlation between number of athletes and number of medals won by each country when hosting vs when not

SELECT
  athletes_avg_comparison.country,
  athletes_avg_host,
  medals_avg_host, 
  athletes_avg_no_host,
  medals_avg_no_host
FROM
  athletes_avg_comparison
JOIN
  medals_avg_comparison    
ON
  athletes_avg_comparison.country = medals_avg_comparison.country;

-- COMMAND ----------

-- Calculating Pearson coefficient to measure the correlation between athletes-medals when hosting vs when not 
-- The result indicates a strong positive correlation when a country was not the host and a weaker positive correlation when hosting

SELECT
  ROUND(CORR(athletes_avg_host, medals_avg_host), 2) AS correlation_host,
  ROUND(CORR(athletes_avg_no_host, medals_avg_no_host), 2) AS correlation_no_host
FROM
  athletes_avg_comparison
JOIN
  medals_avg_comparison    
ON
  athletes_avg_comparison.country = medals_avg_comparison.country;

-- COMMAND ----------

-- Creating metrics for AB testing stats: https://thumbtack.github.io/abba/demo/abba.html#No_host=773%2C2492&Host=1975%2C6845&abba%3AintervalConfidenceLevel=0.95&abba%3AuseMultipleTestCorrection=true
-- Control group or baseline (0): countries when not hosting the Olympics, treatment group or variation (1): countries when hosting the Olympics
-- Number of trials: athletes, number of successes: medals
-- The result indicates with 95.1% confidence that there was a 7% decrease of medals per athlete when the country was hosting the games

SELECT
  ROUND(SUM(athletes_avg_no_host),2) AS total_athletes_no_host,
  ROUND(SUM(medals_avg_no_host),2) AS total_medals_no_host,
  ROUND(SUM(athletes_avg_host),2) AS total_athletes_host,
  ROUND(SUM(medals_avg_host),2) AS total_medals_host
FROM
  athletes_avg_comparison
JOIN
  medals_avg_comparison    
ON
  athletes_avg_comparison.country = medals_avg_comparison.country;

-- COMMAND ----------

-- DBTITLE 1,Data exploration and analysis
-- MAGIC %md
-- MAGIC Relation between number of athletes and number of medals won by each country

-- COMMAND ----------

-- Checking if there's a correlation between number of athletes and number of medals won by each country (both host and non-host)

SELECT
    a.region AS country,
    ROUND(AVG(athletes),2) AS athletes_avg,
    ROUND(AVG(medals),2) AS medals_avg
  FROM
    (SELECT
      region,
      games,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    GROUP BY
      region,
      games
    ) AS a
  JOIN
    (SELECT
      region,
      games,
      COUNT(*) AS medals
    FROM
      olympic_games
    WHERE 
      medal IN ('Gold', 'Silver', 'Bronze')
    GROUP BY
      region,
      games
    ) AS m 
  ON
    a.region = m.region
  GROUP BY  
    a.region

-- COMMAND ----------

-- Calculating Pearson coefficient to measure the correlation: the result indicates a strong positive correlation

SELECT
  ROUND(CORR(athletes_avg, medals_avg), 2) AS correlation
FROM
  (SELECT
    a.region AS country,
    ROUND(AVG(athletes),2) AS athletes_avg,
    ROUND(AVG(medals),2) AS medals_avg
  FROM
    (SELECT
      region,
      games,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    GROUP BY
      region,
      games
    ) AS a
  JOIN
    (SELECT
      region,
      games,
      COUNT(*) AS medals
    FROM
      olympic_games
    WHERE 
      medal IN ('Gold', 'Silver', 'Bronze')
    GROUP BY
      region,
      games
    ) AS m 
  ON
    a.region = m.region
  GROUP BY  
    a.region
  )
 



-- COMMAND ----------

-- Checking if there's a correlation between number of athletes and number of medals, when excluding team events

SELECT
    a.region AS country,
    ROUND(AVG(athletes),2) AS athletes_avg,
    ROUND(AVG(medals),2) AS medals_avg
  FROM
    (SELECT
      region,
      games,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    WHERE
      LOWER(event) NOT LIKE '%team' 
    AND
      sport NOT IN ('Basketball', 'Bobsleigh', 'Curling', 'Football', 'Handball', 'Hockey', 'Ice Hockey', 'Rugby', 'Rugby Sevens', 'Water Polo')   
    GROUP BY
      region,
      games
    ) AS a
  JOIN
    (SELECT
      region,
      games,
      COUNT(*) AS medals
    FROM
      olympic_games
    WHERE 
      medal IN ('Gold', 'Silver', 'Bronze')
    AND
      LOWER(event) NOT LIKE '%team' 
    AND
      sport NOT IN ('Basketball', 'Bobsleigh', 'Cricket', 'Curling', 'Football', 'Handball', 'Hockey', 'Ice Hockey', 'Rugby', 'Rugby Sevens', 'Water Polo')   
    GROUP BY
      region,
      games
    ) AS m 
  ON
    a.region = m.region
  GROUP BY  
    a.region

-- COMMAND ----------

-- Calculating Pearson coefficient to measure the correlation: the result still indicates a strong positive correlation

SELECT
  ROUND(CORR(athletes_avg, medals_avg), 2) AS correlation
FROM
  (SELECT
    a.region AS country,
    ROUND(AVG(athletes),2) AS athletes_avg,
    ROUND(AVG(medals),2) AS medals_avg
  FROM
    (SELECT
      region,
      games,
      COUNT(DISTINCT id) AS athletes
    FROM
      olympic_games
    WHERE
      LOWER(event) NOT LIKE '%team' 
    AND
      sport NOT IN ('Basketball', 'Bobsleigh', 'Cricket', 'Curling', 'Football', 'Handball', 'Hockey', 'Ice Hockey', 'Rugby', 'Rugby Sevens', 'Water Polo')  
    GROUP BY
      region,
      games
    ) AS a
  JOIN
    (SELECT
      region,
      games,
      COUNT(*) AS medals
    FROM
      olympic_games
    WHERE 
      medal IN ('Gold', 'Silver', 'Bronze')
    AND
      LOWER(event) NOT LIKE '%team' 
    AND
      sport NOT IN ('Basketball', 'Bobsleigh', 'Curling', 'Football', 'Handball', 'Hockey', 'Ice Hockey', 'Rugby', 'Rugby Sevens', 'Water Polo')  
    GROUP BY
      region,
      games
    ) AS m 
  ON
    a.region = m.region
  GROUP BY  
    a.region
  )
 

-- COMMAND ----------

-- DBTITLE 1,Data exploration and analysis
-- MAGIC %md
-- MAGIC Athletes and medals by sport

-- COMMAND ----------

-- Athletes and medals average, as well as medal-athlete ratio by sport

SELECT 
  m.sport AS sport,
  ROUND(AVG(athletes),2) AS athletes_avg,
  ROUND(AVG(medals),2) AS medals_avg,
  ROUND((SUM(medals)/SUM(athletes)),2) AS medal_athlete_ratio
FROM    
  (SELECT 
    games,  
    sport,
    COUNT(id) AS medals
  FROM
    olympic_games
  WHERE  
    medal IN ('Gold', 'Silver', 'Bronze')      
  GROUP BY  
    games,  
    sport
  ) AS m
 JOIN
  (SELECT 
    games,  
    sport,
    COUNT(DISTINCT id) AS athletes
  FROM
    olympic_games      
  GROUP BY  
    games,  
    sport
  ) AS a 
 ON
  m.games = a.games 
GROUP BY
  m.sport
ORDER BY
  medal_athlete_ratio DESC;

-- COMMAND ----------

-- Excluding team sport events

SELECT 
  m.sport AS sport,
  ROUND(AVG(athletes),2) AS athletes_avg,
  ROUND(AVG(medals),2) AS medals_avg,
  ROUND((SUM(medals)/SUM(athletes)),2) AS medal_athlete_ratio
FROM    
  (SELECT 
    games,  
    sport,
    COUNT(id) AS medals
  FROM
    olympic_games
  WHERE  
    medal IN ('Gold', 'Silver', 'Bronze') 
  AND
    LOWER(event) NOT LIKE '%team' 
  AND
    sport NOT IN ('Basketball', 'Bobsleigh', 'Cricket', 'Curling', 'Football', 'Handball', 'Hockey', 'Ice Hockey', 'Rugby', 'Rugby Sevens', 'Water Polo')       
  GROUP BY  
    games,  
    sport
  ) AS m
 JOIN
  (SELECT 
    games,  
    sport,
    COUNT(DISTINCT id) AS athletes
  FROM
    olympic_games
  WHERE  
    LOWER(event) NOT LIKE '%team' 
  AND
    sport NOT IN ('Basketball', 'Bobsleigh', 'Curling', 'Football', 'Handball', 'Hockey', 'Ice Hockey', 'Rugby', 'Rugby Sevens', 'Water Polo')       
  GROUP BY  
    games,  
    sport
  ) AS a 
 ON
  m.games = a.games 
GROUP BY
  m.sport
ORDER BY
  medal_athlete_ratio DESC;
