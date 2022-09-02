-- Databricks notebook source
-- MAGIC %md #USE Command

-- COMMAND ----------

USE dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md #External Table

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.dsai_sales_analysis.DSAI_product_group_ext(
Product_Family_ID string NOT NULL,
Product_Group_ID string NOT NULL,
Product_Group_Name string NOT NULL
) USING DELTA
Location 's3://airline-data-bucket/external-storage/dsai_product_group';


-- COMMAND ----------

-- MAGIC %md #Copy INTO

-- COMMAND ----------

COPY INTO dsai_sales_analysis.dsai_product_group_ext
FROM '/FileStore/dsai_sales_analysis/Product_Group.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE');

-- COMMAND ----------

-- MAGIC %md #Describe command

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.dsai_sales_analysis.dsai_product_group_ext;

-- COMMAND ----------

-- MAGIC %md #Drop Extended Table

-- COMMAND ----------

DROP TABLE dsai_proudct_group_ext;

-- COMMAND ----------

-- MAGIC %md #External Table Data;

-- COMMAND ----------

SELECT * FROM delta.'s3://airline-data-bucket/external-storage/dsai_product_group/';

-- COMMAND ----------

-- MAGIC %md Grant Users, an Select previlage to the table sample_data
-- MAGIC GRANT SELECT ON TABLE dsai_fact TO USERS;

-- COMMAND ----------

-- MAGIC %md #Distinct

-- COMMAND ----------

SELECT DISTINCT(Product_Family_ID) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #where

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_fact WHERE Product_Family_ID = 'PF112';

-- COMMAND ----------

-- MAGIC %md #GroupBy

-- COMMAND ----------

SELECT Product_Family_ID,count(*) FROM hive_metastore.dsai_sales_analysis.dsai_fact GROUP BY Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md #Order By

-- COMMAND ----------

SELECT Product_Family_ID,count(*) FROM hive_metastore.dsai_sales_analysis.dsai_fact GROUP BY Product_Family_ID
ORDER BY Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md #STRING Functions

-- COMMAND ----------

--Concat
SELECT CONCAT_WS('-',Product_Family_ID, Product_ID) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

--Concat using ||
SELECT Product_Family_ID || Product_ID FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

--Format string
SELECT format_string('Product ID %s %s = %s', Product_ID,Unit_Price_Name ,Unit_Price) FROM hive_metastore.dsai_sales_analysis.dsai_unit_price;


-- COMMAND ----------

--SUBSTRING
SELECT Product_ID, Unit_Price, SUBSTRING(Unit_Price_Name, 1 ,3) FROM hive_metastore.dsai_sales_analysis.dsai_unit_price;

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE STARTSWITH(Sales_Country_Name, 'A') = true;


-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE ENDSWITH(Sales_Country_Name, 'a') = true;

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE CONTAINS(Sales_Country_Name, 'Ba') = true;

-- COMMAND ----------

-- MAGIC %md #Date Functions

-- COMMAND ----------

SELECT current_date();

-- COMMAND ----------

SELECT now();

-- COMMAND ----------

SELECT current_timestamp();

-- COMMAND ----------

SELECT current_timezone();

-- COMMAND ----------

SELECT DATE_ADD(Sales_Date,1) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

SELECT Sales_Date,date_sub(Sales_Date,3) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

 SELECT CAST(Sales_Date AS STRING) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

SELECT date(Sales_Date),add_months(date(Sales_Date),2) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

SELECT date(Sales_Date),datediff(now(),date(Sales_Date)) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

SELECT date(Sales_Date),datediff('2022-08-02',date(Sales_Date)) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

SELECT date(Sales_Date),now(),months_between(now(),date(Sales_Date)) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

-- MAGIC %md #INSERT INTO

-- COMMAND ----------

INSERT INTO hive_metastore.dsai_sales_analysis.dsai_product_family values ('P115','Garments');

-- COMMAND ----------

-- MAGIC %md #Column Expression

-- COMMAND ----------

SHOW COLUMNS From hive_metastore.dsai_sales_analysis.dsai_sales_country

-- COMMAND ----------

-- MAGIC %md #Compute Columns

-- COMMAND ----------

ALTER TABLE hive_metastore.dsai_sales_analysis.dsai_fact ADD Product_Total AS (integer(Quantity_Sold) * integer(Unit_Price_ID));


-- COMMAND ----------

-- MAGIC %md #Insert Overwrite

-- COMMAND ----------

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    STORED AS orc
    SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family;

-- COMMAND ----------

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT * FROM test_table;

-- COMMAND ----------

-- MAGIC %md #Case When

-- COMMAND ----------

SELECT Customer_ID,Quantity_Sold,CASE WHEN int(Quantity_Sold)  >= 3 THEN 'High Potential' WHEN Quantity_Sold  < 3 and Quantity_Sold >=2 THEN 'Low Potential' ELSE 'Normal' END
from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #INNER JOINS

-- COMMAND ----------

select pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
join hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
group by pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md #LEFT JOIN

-- COMMAND ----------

SELECT pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
LEFT JOIN hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
GROUP BY pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md #SubQuery

-- COMMAND ----------

SELECT * 
   FROM hive_metastore.dsai_sales_analysis.dsai_fact
   WHERE Customer_ID IN (SELECT Customer_ID 
         FROM hive_metastore.dsai_sales_analysis.dsai_fact
         WHERE Revenue > 20)

-- COMMAND ----------

-- MAGIC %md #Grant

-- COMMAND ----------

GRANT SELECT ON TABLE hive_metastore.dsai_sales_analysis.dsai_fact TO user1;

-- COMMAND ----------

-- MAGIC %md #Revoke

-- COMMAND ----------

REVOKE SELECT ON TABLE hive_metastore.dsai_sales_analysis.dsai_fact FROM user1;

-- COMMAND ----------

-- MAGIC %md #SHOW GRANT 

-- COMMAND ----------

SHOW GRANTS `user1` ON SCHEMA hive_metastore.dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md #VERSION AS OF

-- COMMAND ----------

-- Describe history hive_metastore.dsai_sales_analysis.dsai_fact;
SELECT * FROM delta.`dbfs:/user/hive/warehouse/dsai_sales_analysis.db/dsai_fact` VERSION AS OF 8;

-- COMMAND ----------

-- MAGIC %md #TIMESTAMP AS OF

-- COMMAND ----------

--Describe history hive_metastore.dsai_sales_analysis.dsai_fact;
SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_fact TIMESTAMP AS OF '2022-08-26T12:01:03.000'

-- COMMAND ----------

-- MAGIC %md #UNION

-- COMMAND ----------

SELECT * from hive_metastore.dsai_sales_analysis.dsai_product_family 
UNION SELECT * from hive_metastore.dsai_sales_analysis.dsai_product_family_ext;

-- COMMAND ----------

-- MAGIC %md #UNION ALL

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family 
UNION ALL select * from hive_metastore.dsai_sales_analysis.dsai_product_family_ext;

-- COMMAND ----------

-- MAGIC %md #UNION DISTINCT

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family 
UNION DISTINCT SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family_ext;

-- COMMAND ----------

-- MAGIC %md #SELECT AS

-- COMMAND ----------

SELECT Product_Family_ID,vProduct_Family_Name, concat_ws('-',Product_Family_ID,Product_Family_Name) as Product_FName FROM hive_metastore.dsai_sales_analysis.dsai_product_family

-- COMMAND ----------

-- MAGIC %md #Create View & Select from View

-- COMMAND ----------

CREATE OR REPLACE VIEW hive_metastore.dsai_sales_analysis.consolidated_sales as (
SELECT YEAR(Sales_Date) as syear,MONTH(Sales_Date) as smonth,Customer_ID,sum(Revenue) rev FROM hive_metastore.dsai_sales_analysis.dsai_fact 
GROUP BY syear,smonth,Customer_ID);

SELECT * FROM hive_metastore.dsai_sales_analysis.consolidated_sales where rev>=300 ;

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val product_groups= spark.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("/FileStore/dsai_sales_analysis/Product_Group.csv")
-- MAGIC 
-- MAGIC display(product_groups);

-- COMMAND ----------


