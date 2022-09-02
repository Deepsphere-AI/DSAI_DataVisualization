-- Databricks notebook source
-- MAGIC %md #Use Command

-- COMMAND ----------

use dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md #External Table

-- COMMAND ----------

-- Syntax:
-- CREATE TABLE <catalog>.<schema>.<table_name>
-- (
--   <column_specification>
-- )
-- LOCATION 's3://<bucket_path>/<table_directory>';
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

describe extended hive_metastore.dsai_sales_analysis.dsai_product_group_ext;

-- COMMAND ----------

-- MAGIC %md #Drop Extended Table

-- COMMAND ----------

drop table dsai_proudct_group_ext;

-- COMMAND ----------

-- MAGIC %md #External Table Data;

-- COMMAND ----------

select * from delta.'s3://airline-data-bucket/external-storage/dsai_product_group/';

-- COMMAND ----------

-- MAGIC %md Grant Users, an Select previlage to the table sample_data
-- MAGIC GRANT SELECT ON TABLE dsai_fact TO USERS;

-- COMMAND ----------

-- MAGIC %md #Distinct

-- COMMAND ----------

select distinct(Product_Family_ID) from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #where

-- COMMAND ----------

select * from hive_metastore.dsai_sales_analysis.dsai_fact where Product_Family_ID = 'PF112';

-- COMMAND ----------

-- MAGIC %md #GroupBy

-- COMMAND ----------

select Product_Family_ID,count(*) from hive_metastore.dsai_sales_analysis.dsai_fact group by Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md #Order By

-- COMMAND ----------

select Product_Family_ID,count(*) from hive_metastore.dsai_sales_analysis.dsai_fact group by Product_Family_ID
order by Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md #String Function

-- COMMAND ----------

--Concat
SELECT concat_ws('-',Product_Family_ID, Product_ID) from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

SELECT Product_Family_ID || Product_ID from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------



-- COMMAND ----------

SELECT format_string('Product ID %s %s = %s', Product_ID,Unit_Price_Name ,Unit_Price) from hive_metastore.dsai_sales_analysis.dsai_unit_price;


-- COMMAND ----------

SELECT Product_ID, Unit_Price,substring(Unit_Price_Name,1,3) from hive_metastore.dsai_sales_analysis.dsai_unit_price;

-- COMMAND ----------

select Sales_Country_Name from hive_metastore.dsai_sales_analysis.dsai_sales_country
where startswith(Sales_Country_Name, 'A') = true;


-- COMMAND ----------

select Sales_Country_Name from hive_metastore.dsai_sales_analysis.dsai_sales_country
where endswith(Sales_Country_Name, 'a') = true;

-- COMMAND ----------

select Sales_Country_Name from hive_metastore.dsai_sales_analysis.dsai_sales_country
where contains(Sales_Country_Name, 'Ba') = true;

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

add_months(startDate, numMonths)

-- COMMAND ----------

SELECT date_add(Sales_Date,1) from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

SELECT Sales_Date,date_sub(Sales_Date,3) from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

 SELECT CAST(Sales_Date AS STRING) from hive_metastore.dsai_sales_analysis.dsai_fact;

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

-- MAGIC %md #Left Join

-- COMMAND ----------

select pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
left join hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
group by pf.Product_Family_ID,pf.Product_Family_Name;

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
UNION DISTINCT select * from hive_metastore.dsai_sales_analysis.dsai_product_family_ext;
