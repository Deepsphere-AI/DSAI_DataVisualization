-- Databricks notebook source
-- MAGIC %md #USE Command

-- COMMAND ----------

USE dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md #CREATE TABLE USING CSV
-- MAGIC 
-- MAGIC Crete table from CSV File loaded in DFS.

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val df = spark.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .option("sep", ",")
-- MAGIC   .load("/FileStore/dsai_sales_analysis/Product_Group.csv")
-- MAGIC 
-- MAGIC display(df)
-- MAGIC val permanent_table_name = "Product_Group_FROM_CSV"
-- MAGIC df.write.format("parquet").saveAsTable(permanent_table_name)

-- COMMAND ----------

select * from Product_Group_FROM_CSV;

-- COMMAND ----------

-- MAGIC %md #USING DELTA
-- MAGIC The data management tool that combines the scale of a data lake, the reliability and performance of a data warehouse and the low latency of streaming in a single system for the first time is called Databricks Delta.

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.dsai_sales_analysis.DSAI_product_group(
Product_Family_ID string NOT NULL,
Product_Group_ID string NOT NULL,
Product_Group_Name string NOT NULL
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md #External Table
-- MAGIC 
-- MAGIC An external table references an external storage path using a **LOCATION** clause.
-- MAGIC This storage path should be in an existing external location to which the access has been granted access.
-- MAGIC Alternatively, refer to a storage credential to which the access has been granted.
-- MAGIC Using external tables abstracts away the storage path, external location, and storage credentials for users who have access to the external table.
-- MAGIC 
-- MAGIC ##Location
-- MAGIC The path must be a STRING literal. If there is no specific location, the table is managed and databricks create a default table location. Specifying a location makes the table an external table, as specifying a location makes the table an external table.

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.dsai_sales_analysis.DSAI_product_group_ext(
Product_Family_ID string NOT NULL,
Product_Group_ID string NOT NULL,
Product_Group_Name string NOT NULL
) USING DELTA
Location 's3://airline-data-bucket/external-storage/dsai_product_group';


-- COMMAND ----------

-- MAGIC %md #Copy INTO
-- MAGIC It loads data from a file location into a Delta table that is a retriable and idempotent operation—files in the source location that have already been loaded are skipped
-- MAGIC 
-- MAGIC ##FILE
-- MAGIC Databricks File System or DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. The DBFS - is an abstraction on top of scalable object storage that provides an optimised FUSE (Filesystem in Userspace) interface that maps to native cloud storage API calls.
-- MAGIC 
-- MAGIC ##FILEFORMAT
-- MAGIC Apache Parquet is a columnar file format providing optimisations to speed up queries which are far more efficient file formats than CSV or JSON.

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
-- MAGIC Returns the basic metadata of the table. It includes column name, column type, column comment and storage location.

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.dsai_sales_analysis.dsai_product_group_ext;

-- COMMAND ----------

-- MAGIC %md #Drop Extended Table
-- MAGIC 
-- MAGIC Suppose the table is not an EXTERNAL table. In that case, it deletes the table and removes the associated directory with the table from the file system. An oddity is thrown if the table does not exist. In the case of an external table, only the associated metadata information is removed from the metastore schema.

-- COMMAND ----------

DROP TABLE dsai_proudct_group_ext;

-- COMMAND ----------

-- MAGIC %md #External Table Data
-- MAGIC 
-- MAGIC Dropping EXTERNAL TABLES will not delete data. Instead, use TRUNCATE TABLE to get rid of data

-- COMMAND ----------

SELECT * FROM delta.`s3://airline-data-bucket/external-storage/dsai_product_group`;

-- COMMAND ----------

SELECT count(*) FROM delta.`s3://airline-data-bucket/external-storage/dsai_product_group`;

-- COMMAND ----------

-- MAGIC %md #Grant 
-- MAGIC Grants a privilege on a securable object to a principal. Modifying access to the samples Catalogue is not supported. This Catalogue is available to all workspaces but is read-only. Use GRANT ON SHARE to grant recipients access to shares
-- MAGIC ## Privilege
-- MAGIC The table access control is enabled on the workspace. All clusters and SQL objects in Azure Databricks are hierarchical. Privileges are inherited downward, which means granting or denying a privilege on the Catalogue automatically grants or denies the privilege to all schemas in the Catalogue.

-- COMMAND ----------

GRANT SELECT ON TABLE hive_metastore.dsai_sales_analysis.dsai_fact TO user1;

-- COMMAND ----------

-- MAGIC %md #SHOW GRANT
-- MAGIC 
-- MAGIC Displays all inherited, denied and granted privileges that affect the securable object. 
-- MAGIC To run this command, use either:
-- MAGIC A Workspace administrator or the owner of the object.
-- MAGIC User specified in principal.
-- MAGIC 
-- MAGIC Use SHOW GRANTS TO RECIPIENT to list the shares a recipient has access to.

-- COMMAND ----------

SHOW GRANTS `user1` ON SCHEMA hive_metastore.dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md #REVOKE
-- MAGIC Revokes explicitly granted or denied privilege on a securable object from a principal. Modifying access to the samples Catalogue is not supported, which is available to all workspaces but is read-only.

-- COMMAND ----------

REVOKE SELECT ON TABLE hive_metastore.dsai_sales_analysis.dsai_fact FROM user1;

-- COMMAND ----------

-- MAGIC %md #Distinct
-- MAGIC 
-- MAGIC It tests if the arguments have different values where NULLs are considered comparable.

-- COMMAND ----------

SELECT DISTINCT(Product_Family_ID) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #Where
-- MAGIC It limits the results of the FROM clause of a query or a subquery based on the specified condition.

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_fact WHERE Product_Family_ID = 'PF112';

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md #GroupBy
-- MAGIC 
-- MAGIC It is used to group the rows based on a set of specified grouping expressions and calculate aggregations on the group of rows based on one or multiple specified aggregate functions. Also, the Databricks Runtime supports advanced aggregations to do multiple aggregations for the same input record set via GROUPING SETS, CUBE, and ROLLUP clauses. These grouping expressions and advanced aggregations can be mixed in the GROUP BY clause and nested in a GROUPING SETS clause.

-- COMMAND ----------

SELECT Product_Family_ID,count(*) FROM hive_metastore.dsai_sales_analysis.dsai_fact GROUP BY Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md #Order By
-- MAGIC The user-specified order where returns the result rows in a sorted manner. Contrary to the SORT BY clause, this clause guarantees total order in the output.

-- COMMAND ----------

SELECT Product_Family_ID,count(*) FROM hive_metastore.dsai_sales_analysis.dsai_fact GROUP BY Product_Family_ID
ORDER BY Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md #STRING FUNCTIONS

-- COMMAND ----------

-- MAGIC %md ##STRING FUNCTION

-- COMMAND ----------

select string(Sales_date) from hive_metastore.dsai_sales_analysis.dsai_sales_date;

-- COMMAND ----------

-- MAGIC %md ##CONCAT_WS FUNCTION

-- COMMAND ----------

--Concat
SELECT CONCAT_WS('-',Product_Family_ID, Product_ID) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md ##CONCAT using ||

-- COMMAND ----------


SELECT Product_Family_ID || Product_ID FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md ##FORMAT_STRING

-- COMMAND ----------

SELECT format_string('Product ID %s %s = %s', Product_ID,Unit_Price_Name ,Unit_Price) FROM hive_metastore.dsai_sales_analysis.dsai_unit_price;


-- COMMAND ----------

-- MAGIC %md ##SUBSTRING

-- COMMAND ----------

SELECT Product_ID, Unit_Price, SUBSTRING(Unit_Price_Name, 1 ,3) FROM hive_metastore.dsai_sales_analysis.dsai_unit_price;

-- COMMAND ----------

-- MAGIC %md ##STARTSWITH

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE STARTSWITH(Sales_Country_Name, 'A') = true;


-- COMMAND ----------

-- MAGIC %md ##ENDSWITH

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE ENDSWITH(Sales_Country_Name, 'a') = true;

-- COMMAND ----------

-- MAGIC %md ##CONTAINS

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE CONTAINS(Sales_Country_Name, 'Ba') = true;

-- COMMAND ----------

-- MAGIC %md ##INITCAP

-- COMMAND ----------

select initcap(Customer_Full_Name) from hive_metastore.dsai_sales_analysis.dsai_customer;

-- COMMAND ----------

-- MAGIC %md #Date FUNCTIONS

-- COMMAND ----------

-- MAGIC %md ##Date FUNCTION
-- MAGIC The expression to cast DATE and the value expr to DATE.

-- COMMAND ----------

SELECT DATE(CREATED_DT) FROM dsai_sales_analysis.dsai_sales_date

-- COMMAND ----------

-- MAGIC %md ##CURRENT_DATE FUNCTION

-- COMMAND ----------

SELECT current_date();

-- COMMAND ----------

-- MAGIC %md ##NOW FUNCTION
-- MAGIC Return current date and time

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
-- MAGIC 
-- MAGIC This expression Inserts new rows into a table and optionally truncates the table or partitions. Specifying the inserted rows by value expressions or the result of a query where the Databricks SQL supports this statement only for Delta Lake tables.

-- COMMAND ----------

INSERT INTO hive_metastore.dsai_sales_analysis.dsai_product_family values ('P115','Garments');

-- COMMAND ----------

-- MAGIC %md #List Columns

-- COMMAND ----------

SHOW COLUMNS From hive_metastore.dsai_sales_analysis.dsai_sales_country

-- COMMAND ----------

-- MAGIC %md #INSERT OVERWRITE
-- MAGIC Overwrites the existing data in the directory using a given Spark file format with the new values. It specifies the inserted row by value expressions or the query result.

-- COMMAND ----------

INSERT OVERWRITE LOCAL DIRECTORY '/FileStore/dsai_sales_analysis/'
    STORED AS orc
    SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family;

-- COMMAND ----------

-- MAGIC %md #Case When
-- MAGIC 
-- MAGIC It returns resN for the first optN that equals expr or def if none matches and condN evaluating to true, or def if none is found.

-- COMMAND ----------

SELECT Customer_ID,Quantity_Sold,CASE WHEN int(Quantity_Sold)  >= 3 THEN 'High Potential' WHEN Quantity_Sold  < 3 and Quantity_Sold >=2 THEN 'Low Potential' ELSE 'Normal' END
from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #JOINS
-- MAGIC Based on join criteria, it combines rows from two relations.

-- COMMAND ----------

-- MAGIC %md ##INNER JOINS
-- MAGIC Inner Joins are also referred to as Full Outer, which returns all values from both appending NULL values,  relations on the side that does not have a match.

-- COMMAND ----------

select pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
join hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
group by pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md ##LEFT JOIN

-- COMMAND ----------

SELECT pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
LEFT JOIN hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
GROUP BY pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md ##Cross Join
-- MAGIC This type of join returns the Cartesian product of rows from the tables in the join. It combines each row from the first table with each from the second.

-- COMMAND ----------

select * from hive_metastore.dsai_sales_analysis.dsai_sales_date c
cross join hive_metastore.dsai_sales_analysis.dsai_sales_region r

-- COMMAND ----------

-- MAGIC %md #SubQuery
-- MAGIC The subquery is also referred to as sub- SELECT s or nested SELECT s. The full SELECT syntax is valid in subqueries and appears inside another query statement.

-- COMMAND ----------

SELECT * 
   FROM hive_metastore.dsai_sales_analysis.dsai_fact
   WHERE Customer_ID IN (SELECT Customer_ID 
         FROM hive_metastore.dsai_sales_analysis.dsai_fact
         WHERE Revenue > 20)

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
-- MAGIC The API for DataFrame.unionAll() with two different DataFrames with different numbers of columns and names unionAll() does not work. Instead, it returns an array of the elements in the union of array1 and array2 without duplicates

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
-- MAGIC 
-- MAGIC This SELECT AS composes a result set from one or more tables. It can be part of a query that includes common table expressions (CTE), set operations and other clauses.

-- COMMAND ----------

SELECT Product_Family_ID,Product_Family_Name, concat_ws('-',Product_Family_ID,Product_Family_Name) as Product_FName FROM hive_metastore.dsai_sales_analysis.dsai_product_family as f 

-- COMMAND ----------

-- MAGIC %md #Views
-- MAGIC Based on the SQL query result-set, it constructs a virtual table with no physical data where ALTER VIEW and DROP VIEW only change metadata.

-- COMMAND ----------

CREATE OR REPLACE VIEW hive_metastore.dsai_sales_analysis.consolidated_sales as (
SELECT YEAR(Sales_Date) as syear,MONTH(Sales_Date) as smonth,Customer_ID,sum(Revenue) rev FROM hive_metastore.dsai_sales_analysis.dsai_fact 
GROUP BY syear,smonth,Customer_ID);

SELECT * FROM hive_metastore.dsai_sales_analysis.consolidated_sales where rev>=300 ;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md #MERGE
-- MAGIC IF THERE IS A MATCH
-- MAGIC The MATCHED clauses are executed when a source row matches a target table row based on the merge_condition and the optional match_condition.
-- MAGIC IF THERE ARE NO MATCHES
-- MAGIC The MERGE operation can fail if multiple rows of the source dataset match, where an attempt to update the same rows of the target Delta table.

-- COMMAND ----------

MERGE INTO hive_metastore.dsai_sales_analysis.dsai_product_family T
USING hive_metastore.dsai_sales_analysis.dsai_product_family_source S
ON (S.Product_Family_ID = T.Product_Family_ID)
WHEN MATCHED 
     THEN UPDATE 
     SET T.Product_Family_Name = S.Product_Family_Name            
WHEN NOT MATCHED 
     THEN INSERT (Product_Family_ID, Product_Family_Name, created_user, Created_DT, Updated_User, Updated_DT)
     VALUES (S.Product_Family_ID, S.Product_Family_Name, 'DSAI_user',now(),'DSAI_User',now())


-- COMMAND ----------

-- MAGIC %md #Aggrergate

-- COMMAND ----------

Select count(distinct(Customer_ID)) from dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

select sum(Revenue) from dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

select max(Revenue) from dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

select min(Revenue) from dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

select avg(Revenue) from dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #UPDATE
-- MAGIC When no predicate is provided, it updates the column values for all the rows. If any predicate is provided, it updates the column values for rows that match a predicate.

-- COMMAND ----------

update dsai_fact set Quantity_Sold = 2 where sales_id=1;

-- COMMAND ----------

update dsai_fact set Quantity_Sold = 3,Revenue=200 where sales_id=1;

-- COMMAND ----------

-- MAGIC %md #Column Expression
-- MAGIC 
-- MAGIC Using functions or operators, this formula computes a result based on literal references to columns, fields, or variables.

-- COMMAND ----------

SELECT Sales_Date,Customer_ID,Product_Family_ID,Product_ID,Revenue,Cost_Of_Goods_Sold,Labor_Cost,
Operating_Cost,(Revenue-(Cost_Of_Goods_Sold+Labor_Cost+Operating_Cost)) as Profit 
FROM hive_metastore.dsai_sales_analysis.dsai_fact 
