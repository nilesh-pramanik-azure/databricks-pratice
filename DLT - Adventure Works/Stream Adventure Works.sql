-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE bronze_customer
COMMENT 'This is the bronze customer table'
AS SELECT * FROM cloud_files("${FolderPath}/*Customer*.csv", "csv",
map("schema","CustomerID integer, FirstName string, LastName string, FullName string"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_employee
COMMENT 'This is the bronze employee table'
AS SELECT * FROM cloud_files("${FolderPath}/*Employee*.csv", "csv",
map("schema","EmployeeID integer, ManagerID string, FirstName string, LastName string, FullName string, JobTitle string,OrganizationLevel integer, MaritalStatus string, Gender string, Territory string, Country string, Group string"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders
COMMENT 'This is the bronze order table'
AS SELECT * FROM cloud_files("${FolderPath}/*Orders*.csv", "csv",
map("schema","SalesOrderID integer, SalesOrderDetailID integer, OrderDate date, DueDate date, ShipDate string, EmployeeID integer, CustomerID integer,SubTotal double, TaxAmt double, Freight double, TotalDue double, ProductID integer, OrderQty integer, UnitPrice double, UnitPriceDiscount double, LineTotal double"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_products
COMMENT 'This is the bronze product table'
AS SELECT * FROM cloud_files("${FolderPath}/*Products*.csv", "csv",
map("schema","ProductID integer, ProductNumber string, ProductName string, ModelName string, MakeFlag integer, StandardCost double, ListPrice double, SubCategoryID integer"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_product_categories
COMMENT 'This is the bronze product categories table'
AS SELECT * FROM cloud_files("${FolderPath}/*ProductCategori*.csv", "csv",
map("schema","CategoryID integer, CategoryName string"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE gold_quarter_details
COMMENT 'This table hold the summation of total amount and txn amount for each quarter for each customer'
AS
SELECT C.FULLNAME as CUSTOMERNAME, E.FULLNAME AS EMPLOYEENAME, P.PRODUCTNAME, P.MODELNAME, 
CASE
  WHEN to_char(O.ORDERDATE,'MM') IN ('01','02','03') THEN '01'
  WHEN to_char(O.ORDERDATE,'MM') IN ('04','05','06') THEN '02'
  WHEN to_char(O.ORDERDATE,'MM') IN ('07','08','09') THEN '03'
  ELSE '04'
END AS QUARTER--,
--SUM(O.SUBTOTAL) AS TOT_SUBTOTAL,
--SUM(O.TAXAMT) AS TOT_TAXAMT
FROM
stream(live.bronze_orders) AS O
INNER JOIN
stream(live.bronze_customer) AS C
ON O.CUSTOMERID = C.CUSTOMERID
INNER JOIN
stream(live.bronze_employee) AS E
ON O.EMPLOYEEID = E.EMPLOYEEID
INNER JOIN
stream(live.bronze_products) AS P
ON O.PRODUCTID = P.PRODUCTID
--GROUP BY 
--C.FULLNAME, E.FULLNAME, P.PRODUCTNAME, P.MODELNAME, 
--CASE
  --WHEN to_char(O.ORDERDATE,'MM') IN ('01','02','03') THEN '01'
  --WHEN to_char(O.ORDERDATE,'MM') IN ('04','05','06') THEN '02'
  --WHEN to_char(O.ORDERDATE,'MM') IN ('07','08','09') THEN '03'
  --ELSE '04'
--END
