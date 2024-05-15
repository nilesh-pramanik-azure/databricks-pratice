-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE customer_raw
COMMENT 'This is the raw version of table'
AS
SELECT * FROM cloud_files("${folderpath}/Customer", "JSON", 
    map("schema","CustId integer, name struct<forename: string, surname: string>, dob string, nationality string, bookCount integer")
  )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customer_silver
AS
SELECT CustId as customer_id, concat(name.forename, ' ', name.surname) as fullname, to_date(dob, 'yyyy-MM-dd') as birth_date, nationality, bookCount as book_count
from stream(live.customer_raw)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customer_gold
comment 'this is the gold version of table'
as
select customer_id, fullname, birth_date, nationality, sum(book_count) as total_book_count
from
stream(live.customer_silver)
group by customer_id, fullname, birth_date, nationality
