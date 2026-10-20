# Databricks notebook source
# MAGIC %sql
# MAGIC WITH src as (
# MAGIC SELECT EXPLODE(SEQUENCE(DATE'1900-01-01', DATE'2100-12-31', INTERVAL 1 DAY)) as full_date
# MAGIC )
# MAGIC , agg as 
# MAGIC (
# MAGIC     SELECT
# MAGIC         CAST(date_format(src.full_date,'yyyyMMdd') AS INT ) as date_key,        
# MAGIC         src.full_date, 
# MAGIC         year(src.full_date) AS year,
# MAGIC         quarter(src.full_date) AS quarter,
# MAGIC         month(src.full_date) AS month,
# MAGIC         date_format(src.full_date,'MMMM') AS month_name
# MAGIC         FROM src
# MAGIC     )
# MAGIC
# MAGIC INSERT  OVERWRITE  pep_dev.gold.dim_date
# MAGIC (date_key,full_date,year,quarter,month,month_name) 
# MAGIC SELECT * FROM agg;
# MAGIC