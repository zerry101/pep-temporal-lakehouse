# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC WITH src AS
# MAGIC (
# MAGIC     SELECT
# MAGIC            post_id,
# MAGIC            post_name,
# MAGIC            pep_dev.gold.country_fetch(countries) AS country
# MAGIC            FROM pep_dev.silver.post
# MAGIC )
# MAGIC
# MAGIC MERGE INTO pep_dev.gold.dim_post as tgt
# MAGIC
# MAGIC USING src
# MAGIC ON tgt.post_id=src.post_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET 
# MAGIC     tgt.post_name = src.post_name,
# MAGIC     tgt.country = src.country
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT 
# MAGIC (       post_id,
# MAGIC         post_name,
# MAGIC         country
# MAGIC )
# MAGIC     VALUES
# MAGIC     (   src.post_id,
# MAGIC         src.post_name,
# MAGIC         src.country
# MAGIC     )
# MAGIC     -- WHEN NOT MATCHED BY SOURCE THEN DELETE
# MAGIC ;
# MAGIC     
# MAGIC
# MAGIC -- SELECT  * FROM pep_dev.gold.dim_post;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unknown member (-1): fact rows whose person_id has no match in this dim get post_sk = -1.
# MAGIC -- This row gives them something to join to, so they show as 'Unknown' and don't silently drop out of joins.
# MAGIC -- NOT EXISTS makes it insert once only, so reruns are safe.
# MAGIC
# MAGIC
# MAGIC INSERT INTO pep_dev.gold.dim_post (post_sk, post_id, post_name, country)
# MAGIC
# MAGIC SELECT -1, 'UNKNOWN', 'Unknown', NULL
# MAGIC
# MAGIC WHERE NOT EXISTS
# MAGIC (SELECT 1 FROM pep_dev.gold.dim_post WHERE post_sk = -1);