# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC
# MAGIC WITH src as (
# MAGIC     SELECT 
# MAGIC         p.person_id,
# MAGIC         p.primary_name,
# MAGIC         p.birth_year_max,
# MAGIC         p.birth_year_min,
# MAGIC         p.death_date,
# MAGIC         p.is_pep,
# MAGIC         p.is_sanctioned,
# MAGIC         o.occupancy_id,
# MAGIC         o.end_date,
# MAGIC         o.status_norm,
# MAGIC         o._last_seen_snapshot,
# MAGIC         latest_snapshot.latest
# MAGIC
# MAGIC     FROM pep_dev.silver.person as p
# MAGIC     LEFT JOIN
# MAGIC     pep_dev.silver.occupancy as o
# MAGIC     ON p.person_id=o.person_id
# MAGIC     CROSS JOIN
# MAGIC     (   
# MAGIC         SELECT MAX (_last_seen_snapshot) AS latest
# MAGIC         FROM pep_dev.silver.occupancy
# MAGIC     ) AS latest_snapshot
# MAGIC     
# MAGIC     
# MAGIC )
# MAGIC , agg as (
# MAGIC     SELECT 
# MAGIC         src.person_id as person_id ,
# MAGIC         src.primary_name as primary_name,
# MAGIC         src.birth_year_min as birth_year_min,
# MAGIC         src.birth_year_max as birth_year_max,
# MAGIC         src.death_date as death_date,
# MAGIC         src.is_pep as is_pep,
# MAGIC         src.is_sanctioned as is_sanctioned,
# MAGIC         coalesce(bool_or(pep_dev.gold.tenure_verdict(src.death_date, src.end_date, src._last_seen_snapshot,src.status_norm, src.latest)='confirmed_current'),FALSE) AS is_current_pep,
# MAGIC         count(src.occupancy_id) as n_positions
# MAGIC         FROM src
# MAGIC         GROUP BY ALL
# MAGIC )
# MAGIC
# MAGIC
# MAGIC MERGE INTO pep_dev.gold.dim_person as tgt
# MAGIC
# MAGIC USING agg
# MAGIC ON tgt.person_id=agg.person_id
# MAGIC
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET 
# MAGIC     tgt.primary_name   = agg.primary_name,
# MAGIC     tgt.birth_year_min = agg.birth_year_min,
# MAGIC     tgt.birth_year_max = agg.birth_year_max,
# MAGIC     tgt.death_date     = agg.death_date,
# MAGIC     tgt.is_pep         = agg.is_pep,
# MAGIC     tgt.is_sanctioned  = agg.is_sanctioned,
# MAGIC     tgt.is_current_pep = agg.is_current_pep,
# MAGIC     tgt.n_positions    = agg.n_positions
# MAGIC     
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT 
# MAGIC (       person_id,
# MAGIC         primary_name,
# MAGIC         birth_year_min,
# MAGIC         birth_year_max,
# MAGIC         death_date,
# MAGIC         is_pep,
# MAGIC         is_sanctioned,
# MAGIC         is_current_pep,
# MAGIC         n_positions)
# MAGIC     VALUES
# MAGIC     (   agg.person_id,
# MAGIC         agg.primary_name,
# MAGIC         agg.birth_year_min,
# MAGIC         agg.birth_year_max,
# MAGIC         agg.death_date,
# MAGIC         agg.is_pep,
# MAGIC         agg.is_sanctioned,
# MAGIC         agg.is_current_pep,
# MAGIC         agg.n_positions
# MAGIC     )
# MAGIC     -- WHEN NOT MATCHED BY SOURCE THEN DELETE
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Unknown member (-1): fact rows whose person_id has no match in this dim get person_sk = -1.
# MAGIC -- This row gives them something to join to, so they show as 'Unknown' and don't silently drop out of joins.
# MAGIC -- NOT EXISTS makes it insert once only, so reruns are safe.
# MAGIC
# MAGIC INSERT INTO pep_dev.gold.dim_person
# MAGIC     (person_sk, person_id, primary_name, birth_year_min, birth_year_max,
# MAGIC      death_date, is_pep, is_sanctioned, is_current_pep, n_positions)
# MAGIC
# MAGIC SELECT -1, 'UNKNOWN', 'Unknown', NULL, NULL, NULL, FALSE, FALSE, FALSE, 0
# MAGIC
# MAGIC WHERE NOT EXISTS 
# MAGIC (SELECT 1 FROM pep_dev.gold.dim_person WHERE person_sk = -1);