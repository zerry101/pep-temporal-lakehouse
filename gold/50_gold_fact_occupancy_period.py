# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC     WITH src AS 
# MAGIC     (
# MAGIC         SELECT so.occupancy_id as occupancy_id,
# MAGIC             coalesce(gdpe.person_sk, -1) AS person_sk,
# MAGIC             coalesce(gdpo.post_sk, -1) AS post_sk,   
# MAGIC             so.status_norm as status,
# MAGIC             so.start_raw as start_date_raw,
# MAGIC             so.end_raw as end_date_raw,
# MAGIC             CASE 
# MAGIC                 WHEN so.start_date 
# MAGIC                     BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' 
# MAGIC                     THEN
# MAGIC                         CAST(date_format(so.start_date,'yyyyMMdd') AS INT) 
# MAGIC                     END 
# MAGIC             AS start_date_key,
# MAGIC
# MAGIC             CASE 
# MAGIC             WHEN so.end_date 
# MAGIC                 BETWEEN DATE'1900-01-01' AND DATE'2100-12-31' 
# MAGIC                 THEN
# MAGIC                     CAST(date_format(so.end_date,'yyyyMMdd') AS INT) 
# MAGIC                 END 
# MAGIC                 AS end_date_key,
# MAGIC                 pep_dev.gold.tenure_verdict(
# MAGIC                 gdpe.death_date,
# MAGIC                 so.end_date,
# MAGIC                 so._last_seen_snapshot,
# MAGIC                 so.status_norm,
# MAGIC                 l.latest
# MAGIC                 ) as tenure_verdict,
# MAGIC                 so.source_count as source_count,
# MAGIC                 so._first_seen_snapshot AS _first_seen_snapshot,
# MAGIC                 so._last_seen_snapshot AS
# MAGIC                 _last_seen_snapshot
# MAGIC
# MAGIC         FROM 
# MAGIC         pep_dev.silver.occupancy as so
# MAGIC         LEFT JOIN pep_dev.gold.dim_person as gdpe
# MAGIC         ON so.person_id=gdpe.person_id
# MAGIC         LEFT JOIN pep_dev.gold.dim_post as gdpo
# MAGIC         ON so.post_id=gdpo.post_id
# MAGIC         CROSS JOIN (
# MAGIC             SELECT MAX(_last_seen_snapshot) AS latest
# MAGIC         FROM pep_dev.silver.occupancy ) as l 
# MAGIC     
# MAGIC     )   
# MAGIC
# MAGIC MERGE INTO pep_dev.gold.fact_occupancy_period AS tgt
# MAGIC USING src
# MAGIC ON tgt.occupancy_id = src.occupancy_id
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     tgt.person_sk = src.person_sk,
# MAGIC     tgt.post_sk = src.post_sk,
# MAGIC     tgt.start_date_key = src.start_date_key,
# MAGIC     tgt.start_date_raw = src.start_date_raw,
# MAGIC     tgt.end_date_key = src.end_date_key,
# MAGIC     tgt.end_date_raw = src.end_date_raw,
# MAGIC     tgt.status = src.status,
# MAGIC     tgt.tenure_verdict = src.tenure_verdict,
# MAGIC     tgt.source_count = src.source_count,
# MAGIC     tgt._first_seen_snapshot = src._first_seen_snapshot,
# MAGIC     tgt._last_seen_snapshot = src._last_seen_snapshot
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC     (occupancy_id, person_sk, post_sk, start_date_key, start_date_raw,
# MAGIC      end_date_key, end_date_raw, status, tenure_verdict, source_count,
# MAGIC      _first_seen_snapshot, _last_seen_snapshot)
# MAGIC VALUES
# MAGIC     (src.occupancy_id, src.person_sk, src.post_sk, src.start_date_key, src.start_date_raw,
# MAGIC      src.end_date_key, src.end_date_raw, src.status, src.tenure_verdict, src.source_count,
# MAGIC      src._first_seen_snapshot, src._last_seen_snapshot)
# MAGIC
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;