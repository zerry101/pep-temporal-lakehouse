# Databricks notebook source
# MAGIC %sql
# MAGIC WITH src AS (
# MAGIC     SELECT * FROM pep_dev.bronze.statements
# MAGIC     WHERE _snapshot_version=(SELECT MAX(_snapshot_version) FROM pep_dev.bronze.statements)
# MAGIC     AND 
# MAGIC     schema='Position'
# MAGIC ),
# MAGIC agg as (
# MAGIC     SELECT canonical_id as post_id,
# MAGIC     collect_set(CASE WHEN prop='name' THEN VALUE END) as names,
# MAGIC     mode(CASE WHEN prop='name' THEN VALUE END) as post_name,
# MAGIC     collect_set(CASE WHEN prop='country' THEN VALUE END) as countries,
# MAGIC     collect_set(CASE WHEN prop='topics' THEN VALUE END) as topics,
# MAGIC     min(CASE WHEN prop='inceptionDate' THEN VALUE END) as inception_date_raw,
# MAGIC     max(CASE WHEN prop='dissolutionDate' THEN VALUE END) as dissolution_date_raw,
# MAGIC     mode(CASE WHEN prop='subnationalArea' THEN VALUE END ) as subnational_area,
# MAGIC     collect_set(dataset) as datasets,
# MAGIC     size(datasets) as source_count,
# MAGIC     max(_snapshot_version) as _snapshot_version
# MAGIC     FROM src 
# MAGIC     GROUP BY canonical_id
# MAGIC     HAVING count_if(prop='name')>0
# MAGIC
# MAGIC )
# MAGIC MERGE INTO  pep_dev.silver.post t
# MAGIC USING agg s
# MAGIC ON t.post_id=s.post_id
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     t.names                = s.names,
# MAGIC     t.post_name            = s.post_name,
# MAGIC     t.countries            = s.countries,
# MAGIC     t.topics               = s.topics,
# MAGIC     t.inception_date_raw   = s.inception_date_raw,
# MAGIC     t.dissolution_date_raw = s.dissolution_date_raw,
# MAGIC     t.subnational_area     = s.subnational_area,
# MAGIC     t.datasets             = s.datasets,
# MAGIC     t.source_count         = s.source_count,
# MAGIC     t._last_seen_snapshot  = s._snapshot_version
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC (post_id,names,post_name,countries,topics,inception_date_raw,dissolution_date_raw,subnational_area,datasets,source_count,_first_seen_snapshot,_last_seen_snapshot)
# MAGIC VALUES (
# MAGIC     s.post_id,s.names,s.post_name,s.countries,s.topics,s.inception_date_raw,s.dissolution_date_raw,s.subnational_area,s.datasets,s.source_count,s._snapshot_version,
# MAGIC     s._snapshot_version)
# MAGIC
# MAGIC
# MAGIC -- WHEN NOT MATCHED BY SOURCE THEN DELETE;
# MAGIC
# MAGIC