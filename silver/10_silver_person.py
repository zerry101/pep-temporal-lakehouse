# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC -- subtype table
# MAGIC
# MAGIC -- idempotent query
# MAGIC
# MAGIC WITH src as (
# MAGIC
# MAGIC     SELECT * FROM pep_dev.bronze.statements
# MAGIC     WHERE SCHEMA IN ('Person') AND _snapshot_version=(SELECT MAX(_snapshot_version) FROM pep_dev.bronze.statements)
# MAGIC     
# MAGIC ),
# MAGIC agg AS(
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC canonical_id as person_id,
# MAGIC collect_set(CASE WHEN prop = 'name' THEN VALUE END) as names,
# MAGIC mode(CASE WHEN prop = 'name' THEN VALUE END) as primary_name,
# MAGIC collect_set(CASE WHEN prop='birthDate' THEN VALUE END) as birth_dates,
# MAGIC collect_set(CASE WHEN prop='country' THEN VALUE END) as countries,
# MAGIC collect_set(CASE WHEN prop='topics' THEN VALUE END) as topics,
# MAGIC coalesce(bool_or(prop='topics' AND value LIKE 'role.pep%'),FALSE) AS is_pep, -- INCDICATOR COLUMN
# MAGIC coalesce(bool_or(prop='topics' AND value LIKE 'sanction'),FALSE) AS is_sanctioned, -- INCDICATOR COLUMN
# MAGIC MAX(CASE WHEN prop='birthDate' THEN try_cast(LEFT(VALUE,4) AS INT) END ) as birth_year_max,
# MAGIC MIN(CASE WHEN prop="birthDate" THEN try_cast(LEFT(VALUE,4) AS INT) END) as birth_year_min,
# MAGIC MAX(CASE WHEN prop='deathDate' THEN VALUE END) as death_date_raw,
# MAGIC CASE WHEN length(death_date_raw)=10 THEN try_cast(death_date_raw AS DATE) END as death_date,
# MAGIC collect_set(dataset) AS datasets,
# MAGIC size(collect_set(dataset)) AS source_count,
# MAGIC max(_snapshot_version) AS _snapshot_version 
# MAGIC
# MAGIC  from src
# MAGIC  GROUP BY canonical_id
# MAGIC  HAVING COUNT_IF(prop='name') >0
# MAGIC  
# MAGIC )
# MAGIC MERGE INTO pep_dev.silver.person as t
# MAGIC USING agg as s
# MAGIC ON t.person_id=s.person_id
# MAGIC
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     t.primary_name=s.primary_name,
# MAGIC     t.names=s.names,
# MAGIC     t.birth_dates=s.birth_dates,
# MAGIC     t.countries=s.countries,
# MAGIC     t.topics=s.topics,
# MAGIC     t.is_pep=s.is_pep,
# MAGIC     t.is_sanctioned=s.is_sanctioned,
# MAGIC     t.birth_year_max=s.birth_year_max,
# MAGIC     t.birth_year_min=s.birth_year_min,
# MAGIC     t.death_date_raw=s.death_date_raw,
# MAGIC     t.death_date=s.death_date,
# MAGIC     t.datasets=s.datasets,
# MAGIC     t.source_count=s.source_count,
# MAGIC     t._last_seen_snapshot=s._snapshot_version
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC (person_id,primary_name,names,birth_dates,countries,topics,is_pep,is_sanctioned,birth_year_max,birth_year_min,death_date_raw,death_date,datasets,source_count,_first_seen_snapshot,_last_seen_snapshot)
# MAGIC VALUES (s.person_id,s.primary_name,s.names,s.birth_dates,s.countries,s.topics,s.is_pep,s.is_sanctioned,s.birth_year_max,s.birth_year_min,s.death_date_raw,s.death_date,s.datasets,s.source_count,s._snapshot_version,s._snapshot_version)
# MAGIC
# MAGIC -- WHEN NOT MATCHED BY SOURCE THEN DELETE
# MAGIC
# MAGIC