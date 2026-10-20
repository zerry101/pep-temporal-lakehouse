# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO pep_dev.silver.referent as t
# MAGIC USING (
# MAGIC     SELECT entity_id,
# MAGIC     MAX(canonical_id) as canonical_id,
# MAGIC     MAX(schema) as referent_kind,
# MAGIC     max(_snapshot_version) as snap
# MAGIC     FROM  pep_dev.bronze.statements
# MAGIC     WHERE SCHEMA IN ('Person','Position')
# MAGIC     AND _snapshot_version = (SELECT MAX(_snapshot_version) FROM pep_dev.bronze.statements)
# MAGIC     GROUP BY entity_id
# MAGIC
# MAGIC )as s
# MAGIC ON t.entity_id = s.entity_id
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     t._last_seen_snapshot=s.snap,
# MAGIC     t.canonical_id=s.canonical_id,
# MAGIC     t.referent_kind=s.referent_kind
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC     (entity_id, canonical_id, referent_kind, _first_seen_snapshot, _last_seen_snapshot)
# MAGIC     VALUES (s.entity_id, s.canonical_id, s.referent_kind, s.snap, s.snap)
# MAGIC
# MAGIC