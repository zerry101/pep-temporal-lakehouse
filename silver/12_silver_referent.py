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
# MAGIC     coalesce(MAX (CASE WHEN canonical_id <> entity_id THEN canonical_id END),
# MAGIC     MAX(canonical_id)) as canonical_id,
# MAGIC
# MAGIC CASE WHEN array_contains(collect_set(schema), 'Person') THEN 'Person'
# MAGIC      WHEN array_contains(collect_set(schema), 'Company') THEN 'Company'
# MAGIC      WHEN array_contains(collect_set(schema), 'Organization') THEN 'Organization'
# MAGIC      WHEN array_contains(collect_set(schema), 'LegalEntity')  THEN 'LegalEntity'
# MAGIC      ELSE element_at(collect_set(schema),1)
# MAGIC      END as referent_kind,
# MAGIC
# MAGIC     max(_snapshot_version) as snap
# MAGIC     FROM  pep_dev.bronze.statements
# MAGIC     WHERE _snapshot_version = (SELECT MAX(snapshot_version) FROM pep_dev.bronze.snapshot_version_control)
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