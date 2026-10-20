# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE  TABLE IF NOT EXISTS pep_dev.silver.person(
# MAGIC     person_id STRING PRIMARY KEY,
# MAGIC     primary_name STRING NOT NULL,
# MAGIC     names ARRAY <STRING> NOT NULL,
# MAGIC     birth_dates ARRAY <STRING>,
# MAGIC     birth_year_min INTEGER ,
# MAGIC     birth_year_max integer ,
# MAGIC     death_date_raw string ,
# MAGIC     death_date date ,
# MAGIC     countries Array <STRING> ,
# MAGIC     topics  Array <STRING> ,
# MAGIC     is_pep BOOLEAN NOT NULL,
# MAGIC     is_sanctioned BOOLEAN NOT NULL,
# MAGIC     datasets Array <STRING> NOT NULL,
# MAGIC     source_count integer NOT NULL,
# MAGIC     _first_seen_snapshot STRING NOT NULL,
# MAGIC     _last_seen_snapshot STRING NOT NULL
# MAGIC
# MAGIC )       

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS pep_dev.silver.post 
# MAGIC (
# MAGIC   post_id STRING PRIMARY KEY,
# MAGIC   post_name STRING NOT NULL,
# MAGIC   names Array<STRING> NOT NULL,
# MAGIC   countries Array<STRING> ,
# MAGIC   topics Array<STRING> ,
# MAGIC   inception_date_raw STRING,
# MAGIC   dissolution_date_raw STRING,
# MAGIC   subnational_area STRING,
# MAGIC   datasets Array<STRING> NOT NULL,
# MAGIC   source_count integer NOT NULL,
# MAGIC _first_seen_snapshot STRING NOT NULL,
# MAGIC _last_seen_snapshot STRING NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS pep_dev.silver.referent
# MAGIC (
# MAGIC entity_id STRING PRIMARY KEY,
# MAGIC canonical_id STRING NOT NULL,
# MAGIC referent_kind STRING NOT NULL,
# MAGIC _first_seen_snapshot STRING NOT NULL,
# MAGIC _last_seen_snapshot STRING NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS pep_dev.silver.occupancy (
# MAGIC occupancy_id string PRIMARY KEY,
# MAGIC person_id string ,
# MAGIC person_ref string   NOT NULL,
# MAGIC post_id string ,
# MAGIC post_ref string  NOT NULL,
# MAGIC status_raw string ,
# MAGIC status_norm string ,
# MAGIC start_raw string ,
# MAGIC end_raw string,
# MAGIC start_date date ,
# MAGIC end_date date ,
# MAGIC start_precision string ,
# MAGIC end_precision string ,
# MAGIC datasets ARRAY<STRING> NOT NULL,
# MAGIC source_count INT,
# MAGIC date_conflict BOOLEAN,
# MAGIC _first_seen_snapshot STRING NOT NULL,
# MAGIC _last_seen_snapshot STRING NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS pep_dev.silver.quarantine (
# MAGIC   quarantine_id   STRING NOT NULL,
# MAGIC   source_table    STRING NOT NULL,
# MAGIC   source_id       STRING NOT NULL,
# MAGIC   reject_stage    STRING NOT NULL,
# MAGIC   reject_reason   STRING NOT NULL,
# MAGIC   raw_payload     STRING,
# MAGIC   _quarantined_at timestamp NOT NULL,
# MAGIC   _snapshot_version STRING NOT NULL,
# MAGIC CONSTRAINT quarantine_pk PRIMARY KEY (quarantine_id)
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE pep_dev.silver.quarantine
# MAGIC DROP CONSTRAINT IF EXISTS chK_reject_reason; 
# MAGIC ALTER TABLE pep_dev.silver.quarantine ADD CONSTRAINT chK_reject_reason CHECK( reject_reason IN 
# MAGIC ( 'null_entity_id',
# MAGIC   'unresolvable_reference',
# MAGIC   'unparseable_date',
# MAGIC   'unparseable_percentage',
# MAGIC   'schema_property_mismatch',
# MAGIC   'duplicate_grain_violation',
# MAGIC   'missing_required_property',
# MAGIC   'unclassified'
# MAGIC )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE pep_dev.silver.occupancy
# MAGIC DROP CONSTRAINT IF EXISTS fk_occupancy_person;
# MAGIC
# MAGIC ALTER TABLE pep_dev.silver.occupancy
# MAGIC ADD CONSTRAINT fk_occupancy_person
# MAGIC FOREIGN KEY (person_id) REFERENCES  pep_dev.silver.person(person_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE pep_dev.silver.occupancy
# MAGIC DROP CONSTRAINT IF EXISTS fk_occupancy_post;
# MAGIC
# MAGIC ALTER TABLE pep_dev.silver.occupancy
# MAGIC ADD CONSTRAINT fk_occupancy_post
# MAGIC FOREIGN KEY (post_id) REFERENCES pep_dev.silver.post(post_id);
# MAGIC