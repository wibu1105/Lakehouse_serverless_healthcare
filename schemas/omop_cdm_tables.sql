-- OMOP CDM v5.4 Table Definitions for Apache Iceberg
-- Healthcare Lakehouse - Silver Layer
-- Generated for AWS Glue/Athena with Iceberg format

-- ============================================================================
-- STANDARDIZED VOCABULARIES
-- ============================================================================

CREATE TABLE IF NOT EXISTS ${database_name}.concept (
  concept_id              BIGINT      COMMENT 'Unique identifier for each concept',
  concept_name            STRING      COMMENT 'Name/description of the concept',
  domain_id               STRING      COMMENT 'Domain the concept belongs to',
  vocabulary_id           STRING      COMMENT 'Vocabulary the concept belongs to',
  concept_class_id        STRING      COMMENT 'Class of the concept',
  standard_concept        STRING      COMMENT 'S=Standard, C=Classification, NULL=Non-standard',
  concept_code            STRING      COMMENT 'Source code of the concept',
  valid_start_date        DATE        COMMENT 'Date when concept became valid',
  valid_end_date          DATE        COMMENT 'Date when concept became invalid',
  invalid_reason          STRING      COMMENT 'Reason for invalidation'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/concept/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.vocabulary (
  vocabulary_id           STRING      COMMENT 'Unique identifier for each vocabulary',
  vocabulary_name         STRING      COMMENT 'Name of the vocabulary',
  vocabulary_reference    STRING      COMMENT 'Reference to documentation',
  vocabulary_version      STRING      COMMENT 'Version of the vocabulary',
  vocabulary_concept_id   BIGINT      COMMENT 'Concept_id for this vocabulary'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/vocabulary/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.domain (
  domain_id               STRING      COMMENT 'Unique identifier for each domain',
  domain_name             STRING      COMMENT 'Name of the domain',
  domain_concept_id       BIGINT      COMMENT 'Concept_id for this domain'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/domain/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.concept_class (
  concept_class_id        STRING      COMMENT 'Unique identifier for each concept class',
  concept_class_name      STRING      COMMENT 'Name of the concept class',
  concept_class_concept_id BIGINT     COMMENT 'Concept_id for this concept class'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/concept_class/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.concept_relationship (
  concept_id_1            BIGINT      COMMENT 'First concept in the relationship',
  concept_id_2            BIGINT      COMMENT 'Second concept in the relationship',
  relationship_id         STRING      COMMENT 'Type of relationship',
  valid_start_date        DATE        COMMENT 'Date when relationship became valid',
  valid_end_date          DATE        COMMENT 'Date when relationship became invalid',
  invalid_reason          STRING      COMMENT 'Reason for invalidation'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/concept_relationship/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.relationship (
  relationship_id         STRING      COMMENT 'Unique identifier for each relationship',
  relationship_name       STRING      COMMENT 'Name of the relationship',
  is_hierarchical         STRING      COMMENT 'Indicates hierarchical relationship',
  defines_ancestry        STRING      COMMENT 'Defines ancestry relationship',
  reverse_relationship_id STRING      COMMENT 'Reverse of this relationship',
  relationship_concept_id BIGINT      COMMENT 'Concept_id for this relationship'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/relationship/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.concept_synonym (
  concept_id              BIGINT      COMMENT 'Concept this synonym refers to',
  concept_synonym_name    STRING      COMMENT 'Synonym name',
  language_concept_id     BIGINT      COMMENT 'Language of the synonym'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/concept_synonym/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.concept_ancestor (
  ancestor_concept_id     BIGINT      COMMENT 'Ancestor concept',
  descendant_concept_id   BIGINT      COMMENT 'Descendant concept',
  min_levels_of_separation BIGINT     COMMENT 'Minimum levels of separation',
  max_levels_of_separation BIGINT     COMMENT 'Maximum levels of separation'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/concept_ancestor/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.source_to_concept_map (
  source_code             STRING      COMMENT 'Source code to map',
  source_concept_id       BIGINT      COMMENT 'Source concept_id',
  source_vocabulary_id    STRING      COMMENT 'Source vocabulary',
  source_code_description STRING      COMMENT 'Description of source code',
  target_concept_id       BIGINT      COMMENT 'Target standard concept_id',
  target_vocabulary_id    STRING      COMMENT 'Target vocabulary',
  valid_start_date        DATE        COMMENT 'Start date of mapping validity',
  valid_end_date          DATE        COMMENT 'End date of mapping validity',
  invalid_reason          STRING      COMMENT 'Reason for invalidation'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/source_to_concept_map/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.drug_strength (
  drug_concept_id         BIGINT      COMMENT 'Drug concept',
  ingredient_concept_id   BIGINT      COMMENT 'Ingredient concept',
  amount_value            DOUBLE      COMMENT 'Amount value',
  amount_unit_concept_id  BIGINT      COMMENT 'Amount unit concept',
  numerator_value         DOUBLE      COMMENT 'Numerator for ratio',
  numerator_unit_concept_id BIGINT    COMMENT 'Numerator unit concept',
  denominator_value       DOUBLE      COMMENT 'Denominator for ratio',
  denominator_unit_concept_id BIGINT  COMMENT 'Denominator unit concept',
  box_size                BIGINT      COMMENT 'Number of units in box',
  valid_start_date        DATE        COMMENT 'Start date of validity',
  valid_end_date          DATE        COMMENT 'End date of validity',
  invalid_reason          STRING      COMMENT 'Reason for invalidation'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/vocabulary/drug_strength/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

-- ============================================================================
-- STANDARDIZED METADATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS ${database_name}.cdm_source (
  cdm_source_name                 STRING      COMMENT 'Name of the CDM instance',
  cdm_source_abbreviation         STRING      COMMENT 'Abbreviation of the CDM source',
  cdm_holder                      STRING      COMMENT 'Organization holding the CDM',
  source_description              STRING      COMMENT 'Description of the source data',
  source_documentation_reference  STRING      COMMENT 'Reference to documentation',
  cdm_etl_reference               STRING      COMMENT 'Reference to ETL documentation',
  source_release_date             DATE        COMMENT 'Release date of source data',
  cdm_release_date                DATE        COMMENT 'Release date of CDM',
  cdm_version                     STRING      COMMENT 'CDM version',
  cdm_version_concept_id          BIGINT      COMMENT 'Concept_id for CDM version',
  vocabulary_version              STRING      COMMENT 'Vocabulary version used'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/metadata/cdm_source/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.metadata (
  metadata_id             BIGINT      COMMENT 'Unique identifier',
  metadata_concept_id     BIGINT      COMMENT 'Concept for metadata type',
  metadata_type_concept_id BIGINT     COMMENT 'Concept for metadata category',
  name                    STRING      COMMENT 'Name of metadata item',
  value_as_string         STRING      COMMENT 'String value',
  value_as_concept_id     BIGINT      COMMENT 'Concept value',
  value_as_number         DOUBLE      COMMENT 'Numeric value',
  metadata_date           DATE        COMMENT 'Date of metadata',
  metadata_datetime       TIMESTAMP   COMMENT 'Datetime of metadata'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/metadata/metadata/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

-- ============================================================================
-- STANDARDIZED CLINICAL DATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS ${database_name}.person (
  person_id                   BIGINT      COMMENT 'Unique identifier for each person',
  gender_concept_id           BIGINT      COMMENT 'Gender concept',
  year_of_birth               INT         COMMENT 'Year of birth',
  month_of_birth              INT         COMMENT 'Month of birth',
  day_of_birth                INT         COMMENT 'Day of birth',
  birth_datetime              TIMESTAMP   COMMENT 'Full birth datetime',
  race_concept_id             BIGINT      COMMENT 'Race concept',
  ethnicity_concept_id        BIGINT      COMMENT 'Ethnicity concept',
  location_id                 BIGINT      COMMENT 'Location reference',
  provider_id                 BIGINT      COMMENT 'Provider reference',
  care_site_id                BIGINT      COMMENT 'Care site reference',
  person_source_value         STRING      COMMENT 'Source identifier',
  gender_source_value         STRING      COMMENT 'Source gender value',
  gender_source_concept_id    BIGINT      COMMENT 'Source gender concept',
  race_source_value           STRING      COMMENT 'Source race value',
  race_source_concept_id      BIGINT      COMMENT 'Source race concept',
  ethnicity_source_value      STRING      COMMENT 'Source ethnicity value',
  ethnicity_source_concept_id BIGINT      COMMENT 'Source ethnicity concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/person/'
PARTITIONED BY (year_of_birth)
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.observation_period (
  observation_period_id           BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  observation_period_start_date   DATE        COMMENT 'Start of observation',
  observation_period_end_date     DATE        COMMENT 'End of observation',
  period_type_concept_id          BIGINT      COMMENT 'Type of period'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/observation_period/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.visit_occurrence (
  visit_occurrence_id             BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  visit_concept_id                BIGINT      COMMENT 'Visit type concept',
  visit_start_date                DATE        COMMENT 'Start date of visit',
  visit_start_datetime            TIMESTAMP   COMMENT 'Start datetime of visit',
  visit_end_date                  DATE        COMMENT 'End date of visit',
  visit_end_datetime              TIMESTAMP   COMMENT 'End datetime of visit',
  visit_type_concept_id           BIGINT      COMMENT 'Type of visit record',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  care_site_id                    BIGINT      COMMENT 'Care site reference',
  visit_source_value              STRING      COMMENT 'Source visit value',
  visit_source_concept_id         BIGINT      COMMENT 'Source visit concept',
  admitted_from_concept_id        BIGINT      COMMENT 'Admitted from concept',
  admitted_from_source_value      STRING      COMMENT 'Source admitted from',
  discharged_to_concept_id        BIGINT      COMMENT 'Discharged to concept',
  discharged_to_source_value      STRING      COMMENT 'Source discharged to',
  preceding_visit_occurrence_id   BIGINT      COMMENT 'Previous visit reference'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/visit_occurrence/'
PARTITIONED BY (years(visit_start_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.visit_detail (
  visit_detail_id                 BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  visit_detail_concept_id         BIGINT      COMMENT 'Visit detail type concept',
  visit_detail_start_date         DATE        COMMENT 'Start date',
  visit_detail_start_datetime     TIMESTAMP   COMMENT 'Start datetime',
  visit_detail_end_date           DATE        COMMENT 'End date',
  visit_detail_end_datetime       TIMESTAMP   COMMENT 'End datetime',
  visit_detail_type_concept_id    BIGINT      COMMENT 'Type of detail record',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  care_site_id                    BIGINT      COMMENT 'Care site reference',
  visit_detail_source_value       STRING      COMMENT 'Source value',
  visit_detail_source_concept_id  BIGINT      COMMENT 'Source concept',
  admitted_from_concept_id        BIGINT      COMMENT 'Admitted from concept',
  admitted_from_source_value      STRING      COMMENT 'Source admitted from',
  discharged_to_concept_id        BIGINT      COMMENT 'Discharged to concept',
  discharged_to_source_value      STRING      COMMENT 'Source discharged to',
  preceding_visit_detail_id       BIGINT      COMMENT 'Previous detail reference',
  parent_visit_detail_id          BIGINT      COMMENT 'Parent detail reference',
  visit_occurrence_id             BIGINT      COMMENT 'Parent visit reference'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/visit_detail/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.condition_occurrence (
  condition_occurrence_id         BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  condition_concept_id            BIGINT      COMMENT 'Condition concept',
  condition_start_date            DATE        COMMENT 'Start date of condition',
  condition_start_datetime        TIMESTAMP   COMMENT 'Start datetime',
  condition_end_date              DATE        COMMENT 'End date of condition',
  condition_end_datetime          TIMESTAMP   COMMENT 'End datetime',
  condition_type_concept_id       BIGINT      COMMENT 'Type of condition record',
  condition_status_concept_id     BIGINT      COMMENT 'Status of condition',
  stop_reason                     STRING      COMMENT 'Reason for stopping',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  condition_source_value          STRING      COMMENT 'Source condition value',
  condition_source_concept_id     BIGINT      COMMENT 'Source condition concept',
  condition_status_source_value   STRING      COMMENT 'Source status value'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/condition_occurrence/'
PARTITIONED BY (years(condition_start_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.drug_exposure (
  drug_exposure_id                BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  drug_concept_id                 BIGINT      COMMENT 'Drug concept',
  drug_exposure_start_date        DATE        COMMENT 'Start date of exposure',
  drug_exposure_start_datetime    TIMESTAMP   COMMENT 'Start datetime',
  drug_exposure_end_date          DATE        COMMENT 'End date of exposure',
  drug_exposure_end_datetime      TIMESTAMP   COMMENT 'End datetime',
  verbatim_end_date               DATE        COMMENT 'Verbatim end date',
  drug_type_concept_id            BIGINT      COMMENT 'Type of drug record',
  stop_reason                     STRING      COMMENT 'Reason for stopping',
  refills                         INT         COMMENT 'Number of refills',
  quantity                        DOUBLE      COMMENT 'Quantity dispensed',
  days_supply                     INT         COMMENT 'Days of supply',
  sig                             STRING      COMMENT 'Directions for use',
  route_concept_id                BIGINT      COMMENT 'Route of administration',
  lot_number                      STRING      COMMENT 'Lot number',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  drug_source_value               STRING      COMMENT 'Source drug value',
  drug_source_concept_id          BIGINT      COMMENT 'Source drug concept',
  route_source_value              STRING      COMMENT 'Source route value',
  dose_unit_source_value          STRING      COMMENT 'Source dose unit value'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/drug_exposure/'
PARTITIONED BY (years(drug_exposure_start_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.procedure_occurrence (
  procedure_occurrence_id         BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  procedure_concept_id            BIGINT      COMMENT 'Procedure concept',
  procedure_date                  DATE        COMMENT 'Date of procedure',
  procedure_datetime              TIMESTAMP   COMMENT 'Datetime of procedure',
  procedure_end_date              DATE        COMMENT 'End date of procedure',
  procedure_end_datetime          TIMESTAMP   COMMENT 'End datetime',
  procedure_type_concept_id       BIGINT      COMMENT 'Type of procedure record',
  modifier_concept_id             BIGINT      COMMENT 'Modifier concept',
  quantity                        INT         COMMENT 'Quantity of procedures',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  procedure_source_value          STRING      COMMENT 'Source procedure value',
  procedure_source_concept_id     BIGINT      COMMENT 'Source procedure concept',
  modifier_source_value           STRING      COMMENT 'Source modifier value'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/procedure_occurrence/'
PARTITIONED BY (years(procedure_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.device_exposure (
  device_exposure_id              BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  device_concept_id               BIGINT      COMMENT 'Device concept',
  device_exposure_start_date      DATE        COMMENT 'Start date',
  device_exposure_start_datetime  TIMESTAMP   COMMENT 'Start datetime',
  device_exposure_end_date        DATE        COMMENT 'End date',
  device_exposure_end_datetime    TIMESTAMP   COMMENT 'End datetime',
  device_type_concept_id          BIGINT      COMMENT 'Type of device record',
  unique_device_id                STRING      COMMENT 'Unique device identifier',
  production_id                   STRING      COMMENT 'Production identifier',
  quantity                        INT         COMMENT 'Quantity of devices',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  device_source_value             STRING      COMMENT 'Source device value',
  device_source_concept_id        BIGINT      COMMENT 'Source device concept',
  unit_concept_id                 BIGINT      COMMENT 'Unit concept',
  unit_source_value               STRING      COMMENT 'Source unit value',
  unit_source_concept_id          BIGINT      COMMENT 'Source unit concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/device_exposure/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.measurement (
  measurement_id                  BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  measurement_concept_id          BIGINT      COMMENT 'Measurement concept',
  measurement_date                DATE        COMMENT 'Date of measurement',
  measurement_datetime            TIMESTAMP   COMMENT 'Datetime of measurement',
  measurement_time                STRING      COMMENT 'Time of measurement',
  measurement_type_concept_id     BIGINT      COMMENT 'Type of measurement record',
  operator_concept_id             BIGINT      COMMENT 'Operator concept',
  value_as_number                 DOUBLE      COMMENT 'Numeric value',
  value_as_concept_id             BIGINT      COMMENT 'Concept value',
  unit_concept_id                 BIGINT      COMMENT 'Unit concept',
  range_low                       DOUBLE      COMMENT 'Normal range low',
  range_high                      DOUBLE      COMMENT 'Normal range high',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  measurement_source_value        STRING      COMMENT 'Source measurement value',
  measurement_source_concept_id   BIGINT      COMMENT 'Source measurement concept',
  unit_source_value               STRING      COMMENT 'Source unit value',
  unit_source_concept_id          BIGINT      COMMENT 'Source unit concept',
  value_source_value              STRING      COMMENT 'Source value string',
  measurement_event_id            BIGINT      COMMENT 'Related event ID',
  meas_event_field_concept_id     BIGINT      COMMENT 'Event field concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/measurement/'
PARTITIONED BY (years(measurement_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.observation (
  observation_id                  BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  observation_concept_id          BIGINT      COMMENT 'Observation concept',
  observation_date                DATE        COMMENT 'Date of observation',
  observation_datetime            TIMESTAMP   COMMENT 'Datetime of observation',
  observation_type_concept_id     BIGINT      COMMENT 'Type of observation record',
  value_as_number                 DOUBLE      COMMENT 'Numeric value',
  value_as_string                 STRING      COMMENT 'String value',
  value_as_concept_id             BIGINT      COMMENT 'Concept value',
  qualifier_concept_id            BIGINT      COMMENT 'Qualifier concept',
  unit_concept_id                 BIGINT      COMMENT 'Unit concept',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  observation_source_value        STRING      COMMENT 'Source observation value',
  observation_source_concept_id   BIGINT      COMMENT 'Source observation concept',
  unit_source_value               STRING      COMMENT 'Source unit value',
  qualifier_source_value          STRING      COMMENT 'Source qualifier value',
  value_source_value              STRING      COMMENT 'Source value string',
  observation_event_id            BIGINT      COMMENT 'Related event ID',
  obs_event_field_concept_id      BIGINT      COMMENT 'Event field concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/observation/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.death (
  person_id                       BIGINT      COMMENT 'Person reference',
  death_date                      DATE        COMMENT 'Date of death',
  death_datetime                  TIMESTAMP   COMMENT 'Datetime of death',
  death_type_concept_id           BIGINT      COMMENT 'Type of death record',
  cause_concept_id                BIGINT      COMMENT 'Cause of death concept',
  cause_source_value              STRING      COMMENT 'Source cause value',
  cause_source_concept_id         BIGINT      COMMENT 'Source cause concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/death/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.note (
  note_id                         BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  note_date                       DATE        COMMENT 'Date of note',
  note_datetime                   TIMESTAMP   COMMENT 'Datetime of note',
  note_type_concept_id            BIGINT      COMMENT 'Type of note record',
  note_class_concept_id           BIGINT      COMMENT 'Class of note',
  note_title                      STRING      COMMENT 'Title of note',
  note_text                       STRING      COMMENT 'Text content of note',
  encoding_concept_id             BIGINT      COMMENT 'Encoding concept',
  language_concept_id             BIGINT      COMMENT 'Language concept',
  provider_id                     BIGINT      COMMENT 'Provider reference',
  visit_occurrence_id             BIGINT      COMMENT 'Visit reference',
  visit_detail_id                 BIGINT      COMMENT 'Visit detail reference',
  note_source_value               STRING      COMMENT 'Source note value',
  note_event_id                   BIGINT      COMMENT 'Related event ID',
  note_event_field_concept_id     BIGINT      COMMENT 'Event field concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/note/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.note_nlp (
  note_nlp_id                     BIGINT      COMMENT 'Unique identifier',
  note_id                         BIGINT      COMMENT 'Note reference',
  section_concept_id              BIGINT      COMMENT 'Section concept',
  snippet                         STRING      COMMENT 'Text snippet',
  offset                          STRING      COMMENT 'Character offset',
  lexical_variant                 STRING      COMMENT 'Lexical variant',
  note_nlp_concept_id             BIGINT      COMMENT 'NLP concept',
  note_nlp_source_concept_id      BIGINT      COMMENT 'Source NLP concept',
  nlp_system                      STRING      COMMENT 'NLP system used',
  nlp_date                        DATE        COMMENT 'Date of NLP processing',
  nlp_datetime                    TIMESTAMP   COMMENT 'Datetime of NLP processing',
  term_exists                     STRING      COMMENT 'Term existence flag',
  term_temporal                   STRING      COMMENT 'Term temporal aspect',
  term_modifiers                  STRING      COMMENT 'Term modifiers'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/note_nlp/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.specimen (
  specimen_id                     BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  specimen_concept_id             BIGINT      COMMENT 'Specimen concept',
  specimen_type_concept_id        BIGINT      COMMENT 'Type of specimen record',
  specimen_date                   DATE        COMMENT 'Date of specimen collection',
  specimen_datetime               TIMESTAMP   COMMENT 'Datetime of collection',
  quantity                        DOUBLE      COMMENT 'Quantity collected',
  unit_concept_id                 BIGINT      COMMENT 'Unit concept',
  anatomic_site_concept_id        BIGINT      COMMENT 'Anatomic site concept',
  disease_status_concept_id       BIGINT      COMMENT 'Disease status concept',
  specimen_source_id              STRING      COMMENT 'Source identifier',
  specimen_source_value           STRING      COMMENT 'Source specimen value',
  unit_source_value               STRING      COMMENT 'Source unit value',
  anatomic_site_source_value      STRING      COMMENT 'Source anatomic site value',
  disease_status_source_value     STRING      COMMENT 'Source disease status value'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/specimen/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.fact_relationship (
  domain_concept_id_1             BIGINT      COMMENT 'Domain of first fact',
  fact_id_1                       BIGINT      COMMENT 'ID of first fact',
  domain_concept_id_2             BIGINT      COMMENT 'Domain of second fact',
  fact_id_2                       BIGINT      COMMENT 'ID of second fact',
  relationship_concept_id         BIGINT      COMMENT 'Relationship concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/clinical/fact_relationship/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

-- ============================================================================
-- HEALTH SYSTEM DATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS ${database_name}.location (
  location_id                     BIGINT      COMMENT 'Unique identifier',
  address_1                       STRING      COMMENT 'Address line 1',
  address_2                       STRING      COMMENT 'Address line 2',
  city                            STRING      COMMENT 'City',
  state                           STRING      COMMENT 'State',
  zip                             STRING      COMMENT 'ZIP code',
  county                          STRING      COMMENT 'County',
  location_source_value           STRING      COMMENT 'Source location value',
  country_concept_id              BIGINT      COMMENT 'Country concept',
  country_source_value            STRING      COMMENT 'Source country value',
  latitude                        DOUBLE      COMMENT 'Latitude',
  longitude                       DOUBLE      COMMENT 'Longitude'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/health_system/location/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.care_site (
  care_site_id                    BIGINT      COMMENT 'Unique identifier',
  care_site_name                  STRING      COMMENT 'Name of care site',
  place_of_service_concept_id     BIGINT      COMMENT 'Place of service concept',
  location_id                     BIGINT      COMMENT 'Location reference',
  care_site_source_value          STRING      COMMENT 'Source care site value',
  place_of_service_source_value   STRING      COMMENT 'Source place of service value'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/health_system/care_site/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.provider (
  provider_id                     BIGINT      COMMENT 'Unique identifier',
  provider_name                   STRING      COMMENT 'Provider name',
  npi                             STRING      COMMENT 'National Provider Identifier',
  dea                             STRING      COMMENT 'DEA number',
  specialty_concept_id            BIGINT      COMMENT 'Specialty concept',
  care_site_id                    BIGINT      COMMENT 'Care site reference',
  year_of_birth                   INT         COMMENT 'Year of birth',
  gender_concept_id               BIGINT      COMMENT 'Gender concept',
  provider_source_value           STRING      COMMENT 'Source provider value',
  specialty_source_value          STRING      COMMENT 'Source specialty value',
  specialty_source_concept_id     BIGINT      COMMENT 'Source specialty concept',
  gender_source_value             STRING      COMMENT 'Source gender value',
  gender_source_concept_id        BIGINT      COMMENT 'Source gender concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/health_system/provider/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

-- ============================================================================
-- HEALTH ECONOMICS DATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS ${database_name}.payer_plan_period (
  payer_plan_period_id            BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  payer_plan_period_start_date    DATE        COMMENT 'Start date of period',
  payer_plan_period_end_date      DATE        COMMENT 'End date of period',
  payer_concept_id                BIGINT      COMMENT 'Payer concept',
  payer_source_value              STRING      COMMENT 'Source payer value',
  payer_source_concept_id         BIGINT      COMMENT 'Source payer concept',
  plan_concept_id                 BIGINT      COMMENT 'Plan concept',
  plan_source_value               STRING      COMMENT 'Source plan value',
  plan_source_concept_id          BIGINT      COMMENT 'Source plan concept',
  sponsor_concept_id              BIGINT      COMMENT 'Sponsor concept',
  sponsor_source_value            STRING      COMMENT 'Source sponsor value',
  sponsor_source_concept_id       BIGINT      COMMENT 'Source sponsor concept',
  family_source_value             STRING      COMMENT 'Family identifier',
  stop_reason_concept_id          BIGINT      COMMENT 'Stop reason concept',
  stop_reason_source_value        STRING      COMMENT 'Source stop reason value',
  stop_reason_source_concept_id   BIGINT      COMMENT 'Source stop reason concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/economics/payer_plan_period/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.cost (
  cost_id                         BIGINT      COMMENT 'Unique identifier',
  cost_event_id                   BIGINT      COMMENT 'Related event ID',
  cost_domain_id                  STRING      COMMENT 'Domain of cost event',
  cost_type_concept_id            BIGINT      COMMENT 'Type of cost record',
  currency_concept_id             BIGINT      COMMENT 'Currency concept',
  total_charge                    DOUBLE      COMMENT 'Total charge amount',
  total_cost                      DOUBLE      COMMENT 'Total cost amount',
  total_paid                      DOUBLE      COMMENT 'Total paid amount',
  paid_by_payer                   DOUBLE      COMMENT 'Paid by payer',
  paid_by_patient                 DOUBLE      COMMENT 'Paid by patient',
  paid_patient_copay              DOUBLE      COMMENT 'Patient copay',
  paid_patient_coinsurance        DOUBLE      COMMENT 'Patient coinsurance',
  paid_patient_deductible         DOUBLE      COMMENT 'Patient deductible',
  paid_by_primary                 DOUBLE      COMMENT 'Paid by primary insurer',
  paid_ingredient_cost            DOUBLE      COMMENT 'Ingredient cost',
  paid_dispensing_fee             DOUBLE      COMMENT 'Dispensing fee',
  payer_plan_period_id            BIGINT      COMMENT 'Payer plan period reference',
  amount_allowed                  DOUBLE      COMMENT 'Amount allowed',
  revenue_code_concept_id         BIGINT      COMMENT 'Revenue code concept',
  revenue_code_source_value       STRING      COMMENT 'Source revenue code',
  drg_concept_id                  BIGINT      COMMENT 'DRG concept',
  drg_source_value                STRING      COMMENT 'Source DRG value'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/economics/cost/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

-- ============================================================================
-- STANDARDIZED DERIVED ELEMENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS ${database_name}.cohort (
  cohort_definition_id            BIGINT      COMMENT 'Cohort definition reference',
  subject_id                      BIGINT      COMMENT 'Subject ID (usually person_id)',
  cohort_start_date              DATE        COMMENT 'Start date of cohort membership',
  cohort_end_date                DATE        COMMENT 'End date of cohort membership'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/cohort/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.cohort_definition (
  cohort_definition_id            BIGINT      COMMENT 'Unique identifier',
  cohort_definition_name          STRING      COMMENT 'Name of cohort definition',
  cohort_definition_description   STRING      COMMENT 'Description of cohort',
  definition_type_concept_id      BIGINT      COMMENT 'Type of definition',
  cohort_definition_syntax        STRING      COMMENT 'Syntax/logic of definition',
  subject_concept_id              BIGINT      COMMENT 'Subject concept',
  cohort_initiation_date          DATE        COMMENT 'Date cohort was initiated'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/cohort_definition/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.drug_era (
  drug_era_id                     BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  drug_concept_id                 BIGINT      COMMENT 'Drug concept (ingredient level)',
  drug_era_start_date             DATE        COMMENT 'Start date of era',
  drug_era_end_date               DATE        COMMENT 'End date of era',
  drug_exposure_count             INT         COMMENT 'Number of exposures in era',
  gap_days                        INT         COMMENT 'Total gap days in era'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/drug_era/'
PARTITIONED BY (years(drug_era_start_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.dose_era (
  dose_era_id                     BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  drug_concept_id                 BIGINT      COMMENT 'Drug concept',
  unit_concept_id                 BIGINT      COMMENT 'Unit concept',
  dose_value                      DOUBLE      COMMENT 'Dose value',
  dose_era_start_date             DATE        COMMENT 'Start date of dose era',
  dose_era_end_date               DATE        COMMENT 'End date of dose era'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/dose_era/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.condition_era (
  condition_era_id                BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  condition_concept_id            BIGINT      COMMENT 'Condition concept',
  condition_era_start_date        DATE        COMMENT 'Start date of era',
  condition_era_end_date          DATE        COMMENT 'End date of era',
  condition_occurrence_count      INT         COMMENT 'Number of occurrences in era'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/condition_era/'
PARTITIONED BY (years(condition_era_start_date))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.episode (
  episode_id                      BIGINT      COMMENT 'Unique identifier',
  person_id                       BIGINT      COMMENT 'Person reference',
  episode_concept_id              BIGINT      COMMENT 'Episode concept',
  episode_start_date              DATE        COMMENT 'Start date',
  episode_start_datetime          TIMESTAMP   COMMENT 'Start datetime',
  episode_end_date                DATE        COMMENT 'End date',
  episode_end_datetime            TIMESTAMP   COMMENT 'End datetime',
  episode_parent_id               BIGINT      COMMENT 'Parent episode reference',
  episode_number                  INT         COMMENT 'Episode number',
  episode_object_concept_id       BIGINT      COMMENT 'Object concept',
  episode_type_concept_id         BIGINT      COMMENT 'Type concept',
  episode_source_value            STRING      COMMENT 'Source value',
  episode_source_concept_id       BIGINT      COMMENT 'Source concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/episode/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);

CREATE TABLE IF NOT EXISTS ${database_name}.episode_event (
  episode_id                      BIGINT      COMMENT 'Episode reference',
  event_id                        BIGINT      COMMENT 'Event ID',
  episode_event_field_concept_id  BIGINT      COMMENT 'Event field concept'
)
USING ICEBERG
LOCATION 's3://${s3_bucket}/silver/derived/episode_event/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'snappy'
);
