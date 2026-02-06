-- OMOP CDM Vocabulary Mapping Views
-- Source-to-Standard and Source-to-Source mappings for concept translation
-- Used by ETL jobs for SNOMED, ICD-10, LOINC, RxNorm, CVX mappings

-- ============================================================================
-- SOURCE TO STANDARD VOCABULARY MAP
-- Maps source codes to standard OMOP concepts
-- ============================================================================

CREATE OR REPLACE VIEW ${database_name}.source_to_standard_vocab_map AS
WITH vocab_map AS (
  -- Map from concept table via concept_relationship
  SELECT
    c.concept_code           AS source_code,
    c.concept_id            AS source_concept_id,
    c.concept_name          AS source_code_description,
    c.vocabulary_id         AS source_vocabulary_id,
    c.domain_id             AS source_domain_id,
    c.concept_class_id      AS source_concept_class_id,
    c.valid_start_date      AS source_valid_start_date,
    c.valid_end_date        AS source_valid_end_date,
    c.invalid_reason        AS source_invalid_reason,
    c1.concept_id           AS target_concept_id,
    c1.concept_name         AS target_concept_name,
    c1.vocabulary_id        AS target_vocabulary_id,
    c1.domain_id            AS target_domain_id,
    c1.concept_class_id     AS target_concept_class_id,
    c1.invalid_reason       AS target_invalid_reason,
    c1.standard_concept     AS target_standard_concept
  FROM ${database_name}.concept c
  INNER JOIN ${database_name}.concept_relationship cr
    ON c.concept_id = cr.concept_id_1
    AND cr.invalid_reason IS NULL
    AND LOWER(cr.relationship_id) = 'maps to'
  INNER JOIN ${database_name}.concept c1
    ON cr.concept_id_2 = c1.concept_id
    AND c1.invalid_reason IS NULL
  
  UNION ALL
  
  -- Map from source_to_concept_map table
  SELECT
    stcm.source_code,
    stcm.source_concept_id,
    stcm.source_code_description,
    stcm.source_vocabulary_id,
    c1.domain_id            AS source_domain_id,
    c1.concept_class_id     AS source_concept_class_id,
    c1.valid_start_date     AS source_valid_start_date,
    c1.valid_end_date       AS source_valid_end_date,
    stcm.invalid_reason     AS source_invalid_reason,
    stcm.target_concept_id,
    c2.concept_name         AS target_concept_name,
    stcm.target_vocabulary_id,
    c2.domain_id            AS target_domain_id,
    c2.concept_class_id     AS target_concept_class_id,
    c2.invalid_reason       AS target_invalid_reason,
    c2.standard_concept     AS target_standard_concept
  FROM ${database_name}.source_to_concept_map stcm
  LEFT JOIN ${database_name}.concept c1
    ON c1.concept_id = stcm.source_concept_id
  LEFT JOIN ${database_name}.concept c2
    ON c2.concept_id = stcm.target_concept_id
  WHERE stcm.invalid_reason IS NULL
)
SELECT * FROM vocab_map;


-- ============================================================================
-- SOURCE TO SOURCE VOCABULARY MAP
-- Self-mapping for source code metadata
-- ============================================================================

CREATE OR REPLACE VIEW ${database_name}.source_to_source_vocab_map AS
WITH vocab_map AS (
  -- Self-map from concept table
  SELECT
    c.concept_code           AS source_code,
    c.concept_id            AS source_concept_id,
    c.concept_name          AS source_code_description,
    c.vocabulary_id         AS source_vocabulary_id,
    c.domain_id             AS source_domain_id,
    c.concept_class_id      AS source_concept_class_id,
    c.valid_start_date      AS source_valid_start_date,
    c.valid_end_date        AS source_valid_end_date,
    c.invalid_reason        AS source_invalid_reason,
    c.concept_id            AS target_concept_id,
    c.concept_name          AS target_concept_name,
    c.vocabulary_id         AS target_vocabulary_id,
    c.domain_id             AS target_domain_id,
    c.concept_class_id      AS target_concept_class_id,
    c.invalid_reason        AS target_invalid_reason,
    c.standard_concept      AS target_standard_concept
  FROM ${database_name}.concept c
  
  UNION ALL
  
  -- Map from source_to_concept_map table
  SELECT
    stcm.source_code,
    stcm.source_concept_id,
    stcm.source_code_description,
    stcm.source_vocabulary_id,
    c1.domain_id            AS source_domain_id,
    c1.concept_class_id     AS source_concept_class_id,
    c1.valid_start_date     AS source_valid_start_date,
    c1.valid_end_date       AS source_valid_end_date,
    stcm.invalid_reason     AS source_invalid_reason,
    stcm.target_concept_id,
    c2.concept_name         AS target_concept_name,
    stcm.target_vocabulary_id,
    c2.domain_id            AS target_domain_id,
    c2.concept_class_id     AS target_concept_class_id,
    c2.invalid_reason       AS target_invalid_reason,
    c2.standard_concept     AS target_standard_concept
  FROM ${database_name}.source_to_concept_map stcm
  LEFT JOIN ${database_name}.concept c1
    ON c1.concept_id = stcm.source_concept_id
  LEFT JOIN ${database_name}.concept c2
    ON c2.concept_id = stcm.target_concept_id
  WHERE stcm.invalid_reason IS NULL
)
SELECT * FROM vocab_map;


-- ============================================================================
-- HELPER VIEWS FOR ETL
-- ============================================================================

-- Gender concept mapping
CREATE OR REPLACE VIEW ${database_name}.gender_concept_lookup AS
SELECT concept_code, concept_id
FROM ${database_name}.concept
WHERE vocabulary_id = 'Gender'
  AND standard_concept = 'S';

-- Race concept mapping
CREATE OR REPLACE VIEW ${database_name}.race_concept_lookup AS
SELECT concept_code, concept_id, concept_name
FROM ${database_name}.concept
WHERE vocabulary_id = 'Race'
  AND standard_concept = 'S';

-- Ethnicity concept mapping
CREATE OR REPLACE VIEW ${database_name}.ethnicity_concept_lookup AS
SELECT concept_code, concept_id, concept_name
FROM ${database_name}.concept
WHERE vocabulary_id = 'Ethnicity'
  AND standard_concept = 'S';

-- Visit type concept mapping
CREATE OR REPLACE VIEW ${database_name}.visit_concept_lookup AS
SELECT concept_code, concept_id, concept_name
FROM ${database_name}.concept
WHERE vocabulary_id = 'Visit'
  AND standard_concept = 'S'
  AND domain_id = 'Visit';

-- Unit concept mapping (UCUM)
CREATE OR REPLACE VIEW ${database_name}.unit_concept_lookup AS
SELECT concept_code, concept_id, concept_name
FROM ${database_name}.concept
WHERE vocabulary_id = 'UCUM'
  AND standard_concept = 'S';
