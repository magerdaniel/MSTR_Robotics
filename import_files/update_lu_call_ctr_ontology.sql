-- Add ontology columns to LU_CALL_CTR
ALTER TABLE LU_CALL_CTR ADD COLUMN ontology_id VARCHAR(500);

ALTER TABLE LU_CALL_CTR ADD COLUMN ontology_uri VARCHAR(500);

-- Populate ontology from Wikidata columns
UPDATE LU_CALL_CTR SET ontology_id = 'Q23556' , ontology_uri = 'http://www.wikidata.org/entity/Q23556' WHERE call_ctr_id = 1;
UPDATE LU_CALL_CTR SET ontology_id = 'Q16552' , ontology_uri = 'http://www.wikidata.org/entity/Q16552' WHERE call_ctr_id = 2;
UPDATE LU_CALL_CTR SET ontology_id = 'Q62' , ontology_uri = 'http://www.wikidata.org/entity/Q62' WHERE call_ctr_id = 4;
UPDATE LU_CALL_CTR SET ontology_id = 'Q61' , ontology_uri = 'http://www.wikidata.org/entity/Q61' WHERE call_ctr_id = 5;
UPDATE LU_CALL_CTR SET ontology_id = 'Q23337' , ontology_uri = 'http://www.wikidata.org/entity/Q23337' WHERE call_ctr_id = 6;
UPDATE LU_CALL_CTR SET ontology_id = 'Q8652' , ontology_uri = 'http://www.wikidata.org/entity/Q8652' WHERE call_ctr_id = 7;
UPDATE LU_CALL_CTR SET ontology_id = 'Q37836' , ontology_uri = 'http://www.wikidata.org/entity/Q37836' WHERE call_ctr_id = 8;
UPDATE LU_CALL_CTR SET ontology_id = 'Q34404' , ontology_uri = 'http://www.wikidata.org/entity/Q34404' WHERE call_ctr_id = 9;
UPDATE LU_CALL_CTR SET ontology_id = 'Q5083' , ontology_uri = 'http://www.wikidata.org/entity/Q5083' WHERE call_ctr_id = 10;
UPDATE LU_CALL_CTR SET ontology_id = 'Q100' , ontology_uri = 'http://www.wikidata.org/entity/Q100' WHERE call_ctr_id = 11;
UPDATE LU_CALL_CTR SET ontology_id = 'Q60' , ontology_uri = 'http://www.wikidata.org/entity/Q60' WHERE call_ctr_id = 12;
UPDATE LU_CALL_CTR SET ontology_id = 'Q34109' , ontology_uri = 'http://www.wikidata.org/entity/Q34109' WHERE call_ctr_id = 14;
UPDATE LU_CALL_CTR SET ontology_id = 'Q16563' , ontology_uri = 'http://www.wikidata.org/entity/Q16563' WHERE call_ctr_id = 15;
UPDATE LU_CALL_CTR SET ontology_id = 'Q47716' , ontology_uri = 'http://www.wikidata.org/entity/Q47716' WHERE call_ctr_id = 17;
UPDATE LU_CALL_CTR SET ontology_id = 'No Ontology available' , ontology_uri = 'No_ontology' WHERE call_ctr_id = 18;