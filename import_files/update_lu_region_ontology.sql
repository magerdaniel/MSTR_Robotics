-- Add ontology columns to LU_REGION
ALTER TABLE LU_REGION ADD COLUMN ontology_id VARCHAR(500);

ALTER TABLE LU_REGION ADD COLUMN ontology_uri VARCHAR(500);

-- Populate ontology from Wikidata columns
UPDATE LU_REGION SET ontology_id = 'Q24460' , ontology_uri = 'http://www.wikidata.org/entity/Q24460' WHERE region_id = 1;
UPDATE LU_REGION SET ontology_id = 'Q632014' , ontology_uri = 'http://www.wikidata.org/entity/Q632014' WHERE region_id = 2;
UPDATE LU_REGION SET ontology_id = 'Q1139046' , ontology_uri = 'http://www.wikidata.org/entity/Q1139046' WHERE region_id = 3;
UPDATE LU_REGION SET ontology_id = 'Q1140343' , ontology_uri = 'http://www.wikidata.org/entity/Q1140343' WHERE region_id = 4;
UPDATE LU_REGION SET ontology_id = 'Q49042' , ontology_uri = 'http://www.wikidata.org/entity/Q49042' WHERE region_id = 5;
UPDATE LU_REGION SET ontology_id = 'Q944857' , ontology_uri = 'http://www.wikidata.org/entity/Q944857' WHERE region_id = 6;
UPDATE LU_REGION SET ontology_id = 'Q858847' , ontology_uri = 'http://www.wikidata.org/entity/Q858847' WHERE region_id = 7;
UPDATE LU_REGION SET ontology_id = 'No Ontology available' , ontology_uri = 'No_ontology' WHERE region_id = 12;