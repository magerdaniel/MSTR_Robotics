-- Add ontology columns to lu_cust_region
ALTER TABLE lu_cust_region ADD COLUMN ontology_id VARCHAR(500);

ALTER TABLE lu_cust_region ADD COLUMN ontology_uri VARCHAR(500);

-- Populate ontology from Wikidata columns
UPDATE lu_cust_region SET ontology_id = 'Q24460' , ontology_uri = 'http://www.wikidata.org/entity/Q24460' WHERE cust_region_id = 1;
UPDATE lu_cust_region SET ontology_id = 'Q632014' , ontology_uri = 'http://www.wikidata.org/entity/Q632014' WHERE cust_region_id = 2;
UPDATE lu_cust_region SET ontology_id = 'Q1139046' , ontology_uri = 'http://www.wikidata.org/entity/Q1139046' WHERE cust_region_id = 3;
UPDATE lu_cust_region SET ontology_id = 'Q1140343' , ontology_uri = 'http://www.wikidata.org/entity/Q1140343' WHERE cust_region_id = 4;
UPDATE lu_cust_region SET ontology_id = 'Q49042' , ontology_uri = 'http://www.wikidata.org/entity/Q49042' WHERE cust_region_id = 5;
UPDATE lu_cust_region SET ontology_id = 'Q944857' , ontology_uri = 'http://www.wikidata.org/entity/Q944857' WHERE cust_region_id = 6;
UPDATE lu_cust_region SET ontology_id = 'Q858847' , ontology_uri = 'http://www.wikidata.org/entity/Q858847' WHERE cust_region_id = 7;