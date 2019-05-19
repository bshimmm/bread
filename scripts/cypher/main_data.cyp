USING PERIODIC COMMIT 10000

// load country
LOAD CSV WITH HEADERS FROM "file:///country.csv" AS line FIELDTERMINATOR '|'
CREATE (:Country {
    id: line.id, country_code: line.code, name: line.name,
    created: timestamp(), updated: timestamp()
  }
);

// load province/state
LOAD CSV WITH HEADERS FROM "file:///province.csv" AS line FIELDTERMINATOR '|'
CREATE (:Province {
    id: line.id, code: line.code, name: line.name,
    latitude: line.latitude, longitude: line.longitude,
    created: timestamp(), updated: timestamp()
  }
);

// load city/town
LOAD CSV WITH HEADERS FROM "file:///city.csv" AS line FIELDTERMINATOR '|'
CREATE (:City {
    id: line.id, name: line.name,
    latitude: line.latitude, longitude: line.longitude,
    created: timestamp(), updated: timestamp()
  }
);

// load region
LOAD CSV WITH HEADERS FROM "file:///region.csv" AS line FIELDTERMINATOR '|'
CREATE (:Region {
    id: line.id, name: line.name, code: line.code,
    created: timestamp(), updated: timestamp()
  }
);

// load zipcode
LOAD CSV WITH HEADERS FROM "file:///zipcode.csv" AS line FIELDTERMINATOR '|'
CREATE (:Zipcode {
    id: line.id, name: line.code,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: country -> province
LOAD CSV WITH HEADERS FROM "file:///province.csv" AS line FIELDTERMINATOR '|'
MATCH (country:Country {id: line.country_id})
MATCH (province:Province {id: line.id})
MERGE (country)-[:HAS_PROVINCE]->(province);

// create relationship: province -> city/town
LOAD CSV WITH HEADERS FROM "file:///city.csv" AS line FIELDTERMINATOR '|'
MATCH (province:Province {id: line.province_id})
MATCH (city:City {id: line.id})
MERGE (province)-[:HAS_CITY]->(city);

// create relationship: city -> zipcode
LOAD CSV WITH HEADERS FROM "file:///zipcode.csv" AS line FIELDTERMINATOR '|'
MATCH (city:City {id: line.city_id})
MATCH (zipcode:Zipcode {id: line.id})
MERGE (city)-[:HAS_ZIPCODE]->(zipcode);

// create relationship: region -> city
LOAD CSV WITH HEADERS FROM "file:///region.csv" AS line FIELDTERMINATOR '|'
MATCH (region:Region {id: line.id})
MATCH (city:City {id: line.city_id})
MERGE (region)-[:HAS_REGCITY]->(city);

// load nexus
LOAD CSV WITH HEADERS FROM "file:///nexus.csv" AS line FIELDTERMINATOR '|'
CREATE (:Nexus {
    id: line.id, name: line.name,
    latitude: line.latitude, longitude: line.longitude,
    created: timestamp(), updated: timestamp()
  }
);
