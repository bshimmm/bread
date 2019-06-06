USING PERIODIC COMMIT 10000

// load country
LOAD CSV WITH HEADERS FROM "file:///country.csv" AS line FIELDTERMINATOR '|'
CREATE (:Country {
    id: line.id, country_code: line.code, name: line.name,
    latitude: toFloat(line.latitude), longitude: toFloat(line.longitude),
    created: timestamp(), updated: timestamp()
  }
);

// load province/state
LOAD CSV WITH HEADERS FROM "file:///province.csv" AS line FIELDTERMINATOR '|'
CREATE (:Province {
    id: line.id, code: line.code, name: line.name,
    latitude: toFloat(line.latitude), longitude: toFloat(line.longitude),
    created: timestamp(), updated: timestamp()
  }
);

// load city/town
LOAD CSV WITH HEADERS FROM "file:///city.csv" AS line FIELDTERMINATOR '|'
CREATE (:City {
    id: line.id, name: line.name,
    latitude: toFloat(line.latitude), longitude: toFloat(line.longitude),
    created: timestamp(), updated: timestamp()
  }
);

// load region
LOAD CSV WITH HEADERS FROM "file:///region.csv" AS line FIELDTERMINATOR '|'
CREATE (:Region {
    id: line.id, name: line.name, code: line.code,
    latitude: toFloat(line.latitude), longitude: toFloat(line.longitude),
    created: timestamp(), updated: timestamp()
  }
);

// load zipcode
LOAD CSV WITH HEADERS FROM "file:///zipcode.csv" AS line FIELDTERMINATOR '|'
CREATE (:Zipcode {
    id: line.id, name: line.code,
    latitude: toFloat(line.latitude), longitude: toFloat(line.longitude),
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
MERGE (region)-[:HAS_CITY]->(city);

// load nexus
LOAD CSV WITH HEADERS FROM "file:///nexus.csv" AS line FIELDTERMINATOR '|'
CREATE (:Nexus {
    id: line.id, title: line.title,
    desc: line.desc, start_date: line.start_date, end_date: line.end_date,
    approved: line.approved, status: line.status, upvote: line.upvote,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: nexus -> city
LOAD CSV WITH HEADERS FROM "file:///nexus.csv" AS line FIELDTERMINATOR '|'
MATCH (city:City {id: line.city_id})
MATCH (nexus:Nexus {id: line.id})
MERGE (nexus)-[:IS_LOCATED]->(city);

// create relationship: zipcode -> nexus
LOAD CSV WITH HEADERS FROM "file:///nexus.csv" AS line FIELDTERMINATOR '|'
MATCH (zipcode:Zipcode {id: line.zipcode_id})
MATCH (nexus:Nexus {id: line.id})
MERGE (nexus)-[:IS_LOCATED]->(zipcode);

// load organization
LOAD CSV WITH HEADERS FROM "file:///organization.csv" AS line FIELDTERMINATOR '|'
CREATE (:Organization {
    id: line.id, name: line.name, desc: line.desc, code: line.code,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: city -> organization
LOAD CSV WITH HEADERS FROM "file:///organization.csv" AS line FIELDTERMINATOR '|'
MATCH (city:City {id: line.city_id})
MATCH (organization:Organization {id: line.id})
MERGE (city)-[:HAS_ORG]->(organization);

// load contributor
LOAD CSV WITH HEADERS FROM "file:///contributor.csv" AS line FIELDTERMINATOR '|'
CREATE (:Contributor {
    id: line.id, name: line.name, desc: line.desc,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: nexus -> contributor
LOAD CSV WITH HEADERS FROM "file:///nexus_to_contributor.csv" AS line FIELDTERMINATOR '|'
MATCH (nexus:Nexus {id: line.nexus_id})
MATCH (contributor:Contributor {id: line.contributor_id})
MERGE (nexus)-[:NEED_CONTRIBUTOR]->(contributor);

// load category
LOAD CSV WITH HEADERS FROM "file:///category.csv" AS line FIELDTERMINATOR '|'
CREATE (:Category {
    id: line.id, name: line.name, desc: line.desc,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: nexus -> category
LOAD CSV WITH HEADERS FROM "file:///nexus_to_category.csv" AS line FIELDTERMINATOR '|'
MATCH (nexus:Nexus {id: line.nexus_id})
MATCH (category:Category {id: line.category_id})
MERGE (nexus)-[:IS_CAT]->(category);

// load nexus type
LOAD CSV WITH HEADERS FROM "file:///tag.csv" AS line FIELDTERMINATOR '|'
CREATE (:Tag {
    id: line.id, name: line.name, desc: line.desc,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: nexus -> nexus tag
LOAD CSV WITH HEADERS FROM "file:///nexus_to_tag.csv" AS line FIELDTERMINATOR '|'
MATCH (nexus:Nexus {id: line.nexus_id})
MATCH (tag:Tag {id: line.tag_id})
MERGE (nexus)-[:IS_TAG]->(tag);

// load user
LOAD CSV WITH HEADERS FROM "file:///user.csv" AS line FIELDTERMINATOR '|'
CREATE (:User {
    id: line.id, username: line.username, email: line.email, given_name: line.given_name, surname: line.surname,
    status: line.status, about: line.about, facebook_profile: line.facebook_profile, password: line.password,
    created: timestamp(), updated: timestamp()
  }
);

// load badge
LOAD CSV WITH HEADERS FROM "file:///badge.csv" AS line FIELDTERMINATOR '|'
CREATE (:Badge {
    id: line.id, name: line.name, desc: line.desc,
    created: timestamp(), updated: timestamp()
  }
);

// create relationship: user -> badge
LOAD CSV WITH HEADERS FROM "file:///user.csv" AS line FIELDTERMINATOR '|'
MATCH (user:User {id: line.id})
MATCH (badge:Badge {id: line.badge_id})
MERGE (user)-[:IS_BADGE]->(badge);

// create relationship: user -> organization
LOAD CSV WITH HEADERS FROM "file:///user_to_organization.csv" AS line FIELDTERMINATOR '|'
MATCH (user:User {id: line.user_id})
MATCH (organization:Organization {id: line.org_id})
MERGE (user)-[:PART_OF]->(organization);

// create relationship: user -> contributor
LOAD CSV WITH HEADERS FROM "file:///user_to_contributor.csv" AS line FIELDTERMINATOR '|'
MATCH (user:User {id: line.user_id})
MATCH (contributor:Contributor {id: line.contributor_id})
MERGE (user)-[:IS_CONTRIBUTOR]->(contributor);

// create relationship: user -> city
LOAD CSV WITH HEADERS FROM "file:///user.csv" AS line FIELDTERMINATOR '|'
MATCH (user:User {id: line.id})
MATCH (city:City {id: line.city_id})
MERGE (user)-[:RESIDE_IN]->(city);

// create relationship: user -> zipcode
LOAD CSV WITH HEADERS FROM "file:///user.csv" AS line FIELDTERMINATOR '|'
MATCH (user:User {id: line.id})
MATCH (zipcode:Zipcode {id: line.zipcode_id})
MERGE (user)-[:RESIDE_IN]->(zipcode);
