// Create indexes
CREATE INDEX ON :User(id);
CREATE INDEX ON :User(status);
CREATE INDEX ON :User(given_name);
CREATE INDEX ON :User(surname);
CREATE INDEX ON :User(password);

CREATE INDEX ON :Organization(id);
CREATE INDEX ON :Organization(name);

CREATE INDEX ON :Nexus(id);
CREATE INDEX ON :Nexus(title);
CREATE INDEX ON :Nexus(start_date);
CREATE INDEX ON :Nexus(end_date);
CREATE INDEX ON :Nexus(upvote);

CREATE INDEX ON :Contributor(id);

CREATE INDEX ON :Tag(id);
CREATE INDEX ON :Tag(name);

CREATE INDEX ON :Badge(id);

CREATE INDEX ON :Country(id);

CREATE INDEX ON :Region(id);
CREATE INDEX ON :Region(name);

CREATE INDEX ON :Province(id);
CREATE INDEX ON :Province(name);

CREATE INDEX ON :City(id);
CREATE INDEX ON :City(name);

// CREATE INDEX ON :Neighborhood(id);
// CREATE INDEX ON :Neighborhood(name);

CREATE INDEX ON :Zipcode(id);

// Create constraints, will also create indexes automatically
CREATE CONSTRAINT ON (user:User) ASSERT user.username IS UNIQUE;
CREATE CONSTRAINT ON (user:User) ASSERT user.email IS UNIQUE;
CREATE CONSTRAINT ON (user:User) ASSERT user.facebook_profile IS UNIQUE;

CREATE CONSTRAINT ON (organization:Organization) ASSERT organization.code IS UNIQUE;

// CREATE CONSTRAINT ON (nexus:Nexus) ASSERT nexus.code IS UNIQUE;

CREATE CONSTRAINT ON (contributor:Contributor) ASSERT contributor.name IS UNIQUE;

CREATE CONSTRAINT ON (tag:tag) ASSERT tag.values IS UNIQUE;

CREATE CONSTRAINT ON (badge:Badge) ASSERT badge.name IS UNIQUE;

CREATE CONSTRAINT ON (country:Country) ASSERT country.code IS UNIQUE;

CREATE CONSTRAINT ON (region:Region) ASSERT region.code IS UNIQUE;

CREATE CONSTRAINT ON (province:Province) ASSERT province.code IS UNIQUE;

CREATE CONSTRAINT ON (city:City) ASSERT city.code IS UNIQUE;

CREATE CONSTRAINT ON (zipcode:Zipcode) ASSERT zipcode.code IS UNIQUE;
