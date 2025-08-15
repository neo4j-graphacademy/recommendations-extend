:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'recommendations-nodes-Person.csv',
  file_1: 'recommendations-nodes-Movie.csv',
  file_2: 'recommendations-nodes-User.csv',
  file_3: 'recommendations-nodes-Genre.csv',
  file_4: 'recommendations-relationships-ACTED_IN.csv',
  file_5: 'recommendations-relationships-DIRECTED.csv',
  file_6: 'recommendations-relationships-RATED.csv',
  file_7: 'recommendations-relationships-IN_GENRE.csv',
  file_8: 'Customer.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 5.27.0.
CREATE CONSTRAINT `tmdbId_Person_uniq` IF NOT EXISTS
FOR (n: `Person`)
REQUIRE (n.`tmdbId`) IS UNIQUE;
CREATE CONSTRAINT `movieId_Movie_uniq` IF NOT EXISTS
FOR (n: `Movie`)
REQUIRE (n.`movieId`) IS UNIQUE;
CREATE CONSTRAINT `userId_User_uniq` IF NOT EXISTS
FOR (n: `User`)
REQUIRE (n.`userId`) IS UNIQUE;
CREATE CONSTRAINT `name_Genre_uniq` IF NOT EXISTS
FOR (n: `Genre`)
REQUIRE (n.`name`) IS UNIQUE;


// INDEX creation
// -------------------
//
// Create node indexes
//
CREATE INDEX `name_Person` IF NOT EXISTS
FOR (n: `Person`)
ON (n.`name`);
CREATE INDEX `tmdbId_Movie` IF NOT EXISTS
FOR (n: `Movie`)
ON (n.`tmdbId`);
CREATE INDEX `released_Movie` IF NOT EXISTS
FOR (n: `Movie`)
ON (n.`released`);
CREATE INDEX `imdbRating_Movie` IF NOT EXISTS
FOR (n: `Movie`)
ON (n.`imdbRating`);
CREATE INDEX `title_Movie` IF NOT EXISTS
FOR (n: `Movie`)
ON (n.`title`);
CREATE INDEX `year_Movie` IF NOT EXISTS
FOR (n: `Movie`)
ON (n.`year`);
CREATE INDEX `imdbId_Movie` IF NOT EXISTS
FOR (n: `Movie`)
ON (n.`imdbId`);
CREATE INDEX `name_User` IF NOT EXISTS
FOR (n: `User`)
ON (n.`name`);

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`tmdbId` IN $idsToSkip AND NOT toInteger(trim(row.`tmdbId`)) IS NULL
CALL (row) {
  MERGE (n: `Person` { `tmdbId`: toInteger(trim(row.`tmdbId`)) })
  SET n.`tmdbId` = toInteger(trim(row.`tmdbId`))
  SET n.`name` = row.`name`
  SET n.`bornIn` = row.`bornIn`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`born` = datetime(row.`born`)
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`died` = datetime(row.`died`)
  SET n.`imdbId` = toInteger(trim(row.`imdbId`))
  SET n.`url` = row.`url`
  SET n.`bio` = row.`bio`
  SET n.`poster` = row.`poster`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`movieId` IN $idsToSkip AND NOT toInteger(trim(row.`movieId`)) IS NULL
CALL (row) {
  MERGE (n: `Movie` { `movieId`: toInteger(trim(row.`movieId`)) })
  SET n.`movieId` = toInteger(trim(row.`movieId`))
  SET n.`title` = row.`title`
  SET n.`url` = row.`url`
  SET n.`runtime` = toInteger(trim(row.`runtime`))
  SET n.`revenue` = toInteger(trim(row.`revenue`))
  SET n.`budget` = toInteger(trim(row.`budget`))
  SET n.`imdbRating` = toFloat(trim(row.`imdbRating`))
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`released` = datetime(row.`released`)
  SET n.`countries` = row.`countries`
  SET n.`languages` = row.`languages`
  SET n.`plot` = row.`plot`
  SET n.`imdbVotes` = toInteger(trim(row.`imdbVotes`))
  SET n.`imdbId` = toInteger(trim(row.`imdbId`))
  SET n.`year` = toInteger(trim(row.`year`))
  SET n.`poster` = row.`poster`
  SET n.`tmdbId` = toInteger(trim(row.`tmdbId`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`userId` IN $idsToSkip AND NOT toInteger(trim(row.`userId`)) IS NULL
CALL (row) {
  MERGE (n: `User` { `userId`: toInteger(trim(row.`userId`)) })
  SET n.`userId` = toInteger(trim(row.`userId`))
  SET n.`name` = row.`name`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row
WHERE NOT row.`name` IN $idsToSkip AND NOT row.`name` IS NULL
CALL (row) {
  MERGE (n: `Genre` { `name`: row.`name` })
  SET n.`name` = row.`name`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL (row) {
  MATCH (source: `Person` { `tmdbId`: toInteger(trim(row.`person_tmdbId`)) })
  MATCH (target: `Movie` { `movieId`: toInteger(trim(row.`movie_movieId`)) })
  MERGE (source)-[r: `ACTED_IN`]->(target)
  SET r.`role` = row.`role`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_5) AS row
WITH row 
CALL (row) {
  MATCH (source: `Person` { `tmdbId`: toInteger(trim(row.`person_tmdbId`)) })
  MATCH (target: `Movie` { `movieId`: toInteger(trim(row.`movie_movieId`)) })
  MERGE (source)-[r: `DIRECTED`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_6) AS row
WITH row 
CALL (row) {
  MATCH (source: `User` { `userId`: toInteger(trim(row.`user_userId`)) })
  MATCH (target: `Movie` { `movieId`: toInteger(trim(row.`movie_movieId`)) })
  MERGE (source)-[r: `RATED`]->(target)
  SET r.`rating` = toFloat(trim(row.`rating`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_7) AS row
WITH row 
CALL (row) {
  MATCH (source: `Movie` { `movieId`: toInteger(trim(row.`movie_movieId`)) })
  MATCH (target: `Genre` { `name`: row.`genre_name` })
  MERGE (source)-[r: `IN_GENRE`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

MATCH (p:Person)-[:ACTED_IN]-()
SET p:Actor;

MATCH (p:Person)-[:DIRECTED]-()
SET p:Director;



LOAD CSV WITH HEADERS FROM ($file_path_root + $file_8) AS row
WITH row
WHERE NOT row.`customerId` IN $idsToSkip AND NOT row.`customerId` IS NULL
CALL (row) {
  MERGE (n: `Customer` { `customerId`: row.`customerId` })
  SET n.`name` = row.`name`,
    n.`email` = row.`email`,
    n.`phone` = row.`phone`,
    n.`address` = row.`address`
} IN TRANSACTIONS OF 10000 ROWS;