:param {
  // Define the file path root and the individual file names required for loading.
  // file_path_root:'file:///', Use this for local import
  file_path_root:'https://raw.githubusercontent.com/neo4j-graphacademy/recommendations-extend/refs/heads/main/recommendations-extended/data/csv/', 
  file_person:'recommendations-nodes-Person.csv',
  file_movie:'recommendations-nodes-Movie.csv',
  file_user:'recommendations-nodes-User.csv',
  file_genre:'recommendations-nodes-Genre.csv',
  file_acted_in:'recommendations-relationships-ACTED_IN.csv',
  file_directed:'recommendations-relationships-DIRECTED.csv',
  file_rated:'recommendations-relationships-RATED.csv',
  file_in_genre:'recommendations-relationships-IN_GENRE.csv'
};

// CONSTRAINT creation
// -------------------
CREATE CONSTRAINT tmdbId_Person_uniq IF NOT EXISTS
FOR (n:Person)
REQUIRE (n.tmdbId) IS UNIQUE;
CREATE CONSTRAINT movieId_Movie_uniq IF NOT EXISTS
FOR (n:Movie)
REQUIRE (n.movieId) IS UNIQUE;
CREATE CONSTRAINT userId_User_uniq IF NOT EXISTS
FOR (n:User)
REQUIRE (n.userId) IS UNIQUE;
CREATE CONSTRAINT name_Genre_uniq IF NOT EXISTS
FOR (n:Genre)
REQUIRE (n.name) IS UNIQUE;


// INDEX creation
// -------------------
CREATE INDEX name_Person IF NOT EXISTS
FOR (n:Person)
ON (n.name);
CREATE INDEX tmdbId_Movie IF NOT EXISTS
FOR (n:Movie)
ON (n.tmdbId);
CREATE INDEX released_Movie IF NOT EXISTS
FOR (n:Movie)
ON (n.released);
CREATE INDEX imdbRating_Movie IF NOT EXISTS
FOR (n:Movie)
ON (n.imdbRating);
CREATE INDEX title_Movie IF NOT EXISTS
FOR (n:Movie)
ON (n.title);
CREATE INDEX year_Movie IF NOT EXISTS
FOR (n:Movie)
ON (n.year);
CREATE INDEX imdbId_Movie IF NOT EXISTS
FOR (n:Movie)
ON (n.imdbId);
CREATE INDEX name_User IF NOT EXISTS
FOR (n:User)
ON (n.name);

// NODE load
// ---------
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_person) AS row
WITH row
CALL (row) {
  MERGE (n:Person { tmdbId:toInteger(trim(row.tmdbId)) })
  SET n.tmdbId = toInteger(trim(row.tmdbId))
  SET n.name = row.name
  SET n.bornIn = row.bornIn
  SET n.born = CASE row.born WHEN "" THEN null ELSE date(row.born) END
  SET n.died = CASE row.died WHEN "" THEN null ELSE date(row.died) END
  SET n.imdbId = toInteger(trim(row.imdbId))
  SET n.url = row.url
  SET n.bio = row.bio
  SET n.poster = row.poster
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_movie) AS row
WITH row
CALL (row) {
  MERGE (n:Movie { movieId:toInteger(trim(row.movieId)) })
  SET n.movieId = toInteger(trim(row.movieId))
  SET n.title = row.title
  SET n.url = row.url
  SET n.runtime = toInteger(trim(row.runtime))
  SET n.revenue = toInteger(trim(row.revenue))
  SET n.budget = toInteger(trim(row.budget))
  SET n.imdbRating = toFloat(trim(row.imdbRating))
  SET n.released = row.released
  SET n.countries = row.countries
  SET n.languages = row.languages
  SET n.plot = row.plot
  SET n.imdbVotes = toInteger(trim(row.imdbVotes))
  SET n.imdbId = toInteger(trim(row.imdbId))
  SET n.year = toInteger(trim(row.year))
  SET n.poster = row.poster
  SET n.tmdbId = toInteger(trim(row.tmdbId))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_user) AS row
WITH row
CALL (row) {
  MERGE (n:User { userId:toInteger(trim(row.userId)) })
  SET n.userId = toInteger(trim(row.userId))
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_genre) AS row
WITH row
CALL (row) {
  MERGE (n:Genre { name:row.name })
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_acted_in) AS row
WITH row 
CALL (row) {
  MATCH (source:Person { tmdbId:toInteger(trim(row.person_tmdbId)) })
  MATCH (target:Movie { movieId:toInteger(trim(row.movie_movieId)) })
  MERGE (source)-[r:ACTED_IN]->(target)
  SET r.role = row.role
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_directed) AS row
WITH row 
CALL (row) {
  MATCH (source:Person { tmdbId:toInteger(trim(row.person_tmdbId)) })
  MATCH (target:Movie { movieId:toInteger(trim(row.movie_movieId)) })
  MERGE (source)-[r:DIRECTED]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_rated) AS row
WITH row 
CALL (row) {
  MATCH (source:User { userId:toInteger(trim(row.user_userId)) })
  MATCH (target:Movie { movieId:toInteger(trim(row.movie_movieId)) })
  MERGE (source)-[r:RATED]->(target)
  SET r.rating = toFloat(trim(row.rating))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_in_genre) AS row
WITH row 
CALL (row) {
  MATCH (source:Movie { movieId:toInteger(trim(row.movie_movieId)) })
  MATCH (target:Genre { name:row.genre_name })
  MERGE (source)-[r:IN_GENRE]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

MATCH (p:Person)-[:ACTED_IN]-()
SET p:Actor;

MATCH (p:Person)-[:DIRECTED]-()
SET p:Director;
