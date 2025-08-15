// Generate the CSV files from recommendations dataset
WITH "MATCH (p:Person)
      RETURN p.tmdbId as tmdbId, p.name AS name, p.bornIn AS bornIn, p.born AS born, p.died AS died, p.imdbId AS imdbId, p.url AS url, p.bio as bio, p.poster AS poster" AS query
CALL apoc.export.csv.query(query, "recommendations-nodes-Person.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (m:Movie)
      RETURN m.movieId AS movieId, m.title AS title, m.url AS url, m.runtime AS runtime, m.revenue AS revenue, m.budget AS budget, m.imdbRating AS imdbRating, m.released AS released, m.countries AS countries, m.languages AS languages, m.plot AS plot, m.imdbVotes AS imdbVotes, m.imdbId AS imdbId, m.year AS year, m.poster AS poster,  m.tmdbId AS tmdbId" AS query
CALL apoc.export.csv.query(query, "recommendations-nodes-Movie.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (u:User)
      RETURN u.userId AS userId, u.name AS name" AS query
CALL apoc.export.csv.query(query, "recommendations-nodes-User.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (g:Genre)
      RETURN g.name AS name" AS query
CALL apoc.export.csv.query(query, "recommendations-nodes-Genre.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
      RETURN p.tmdbId AS person_tmdbId, m.movieId AS movie_movieId, a.role AS role" AS query
CALL apoc.export.csv.query(query, "recommendations-relationships-ACTED_IN.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (p:Person)-[a:DIRECTED]->(m:Movie)
      RETURN p.tmdbId AS person_tmdbId, m.movieId AS movie_movieId" AS query
CALL apoc.export.csv.query(query, "recommendations-relationships-DIRECTED.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
      RETURN m.movieId AS movie_movieId, g.name AS genre_name" AS query
CALL apoc.export.csv.query(query, "recommendations-relationships-IN_GENRE.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;

WITH "MATCH (u:User)-[r:RATED]->(m:Movie)
      RETURN u.userId AS user_userId, m.movieId AS movie_movieId, r.rating AS rating" AS query
CALL apoc.export.csv.query(query, "recommendations-relationships-RATED.csv", {})
YIELD file, nodes, relationships, properties, data
RETURN file, nodes, relationships, properties, data;