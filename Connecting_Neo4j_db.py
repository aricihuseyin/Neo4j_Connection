from neo4j import GraphDatabase

# Function to create a Neo4j database connection
def get_neo4j_connection():
    uri = "bolt://localhost:7687"  # Replace with your Neo4j server URI
    user = "neo4j"  # Replace with your Neo4j username
    password = "HsYn1905"  # Replace with your Neo4j password
    return GraphDatabase.driver(uri, auth=(user, password))

# Function to get the top 10 actors by age
def get_top_10_actors_by_age(tx):
    query = (
        "MATCH (actor:Person) "
        "WHERE actor.born IS NOT NULL "
        "WITH actor, date().year - actor.born AS age "
        "WHERE age IS NOT NULL "
        "RETURN actor.name AS name, age AS actor_age "
        "ORDER BY actor_age DESC "
        "LIMIT 10"
    )
    result = tx.run(query)
    return result.data()

# Function to get the top 10 movies by the number of actors
def get_top_10_movies_by_actors(tx):
    query = (
        "MATCH (actor:Person)-[r:ACTED_IN]->(movie:Movie) "
        "RETURN movie.title AS movieTitle, COUNT(actor) AS actorCount "
        "ORDER BY actorCount DESC "  # Burada boşluk ekledim
        "LIMIT 10"
    )
    result = tx.run(query)
    return result.data()


# Function to get the top 10 movies by the number of reviews
def get_top_10_movies_by_reviews(tx):
    query = (
        "MATCH (reviewer)-[:REVIEWED]->(movie:Movie) "
        "RETURN movie.title AS Movie, count(reviewer) AS ReviewCount "
        "ORDER BY ReviewCount DESC "
        "LIMIT 10"
    )
    result = tx.run(query)
    return result.data()

# Function to get the top 10 movies by average rating
def get_top_10_movies_by_rating(tx):
    query = (
        "MATCH (reviewer)-[r:REVIEWED]->(movie:Movie) "
        "WITH movie, AVG(r.rating) AS avgRating "
        "RETURN movie.title AS title, avgRating "
        "ORDER BY avgRating DESC "
        "LIMIT 10"
    )
    result = tx.run(query)
    return result.data()


# Function to get the top 10 persons by the number of movies played and their movie names
def get_top_10_persons_by_movies_played(tx):
    query = (
        "MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie) "
        "RETURN actor.name AS actorName, count(movie) AS movieCount, collect(movie.title) AS movieNames "
        "ORDER BY movieCount DESC "
        "LIMIT 10"
    )
    result = tx.run(query)
    return result.data()

# Main function to execute all queries and display results
def main():
    with get_neo4j_connection() as driver:
        with driver.session() as session:
            print("Top 10 Actors by Age (Descending Order):")
            top_actors_by_age = session.read_transaction(get_top_10_actors_by_age)
            for index, actor in enumerate(top_actors_by_age):
                print(f"{index + 1}. {actor['name']} (Age: {actor['actor_age']})")

            print("\nTop 10 Movies by the Number of Actors:")
            top_movies_by_actors = session.read_transaction(get_top_10_movies_by_actors)
            for index, movie in enumerate(top_movies_by_actors):
                print(f"{index + 1}. {movie['movieTitle']} ({movie['actorCount']} actors)")

            print("\nTop 10 Movies by the Number of Reviews:")
            top_movies_by_reviews = session.read_transaction(get_top_10_movies_by_reviews)
            for index, movie in enumerate(top_movies_by_reviews):
                print(f"{index + 1}. {movie['Movie']} ({movie['ReviewCount']} reviews)")

            print("\nTop 10 Movies by Average Rating:")
            top_movies_by_rating = session.read_transaction(get_top_10_movies_by_rating)
            for index, movie in enumerate(top_movies_by_rating):
                if 'avgRating' in movie and movie['avgRating'] is not None:
                    print(f"{index + 1}. {movie['title']} (Average Rating: {movie['avgRating']:.2f})")

            print("\nTop 10 Persons by the Number of Movies Played (with Movie Names):")
            top_persons_by_movies_played = session.read_transaction(get_top_10_persons_by_movies_played)
            for index, person in enumerate(top_persons_by_movies_played):
                print(f"{index + 1}. {person['actorName']} ({person['movieCount']} movies played):")
                for movie_name in person.get('movieNames', []):
                    print(f"   - {movie_name}")

if __name__ == "__main__":
    main()