from neo4j import GraphDatabase, RoutingControl

uri = "neo4j+s://dcbd9a72.databases.neo4j.io:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "7xMq5ln--6oseUM0ErqfKMfLI3ijDaA3k46e_R0P0w0"))

class GraphDBConnector:
    def set_constraints():
        unique_email_constraint = """
        CREATE CONSTRAINT unique_email IF NOT EXISTS
        FOR (u:User)
        REQUIRE (u.email) IS UNIQUE
        """

        unique_sport_name_constraint = """
        CREATE CONSTRAINT unique_sport_name IF NOT EXISTS
        FOR (s:Sport)
        REQUIRE (s.sport_name) IS UNIQUE
        """

        unique_genre_name_constraint = """
        CREATE CONSTRAINT unique_genre_name IF NOT EXISTS
        FOR (g:Movie_Genre)
        REQUIRE (g.genre_name) IS UNIQUE
        """

        constraints = [
            unique_email_constraint,
            unique_sport_name_constraint,
            unique_genre_name_constraint
        ]

        try:
            with driver.session() as session:
                for query in constraints:
                    session.run(query)
        except Exception as e:
            print(f"Problem adding constraint:\n\n{e}\n")
    
    def add_user(email, firstname, lastname):
        try:
            query = """
            CREATE (u:User {
                email: $email,
                firstname: $firstname,
                lastname: $lastname
            })
            RETURN u
            """
            with driver.session() as session:
                result = session.run(
                    query, email=email, firstname=firstname, lastname=lastname
                )
                user_node = result.single()
                return user_node
        except Exception as e:
            print(f"Failed to add User:\n\n{e}\n")

    def add_sport(sport_name):
        try:
            query = """
            CREATE (s:Sport {
                sport_name: $sport_name
            })
            RETURN s
            """
            with driver.session() as session:
                result = session.run(
                    query, sport_name=sport_name
                )
                user_node = result.single()
                return user_node
        except Exception as e:
            print(f"Failed to add Sport:\n\n{e}\n")
    
    def add_movie_genre(genre_name):
        try:
            query = """
            CREATE (g:MovieGenre {
                genre_name: $genre_name
            })
            RETURN g
            """
            with driver.session() as session:
                result = session.run(
                    query, genre_name=genre_name
                )
                user_node = result.single()
                return user_node
        except Exception as e:
            print(f"Failed to add MovieGenre:\n\n{e}\n")

    def connect_user_to_sport(email, sport_name, relationship_type="PLAYS"):
        try:
            query = """
            MATCH (u:User {email: $email})
            MATCH (s:Sport {sport_name: $sport_name})
            CREATE (u)-[r:%s]->(s)
            RETURN u, r, s
            """ % relationship_type
            with driver.session() as session:
                result = session.run(
                    query, email=email, sport_name=sport_name
                )
                connections = result.single()
                return connections
        except Exception as e:
            print(f"Failed to connect user to sport\n\n{e}\n")

    def connect_user_to_movie_genre(email, genre_name, relationship_type="LIKES_WATCHING"):
        try:
            query = """
            MATCH (u:User {email: $email})
            MATCH (m:MovieGenre {genre_name: $genre_name})
            CREATE (u)-[r:%s]->(m)
            RETURN u, r, m
            """ % relationship_type
            with driver.session() as session:
                result = session.run(
                    query, email=email, genre_name=genre_name
                )
                connections = result.single()
                return connections
        except Exception as e:
            print(f"Failed to connect user to sport\n\n{e}\n")

    def find_sport_connections(email):
        try:
            query = """
            MATCH (currentUser:User {email: $email})-[:PLAYS]->(sport)<-[:PLAYS]-(otherUser:User)
            WHERE currentUser <> otherUser
            WITH otherUser, COUNT(sport) AS sharedSports
            RETURN otherUser.email AS similarUserEmail, sharedSports
            ORDER BY sharedSports DESC
            LIMIT 10
            """
            with driver.session() as session:
                result = session.run(query, email=email)
                res = [(record["similarUserEmail"], record["sharedSports"]) for record in result]
                return res
        except Exception as e:
            print(f"Failed to get connections for Users based on sports:\n\n{e}\n")

    def find_movie_genre_connections(email):
        try:
            query = """
            MATCH (currentUser:User {email: $email})-[:LIKES_WATCHING]->(movie_genre)<-[:LIKES_WATCHING]-(otherUser:User)
            WHERE currentUser <> otherUser
            WITH otherUser, COUNT(movie_genre) AS sharedMovieGenres
            RETURN otherUser.email AS similarUserEmail, sharedMovieGenres
            ORDER BY sharedMovieGenres DESC
            LIMIT 10
            """
            with driver.session() as session:
                result = session.run(query, email=email)
                res = [(record["similarUserEmail"], record["sharedMovieGenres"]) for record in result]
                return res
        except Exception as e:
            print(f"Failed to get connections for Movie Genres based on sports:\n\n{e}\n")

    def find_global_connections(email):
        scores = {}

        sport_similarity = GraphDBConnector.find_sport_connections(email)
        for otherEmail, similarity in sport_similarity:
            scores[otherEmail] = scores.get(otherEmail, 0) + similarity
        
        movie_genre_similarity = GraphDBConnector.find_movie_genre_connections(email)
        for otherEmail, similarity in movie_genre_similarity:
            scores[otherEmail] = scores.get(otherEmail, 0) + similarity

        res = [ (otherEmail, score) for otherEmail, score in scores.items() ]
        res.sort(key=lambda x: x[1], reverse=True)

        return [ otherEmail for otherEmail, score in res ]


david = ("david@htn.ca", "BC", "gang")
shraddha = ("shraddha@htn.ca", "from", "bengaluru")
lucas = ("lucas@htn.ca", "grade", "11")
krishna = ("krishna@htn.ca", "too", "old")

users = [ david, shraddha, lucas, krishna ]
sport_names = [ "Hockey", "Baseball", "Tennis", "Volleyball", "Rugby", "Table Tennis", "Golf" ]
movie_genres = [ "Action", "Drama", "Horror", "Fantasy", "Romance", "Adventure" ]

# GraphDBConnector.set_constraints()

# Creating Nodes

# for user in users: 
#     GraphDBConnector.add_user(user[0], user[1], user[2])

# for sport_name in sport_names: 
#     GraphDBConnector.add_sport(sport_name)

# for genre_name in movie_genres: 
#     GraphDBConnector.add_movie_genre(genre_name)

# Adding Connections

# David
# GraphDBConnector.connect_user_to_sport(david[0], sport_names[0])
# GraphDBConnector.connect_user_to_sport(david[0], sport_names[1])
# GraphDBConnector.connect_user_to_sport(david[0], sport_names[2])
# GraphDBConnector.connect_user_to_movie_genre(david[0], movie_genres[0])
# GraphDBConnector.connect_user_to_movie_genre(david[0], movie_genres[1])
# GraphDBConnector.connect_user_to_movie_genre(david[0], movie_genres[2])

# Lucas
# GraphDBConnector.connect_user_to_sport(lucas[0], sport_names[0])
# GraphDBConnector.connect_user_to_sport(lucas[0], sport_names[1])
# GraphDBConnector.connect_user_to_sport(lucas[0], sport_names[2])
# GraphDBConnector.connect_user_to_movie_genre(lucas[0], movie_genres[0])
# GraphDBConnector.connect_user_to_movie_genre(lucas[0], movie_genres[1])
# GraphDBConnector.connect_user_to_movie_genre(lucas[0], movie_genres[5])

# Shraddha
# GraphDBConnector.connect_user_to_sport(shraddha[0], sport_names[2])
# GraphDBConnector.connect_user_to_sport(shraddha[0], sport_names[3])
# GraphDBConnector.connect_user_to_sport(shraddha[0], sport_names[4])
# GraphDBConnector.connect_user_to_movie_genre(shraddha[0], movie_genres[2])
# GraphDBConnector.connect_user_to_movie_genre(shraddha[0], movie_genres[3])
# GraphDBConnector.connect_user_to_movie_genre(shraddha[0], movie_genres[4])

# Krishna
# GraphDBConnector.connect_user_to_sport(krishna[0], sport_names[3])
# GraphDBConnector.connect_user_to_sport(krishna[0], sport_names[4])
# GraphDBConnector.connect_user_to_sport(krishna[0], sport_names[5])
# GraphDBConnector.connect_user_to_movie_genre(krishna[0], movie_genres[3])
# GraphDBConnector.connect_user_to_movie_genre(krishna[0], movie_genres[4])
# GraphDBConnector.connect_user_to_movie_genre(krishna[0], movie_genres[5])

# Getting Data

# David
print(GraphDBConnector.find_sport_connections(david[0]))
print(GraphDBConnector.find_movie_genre_connections(david[0]))
print(GraphDBConnector.find_global_connections(david[0]))

# Lucas

# Shraddha

# Krishna


driver.close()
