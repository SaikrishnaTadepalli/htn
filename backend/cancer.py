from neo4j import GraphDatabase, RoutingControl

uri = "neo4j+s://dcbd9a72.databases.neo4j.io:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "7xMq5ln--6oseUM0ErqfKMfLI3ijDaA3k46e_R0P0w0"))


def create_uniqueness_constraints():
    try:
        with driver.session() as session:
            query = """
            CREATE CONSTRAINT unique_email IF NOT EXISTS
            FOR (u:User)
            REQUIRE (u.email) IS UNIQUE
            """
            session.run(query)
    except:
        print("Something wrong with constrainting unique email")
        
    try:
        with driver.session() as session:
            query = """
            CREATE CONSTRAINT unique_sport_name IF NOT EXISTS
            FOR (s:Sport)
            REQUIRE (s.sport_name) IS UNIQUE
            """
            session.run(query)
    except Exception as e:
        print("Something wrong with constrainting sport_name")
    
    try:
        with driver.session() as session:
            query = """
            CREATE CONSTRAINT unique_genre_name IF NOT EXISTS
            FOR (g:Movie_Genre)
            REQUIRE (g.genre_name) IS UNIQUE
            """
            session.run(query)
    except Exception as e:
        print("Something wrong with constrainting genre_name")



def create_genre(genre_name):
    try:
        with driver.session() as session:
            query = """
            CREATE (g:Movie_Genre {
                genre_name: $genre_name
            })
            RETURN g
            """
            result = session.run(query, genre_name=genre_name)
            movie_genre_node = result.single()  # Get the created node (optional)
            return movie_genre_node
    except Exception as e:
        return None


def create_sport(sport_name):
    try:
        # Inserting
        with driver.session() as session:
            query = """
            CREATE (s:Sport {
                sport_name: $sport_name
            })
            RETURN s
            """
            result = session.run(query, sport_name=sport_name)
            sport_node = result.single()  # Get the created node (optional)
            return sport_node
    except Exception as e:
        return None
    
def create_user(email, firstname, lastname):
    try:
        with driver.session() as session:
            query = """
            CREATE (u:User {
                email: $email,
                firstname: $firstname,
                lastname: $lastname
            })
            RETURN u
            """
            result = session.run(query, email=email, firstname=firstname, lastname=lastname)
            user_node = result.single()  # Get the created node (optional)
            return user_node
    except Exception as e:
        return None
    
def connect_user_to_sport(email, sport_name, relationship_type):
    try:
        with driver.session() as session:
            query = """
            MATCH (u:User {email: $email})
            MATCH (s:Sport {sport_name: $sport_name})
            CREATE (u)-[r:%s]->(s)
            RETURN u, r, s
            """ % relationship_type  # You can replace %s with a specific relationship type if needed
            
            result = session.run(query, email=email, sport_name=sport_name)
            connection = result.single()  # Get the created relationship and nodes (optional)
            return connection
    except Exception as e:
        return None
    
def connect_user_to_genre(email, genre_name, relationship_type):
    try:
        with driver.session() as session:
            query = """
            MATCH (u:User {email: $email})
            MATCH (g:Movie_Genre {genre_name: $genre_name})
            CREATE (u)-[r:%s]->(g)
            RETURN u, r, g
            """ % relationship_type  # You can replace %s with a specific relationship type if needed
            
            result = session.run(query, email=email, genre_name=genre_name)
            connection = result.single()  # Get the created relationship and nodes (optional)
            return connection
    except Exception as e:
        print(f"died 12: {e}")
        return None
    
def build_test_graph1():
    create_uniqueness_constraints()

    h = "Hockey"
    b = "Baseball"
    t = "Tennis"
    v = "Volleyball"
    r = "Rugby"
    tt = "Table Tennis"

    D = "david@htn.com"
    L = "lucas@htn.com"
    K = "krishna@htn.com"
    S = "shrad@htn.com"

    sports = [h, b, t, v, r, tt]

    for sport in sports:
        create_sport(sport)

    david = create_user(D, "from", "BC")
    lucas = create_user(L, "grade", "11")
    krishna = create_user(K, "dont", "suspend_me")
    shrad = create_user(S, "from", "bengaluru")

    D_group = [D, [h, b, t]]
    L_group = [L, [h, b, t]]
    K_group = [K, [v, r, tt]]
    S_group = [S, [t, v, r]]

    all_groups = [D_group, L_group, K_group, S_group]

    for name, group in all_groups:
        for g in group:
            connect_user_to_sport(name, g, "PLAYS")

def get_connections_for_graph1(user_email):
    query = """
    MATCH (currentUser:User {email: $email})-[:PLAYS]->(sport)<-[:PLAYS]-(otherUser:User)
    WHERE currentUser <> otherUser
    WITH otherUser, COUNT(sport) AS sharedSports
    RETURN otherUser.email AS similarUserEmail, sharedSports
    ORDER BY sharedSports DESC
    LIMIT 10
    """
    with driver.session() as session:
        result = session.run(query, email=user_email)
        res = [(record["similarUserEmail"], record["sharedSports"]) for record in result]
        print(res)
        return res

def get_connections_for_graph1_driver():
    print("David:")
    get_connections_for_graph1("david@htn.com")
    print("\nLucas:")
    get_connections_for_graph1("lucas@htn.com")
    print("\nKrishna")
    get_connections_for_graph1("krishna@htn.com")
    print("\nShrad")
    get_connections_for_graph1("shrad@htn.com")



def add_movie_genre_graph():
    a = "Action"
    d = "Drama"
    h = "Horror"
    f = "Fantasy"
    r = "Romance"

    D = "david@htn.com"
    L = "lucas@htn.com"
    K = "krishna@htn.com"
    S = "shrad@htn.com"

    grp = [a, d, h, f, r]

    for g in grp:
        create_genre(g)

    D_group = [D, [a, d]]
    L_group = [L, [h, f, r]]
    K_group = [K, [a, f]]
    S_group = [S, [f, r]]

    all_groups = [D_group, L_group, K_group, S_group]

    for name, group in all_groups:
        for g in group:
            print(f"{name} LIKES {g}")
            connect_user_to_genre(name, g, "LIKES")



def sport_plus_movie_connections():
    try:
        with driver.session() as session:
            query = (
                """
        MATCH (u:User {email: $email})-[:PLAYS]->(s:Sport)<-[:PLAYS]-(other:User),
              (u)-[:LIKES]->(g:Movie_Genre)<-[:LIKES]-(other)
        WITH other, COUNT(DISTINCT s) AS sports_count, COUNT(DISTINCT g) AS genres_count
        RETURN other.email AS similar_user_email, sports_count, genres_count
        ORDER BY sports_count DESC, genres_count DESC
        """
            )
            result = session.run(query, email="david@htn.ca")
            res = [record for record in result]
            print(res)
            return res
    except Exception as e:
        print(f"\n\n{e}\n\n")
        print("Something died")

# create_uniqueness_constraints()

# build_test_graph1()
# add_movie_genre_graph()


sport_plus_movie_connections()

# get_connections_for_graph1()


driver.close()
