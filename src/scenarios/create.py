import json
from datetime import datetime

# Globalne prefiksy do analizy zapytań
MY_SQL_EXPLAIN_PREFIX = "EXPLAIN FORMAT=JSON "
POSTGRES_EXPLAIN_PREFIX = "EXPLAIN (FORMAT JSON) "
NEO4J_EXPLAIN_PREFIX = "PROFILE "


def _execute_sql_write(db_type, conn, query, params=None, explain=False):
    """
    Funkcja pomocnicza dla MySQL i PostgreSQL dla zapytań modyfikujących (UPDATE/DELETE/INSERT).
    """
    if explain:
        prefix = POSTGRES_EXPLAIN_PREFIX if db_type == 'postgres' else MY_SQL_EXPLAIN_PREFIX
        query = prefix + query

    cur = conn.cursor()
    if params:
        cur.execute(query, params)
    else:
        cur.execute(query)

    if explain:
        result = cur.fetchone()[0]
        if db_type == 'mysql' and isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return result
        return result
    
    conn.commit()
    return None


def c1_register_user(db_type, conn, data, explain=False):
    """Scenario C1: Register a new user."""
    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO users (id, username, email, password, created_at, settings_json) VALUES (%s, %s, %s, %s, %s, %s)"
        return _execute_sql_write(db_type, conn, query, data, explain)

    elif db_type == 'mongodb':
        if explain:
            return {"note": "MongoDB does not support EXPLAIN for direct INSERT operations."}
        
        doc = {
            "id": data[0], "username": data[1], "email": data[2], 
            "password": data[3], "created_at": data[4], "settings_json": json.loads(data[5])
        }
        conn.users.insert_one(doc)

    elif db_type == 'neo4j':
        base_query = "CREATE (:User {id: $id, username: $username, email: $email, settings_json: $settings})"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=data[0], username=data[1], email=data[2], settings=data[5])
            if explain:
                return result.consume().profile


def c2_create_post_with_tags(db_type, conn, post_data, tag_ids, explain=False):
    """Scenario C2: Create a post and link it to 3 tags."""
    if db_type in ['postgres', 'mysql']:
        tag_inserts = [(post_data[0], tid) for tid in tag_ids]

        if explain:
            q1 = "INSERT INTO posts (id, user_id, content, created_at) VALUES (%s, %s, %s, %s)"
            q2 = "INSERT INTO post_tags (post_id, tag_id) VALUES (%s, %s)"
            plan1 = _execute_sql_write(db_type, conn, q1, post_data, explain=True)
            # Analizujemy INSERT tylko dla pierwszego tagu, bo executemany nie działa z EXPLAIN
            plan2 = _execute_sql_write(db_type, conn, q2, tag_inserts[0] if tag_inserts else (post_data[0], None), explain=True)
            return {"plan_post": plan1, "plan_tags": plan2}
        else:
            cur = conn.cursor()
            cur.execute("INSERT INTO posts (id, user_id, content, created_at) VALUES (%s, %s, %s, %s)", post_data)
            cur.executemany("INSERT INTO post_tags (post_id, tag_id) VALUES (%s, %s)", tag_inserts)
            conn.commit()

    elif db_type == 'mongodb':
        if explain:
            return {"note": "MongoDB does not support EXPLAIN for direct INSERT operations."}
            
        conn.posts.insert_one({"id": post_data[0], "user_id": post_data[1], "content": post_data[2], "created_at": post_data[3]})
        tag_docs = [{"post_id": post_data[0], "tag_id": tid} for tid in tag_ids]
        if tag_docs:
            conn.post_tags.insert_many(tag_docs)

    elif db_type == 'neo4j':
        base_query = """
        MATCH (u:User {id: $u_id})
        CREATE (u)-[:POSTED]->(p:Post {id: $p_id, content: $content, created_at: $date})
        WITH p
        UNWIND $t_ids AS tid
        MATCH (t:Tag {id: tid})
        CREATE (p)-[:HAS_TAG]->(t)
        """
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, u_id=post_data[1], p_id=post_data[0], content=post_data[2], date=post_data[3], t_ids=tag_ids)
            if explain:
                return result.consume().profile


def c3_follow_user(db_type, conn, follower_id, followed_id, explain=False):
    """Scenario C3: A user following another user."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO followers (follower_id, followed_id, created_at) VALUES (%s, %s, %s)"
        return _execute_sql_write(db_type, conn, query, (follower_id, followed_id, now), explain)

    elif db_type == 'mongodb':
        if explain:
            return {"note": "MongoDB does not support EXPLAIN for direct INSERT operations."}
            
        conn.followers.insert_one({"follower_id": follower_id, "followed_id": followed_id, "created_at": now})

    elif db_type == 'neo4j':
        base_query = "MATCH (a:User {id: $fid}), (b:User {id: $tid}) CREATE (a)-[:FOLLOWS {created_at: $now}]->(b)"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, fid=follower_id, tid=followed_id, now=now)
            if explain:
                return result.consume().profile


def c4_add_comment(db_type, conn, data, explain=False):
    """Scenario C4: Add a comment to a post."""
    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (%s, %s, %s, %s, %s)"
        return _execute_sql_write(db_type, conn, query, data, explain)

    elif db_type == 'mongodb':
        if explain:
            return {"note": "MongoDB does not support EXPLAIN for direct INSERT operations."}
            
        doc = {"id": data[0], "post_id": data[1], "user_id": data[2], "content": data[3], "created_at": data[4]}
        conn.comments.insert_one(doc)

    elif db_type == 'neo4j':
        base_query = """
        MATCH (u:User {id: $u_id}), (p:Post {id: $p_id})
        CREATE (u)-[:COMMENTED]->(c:Comment {id: $c_id, content: $content, created_at: $date})-[:ON_POST]->(p)
        """
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, u_id=data[2], p_id=data[1], c_id=data[0], content=data[3], date=data[4])
            if explain:
                return result.consume().profile


def c5_create_group(db_type, conn, data, explain=False):
    """Scenario C5: Create a new group."""
    if db_type == 'postgres':
        query = 'INSERT INTO "groups" (id, name, description, owner_id) VALUES (%s, %s, %s, %s)'
        return _execute_sql_write(db_type, conn, query, data, explain)

    elif db_type == 'mysql':
        query = "INSERT INTO `groups` (id, name, description, owner_id) VALUES (%s, %s, %s, %s)"
        return _execute_sql_write(db_type, conn, query, data, explain)

    elif db_type == 'mongodb':
        if explain:
            return {"note": "MongoDB does not support EXPLAIN for direct INSERT operations."}
            
        doc = {"id": data[0], "name": data[1], "description": data[2], "owner_id": data[3]}
        conn.groups.insert_one(doc)

    elif db_type == 'neo4j':
        base_query = "MATCH (u:User {id: $o_id}) CREATE (u)-[:OWNS_GROUP]->(:Group {id: $g_id, name: $name, description: $desc})"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, o_id=data[3], g_id=data[0], name=data[1], desc=data[2])
            if explain:
                return result.consume().profile


def c6_join_group(db_type, conn, group_id, user_id, explain=False):
    """Scenario C6: Join a group."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO group_members (group_id, user_id, joined_at) VALUES (%s, %s, %s)"
        return _execute_sql_write(db_type, conn, query, (group_id, user_id, now), explain)

    elif db_type == 'mongodb':
        if explain:
            return {"note": "MongoDB does not support EXPLAIN for direct INSERT operations."}
            
        conn.group_members.insert_one({"group_id": group_id, "user_id": user_id, "joined_at": now})
        
    elif db_type == 'neo4j':
        base_query = "MATCH (u:User {id: $uid}), (g:Group {id: $gid}) CREATE (u)-[:MEMBER_OF {joined_at: $now}]->(g)"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, uid=user_id, gid=group_id, now=now)
            if explain:
                return result.consume().profile