import json
from datetime import datetime

def c1_register_user(db_type, conn, data):
    """Scenario C1: Register a new user."""
    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO users (id, username, email, password, created_at, settings_json) VALUES (%s, %s, %s, %s, %s, %s)"
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()

    elif db_type == 'mongodb':
        doc = {
            "id": data[0], "username": data[1], "email": data[2], 
            "password": data[3], "created_at": data[4], "settings_json": json.loads(data[5])
        }
        conn.users.insert_one(doc)

    elif db_type == 'neo4j':
        query = "CREATE (:User {id: $id, username: $username, email: $email, settings_json: $settings})"
        with conn.session() as session:
            session.run(query, id=data[0], username=data[1], email=data[2], settings=data[5])

def c2_create_post_with_tags(db_type, conn, post_data, tag_ids):
    """Scenario C2: Create a post and link it to 3 tags."""
    if db_type in ['postgres', 'mysql']:
        cur = conn.cursor()
        cur.execute("INSERT INTO posts (id, user_id, content, created_at) VALUES (%s, %s, %s, %s)", post_data)
        tag_inserts = [(post_data[0], tid) for tid in tag_ids]
        cur.executemany("INSERT INTO post_tags (post_id, tag_id) VALUES (%s, %s)", tag_inserts)
        conn.commit()

    elif db_type == 'mongodb':
        conn.posts.insert_one({"id": post_data[0], "user_id": post_data[1], "content": post_data[2], "created_at": post_data[3]})
        tag_docs = [{"post_id": post_data[0], "tag_id": tid} for tid in tag_ids]
        conn.post_tags.insert_many(tag_docs)

    elif db_type == 'neo4j':
        query = """
        MATCH (u:User {id: $u_id})
        CREATE (u)-[:POSTED]->(p:Post {id: $p_id, content: $content, created_at: $date})
        WITH p
        UNWIND $t_ids AS tid
        MATCH (t:Tag {id: tid})
        CREATE (p)-[:HAS_TAG]->(t)
        """
        with conn.session() as session:
            session.run(query, u_id=post_data[1], p_id=post_data[0], content=post_data[2], date=post_data[3], t_ids=tag_ids)

def c3_follow_user(db_type, conn, follower_id, followed_id):
    """Scenario C3: A user following another user."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO followers (follower_id, followed_id, created_at) VALUES (%s, %s, %s)"
        cur = conn.cursor()
        cur.execute(query, (follower_id, followed_id, now))
        conn.commit()

    elif db_type == 'mongodb':
        conn.followers.insert_one({"follower_id": follower_id, "followed_id": followed_id, "created_at": now})

    elif db_type == 'neo4j':
        query = "MATCH (a:User {id: $fid}), (b:User {id: $tid}) CREATE (a)-[:FOLLOWS {created_at: $now}]->(b)"
        with conn.session() as session:
            session.run(query, fid=follower_id, tid=followed_id, now=now)

def c4_add_comment(db_type, conn, data):
    """Scenario C4: Add a comment to a post."""
    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (%s, %s, %s, %s, %s)"
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()

    elif db_type == 'mongodb':
        doc = {"id": data[0], "post_id": data[1], "user_id": data[2], "content": data[3], "created_at": data[4]}
        conn.comments.insert_one(doc)

    elif db_type == 'neo4j':
        query = """
        MATCH (u:User {id: $u_id}), (p:Post {id: $p_id})
        CREATE (u)-[:COMMENTED]->(c:Comment {id: $c_id, content: $content, created_at: $date})-[:ON_POST]->(p)
        """
        with conn.session() as session:
            session.run(query, u_id=data[2], p_id=data[1], c_id=data[0], content=data[3], date=data[4])

def c5_create_group(db_type, conn, data):
    """Scenario C5: Create a new group."""
    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO groups (id, name, description, owner_id) VALUES (%s, %s, %s, %s)"
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()

    elif db_type == 'mongodb':
        doc = {"id": data[0], "name": data[1], "description": data[2], "owner_id": data[3]}
        conn.groups.insert_one(doc)

    elif db_type == 'neo4j':
        query = "MATCH (u:User {id: $o_id}) CREATE (u)-[:OWNS_GROUP]->(:Group {id: $g_id, name: $name, description: $desc})"
        with conn.session() as session:
            session.run(query, o_id=data[3], g_id=data[0], name=data[1], desc=data[2])

def c6_join_group(db_type, conn, group_id, user_id):
    """Scenario C6: Join a group."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if db_type in ['postgres', 'mysql']:
        query = "INSERT INTO group_members (group_id, user_id, joined_at) VALUES (%s, %s, %s)"
        cur = conn.cursor()
        cur.execute(query, (group_id, user_id, now))
        conn.commit()

    elif db_type == 'mongodb':
        conn.group_members.insert_one({"group_id": group_id, "user_id": user_id, "joined_at": now})
        
    elif db_type == 'neo4j':
        query = "MATCH (u:User {id: $uid}), (g:Group {id: $gid}) CREATE (u)-[:MEMBER_OF {joined_at: $now}]->(g)"
        with conn.session() as session:
            session.run(query, uid=user_id, gid=group_id, now=now)
