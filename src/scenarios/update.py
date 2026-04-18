import json

def u1_update_username(db_type, conn, user_id, new_username):
    """Scenario U1: Updating a user's username."""
    if db_type in ['postgres', 'mysql']:
        query = "UPDATE users SET username = %s WHERE id = %s"
        cur = conn.cursor()
        cur.execute(query, (new_username, user_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.users.update_one({"id": user_id}, {"$set": {"username": new_username}})

    elif db_type == 'neo4j':
        query = "MATCH (u:User {id: $id}) SET u.username = $username"
        with conn.session() as session:
            session.run(query, id=user_id, username=new_username)

def u2_update_password(db_type, conn, user_id, new_password):
    """Scenario U2: Updating a user's password."""
    if db_type in ['postgres', 'mysql']:
        query = "UPDATE users SET password = %s WHERE id = %s"
        cur = conn.cursor()
        cur.execute(query, (new_password, user_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.users.update_one({"id": user_id}, {"$set": {"password": new_password}})

    elif db_type == 'neo4j':
        query = "MATCH (u:User {id: $id}) SET u.password = $password"
        with conn.session() as session:
            session.run(query, id=user_id, password=new_password)

def u3_update_user_settings(db_type, conn, user_id, theme_value):
    """Scenario U3: Updating a specific key inside the settings_json blob."""
    if db_type == 'postgres':
        query = "UPDATE users SET settings_json = settings_json || jsonb_build_object('theme', %s) WHERE id = %s"
        cur = conn.cursor()
        cur.execute(query, (theme_value, user_id))
        conn.commit()

    elif db_type == 'mysql':
        query = "UPDATE users SET settings_json = JSON_SET(settings_json, '$.theme', %s) WHERE id = %s"
        cur = conn.cursor()
        cur.execute(query, (theme_value, user_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.users.update_one({"id": user_id}, {"$set": {"settings_json.theme": theme_value}})

    elif db_type == 'neo4j':
        new_settings = json.dumps({"theme": theme_value, "notifications": True})
        query = "MATCH (u:User {id: $id}) SET u.settings_json = $settings"
        with conn.session() as session:
            session.run(query, id=user_id, settings=new_settings)

def u4_user_censorship_bulk(db_type, conn, user_id, placeholder="[CONTENT REMOVED]"):
    """Scenario U4: Censorship simulation. Update all posts and comments for a user to a placeholder."""
    if db_type in ['postgres', 'mysql']:
        cur = conn.cursor()
        cur.execute("UPDATE posts SET content = %s WHERE user_id = %s", (placeholder, user_id))
        cur.execute("UPDATE comments SET content = %s WHERE user_id = %s", (placeholder, user_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.posts.update_many({"user_id": user_id}, {"$set": {"content": placeholder}})
        conn.comments.update_many({"user_id": user_id}, {"$set": {"content": placeholder}})

    elif db_type == 'neo4j':
        query = """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[:POSTED]->(p:Post)
        SET p.content = $val
        WITH u
        OPTIONAL MATCH (u)-[:COMMENTED]->(c:Comment)
        SET c.content = $val
        """
        with conn.session() as session:
            session.run(query, uid=user_id, val=placeholder)

def u5_update_tag_text(db_type, conn, tag_id, new_name):
    """Scenario U5: Updating a tag's text globally."""
    if db_type in ['postgres', 'mysql']:
        query = "UPDATE tags SET name = %s WHERE id = %s"
        cur = conn.cursor()
        cur.execute(query, (new_name, tag_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.tags.update_one({"id": tag_id}, {"$set": {"name": new_name}})

    elif db_type == 'neo4j':
        query = "MATCH (t:Tag {id: $id}) SET t.name = $name"
        with conn.session() as session:
            session.run(query, id=tag_id, name=new_name)

def u6_update_group_info(db_type, conn, group_id, name, description, new_owner_id):
    """Scenario U6: Updating group name, description, and changing the owner."""
    if db_type == 'postgres':
        query = 'UPDATE "groups" SET name = %s, description = %s, owner_id = %s WHERE id = %s'
        cur = conn.cursor()
        cur.execute(query, (name, description, new_owner_id, group_id))
        conn.commit()

    elif db_type == 'mysql':
        query = "UPDATE `groups` SET name = %s, description = %s, owner_id = %s WHERE id = %s"
        cur = conn.cursor()
        cur.execute(query, (name, description, new_owner_id, group_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.groups.update_one(
            {"id": group_id}, 
            {"$set": {"name": name, "description": description, "owner_id": new_owner_id}}
        )
    elif db_type == 'neo4j':
        query = """
        MATCH (g:Group {id: $gid})
        SET g.name = $name, g.description = $desc
        WITH g
        MATCH (:User)-[r:OWNS_GROUP]->(g)
        DELETE r
        WITH g
        MATCH (new_owner:User {id: $new_uid})
        CREATE (new_owner)-[:OWNS_GROUP]->(g)
        """
        with conn.session() as session:
            session.run(query, gid=group_id, name=name, desc=description, new_uid=new_owner_id)
