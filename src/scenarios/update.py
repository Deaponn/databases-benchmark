import json

# Globalne prefiksy do analizy zapytań
MY_SQL_EXPLAIN_PREFIX = "EXPLAIN FORMAT=JSON "
POSTGRES_EXPLAIN_PREFIX = "EXPLAIN (FORMAT JSON) "
NEO4J_EXPLAIN_PREFIX = "PROFILE "


def _execute_sql_write(db_type, conn, query, params=None, explain=False):
    """
    Funkcja pomocnicza dla MySQL i PostgreSQL dla zapytań modyfikujących (UPDATE/DELETE/INSERT).
    Jeśli explain=True, pobiera plan wykonania. 
    Jeśli explain=False, wykonuje commit zapisu do bazy.
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
    
    # Dla normalnego wykonania (explain=False) robimy commit
    conn.commit()
    return None


def u1_update_username(db_type, conn, user_id, new_username, explain=False):
    """Scenario U1: Updating a user's username."""
    if db_type in ['postgres', 'mysql']:
        query = "UPDATE users SET username = %s WHERE id = %s"
        return _execute_sql_write(db_type, conn, query, (new_username, user_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"update": "users", "updates": [{"q": {"id": user_id}, "u": {"$set": {"username": new_username}}}]},
                verbosity="executionStats"
            )
        conn.users.update_one({"id": user_id}, {"$set": {"username": new_username}})

    elif db_type == 'neo4j':
        base_query = "MATCH (u:User {id: $id}) SET u.username = $username"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=user_id, username=new_username)
            if explain:
                return result.consume().profile


def u2_update_password(db_type, conn, user_id, new_password, explain=False):
    """Scenario U2: Updating a user's password."""
    if db_type in ['postgres', 'mysql']:
        query = "UPDATE users SET password = %s WHERE id = %s"
        return _execute_sql_write(db_type, conn, query, (new_password, user_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"update": "users", "updates": [{"q": {"id": user_id}, "u": {"$set": {"password": new_password}}}]},
                verbosity="executionStats"
            )
        conn.users.update_one({"id": user_id}, {"$set": {"password": new_password}})

    elif db_type == 'neo4j':
        base_query = "MATCH (u:User {id: $id}) SET u.password = $password"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=user_id, password=new_password)
            if explain:
                return result.consume().profile


def u3_update_user_settings(db_type, conn, user_id, theme_value, explain=False):
    """Scenario U3: Updating a specific key inside the settings_json blob."""
    if db_type == 'postgres':
        query = "UPDATE users SET settings_json = settings_json || jsonb_build_object('theme', %s) WHERE id = %s"
        return _execute_sql_write(db_type, conn, query, (theme_value, user_id), explain)

    elif db_type == 'mysql':
        query = "UPDATE users SET settings_json = JSON_SET(settings_json, '$.theme', %s) WHERE id = %s"
        return _execute_sql_write(db_type, conn, query, (theme_value, user_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"update": "users", "updates": [{"q": {"id": user_id}, "u": {"$set": {"settings_json.theme": theme_value}}}]},
                verbosity="executionStats"
            )
        conn.users.update_one({"id": user_id}, {"$set": {"settings_json.theme": theme_value}})

    elif db_type == 'neo4j':
        new_settings = json.dumps({"theme": theme_value, "notifications": True})
        base_query = "MATCH (u:User {id: $id}) SET u.settings_json = $settings"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=user_id, settings=new_settings)
            if explain:
                return result.consume().profile


def u4_user_censorship_bulk(db_type, conn, user_id, placeholder="[CONTENT REMOVED]", explain=False):
    """Scenario U4: Censorship simulation. Update all posts and comments for a user to a placeholder."""
    if db_type in ['postgres', 'mysql']:
        q1 = "UPDATE posts SET content = %s WHERE user_id = %s"
        q2 = "UPDATE comments SET content = %s WHERE user_id = %s"
        
        if explain:
            plan1 = _execute_sql_write(db_type, conn, q1, (placeholder, user_id), explain=True)
            plan2 = _execute_sql_write(db_type, conn, q2, (placeholder, user_id), explain=True)
            return {"posts_update_plan": plan1, "comments_update_plan": plan2}
        else:
            cur = conn.cursor()
            cur.execute(q1, (placeholder, user_id))
            cur.execute(q2, (placeholder, user_id))
            conn.commit()

    elif db_type == 'mongodb':
        if explain:
            plan1 = conn.command(
                "explain", 
                {"update": "posts", "updates": [{"q": {"user_id": user_id}, "u": {"$set": {"content": placeholder}}, "multi": True}]},
                verbosity="executionStats"
            )
            plan2 = conn.command(
                "explain", 
                {"update": "comments", "updates": [{"q": {"user_id": user_id}, "u": {"$set": {"content": placeholder}}, "multi": True}]},
                verbosity="executionStats"
            )
            return {"posts_update_plan": plan1, "comments_update_plan": plan2}
        else:
            conn.posts.update_many({"user_id": user_id}, {"$set": {"content": placeholder}})
            conn.comments.update_many({"user_id": user_id}, {"$set": {"content": placeholder}})

    elif db_type == 'neo4j':
        base_query = """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[:POSTED]->(p:Post)
        SET p.content = $val
        WITH u
        OPTIONAL MATCH (u)-[:COMMENTED]->(c:Comment)
        SET c.content = $val
        """
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, uid=user_id, val=placeholder)
            if explain:
                return result.consume().profile


def u5_update_tag_text(db_type, conn, tag_id, new_name, explain=False):
    """Scenario U5: Updating a tag's text globally."""
    if db_type in ['postgres', 'mysql']:
        query = "UPDATE tags SET name = %s WHERE id = %s"
        return _execute_sql_write(db_type, conn, query, (new_name, tag_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"update": "tags", "updates": [{"q": {"id": tag_id}, "u": {"$set": {"name": new_name}}}]},
                verbosity="executionStats"
            )
        conn.tags.update_one({"id": tag_id}, {"$set": {"name": new_name}})

    elif db_type == 'neo4j':
        base_query = "MATCH (t:Tag {id: $id}) SET t.name = $name"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=tag_id, name=new_name)
            if explain:
                return result.consume().profile


def u6_update_group_info(db_type, conn, group_id, name, description, new_owner_id, explain=False):
    """Scenario U6: Updating group name, description, and changing the owner."""
    if db_type == 'postgres':
        query = 'UPDATE "groups" SET name = %s, description = %s, owner_id = %s WHERE id = %s'
        return _execute_sql_write(db_type, conn, query, (name, description, new_owner_id, group_id), explain)

    elif db_type == 'mysql':
        query = "UPDATE `groups` SET name = %s, description = %s, owner_id = %s WHERE id = %s"
        return _execute_sql_write(db_type, conn, query, (name, description, new_owner_id, group_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"update": "groups", "updates": [{"q": {"id": group_id}, "u": {"$set": {"name": name, "description": description, "owner_id": new_owner_id}}}]},
                verbosity="executionStats"
            )
        conn.groups.update_one(
            {"id": group_id}, 
            {"$set": {"name": name, "description": description, "owner_id": new_owner_id}}
        )
        
    elif db_type == 'neo4j':
        base_query = """
        MATCH (g:Group {id: $gid})
        SET g.name = $name, g.description = $desc
        WITH g
        MATCH (:User)-[r:OWNS_GROUP]->(g)
        DELETE r
        WITH g
        MATCH (new_owner:User {id: $new_uid})
        CREATE (new_owner)-[:OWNS_GROUP]->(g)
        """
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, gid=group_id, name=name, desc=description, new_uid=new_owner_id)
            if explain:
                return result.consume().profile