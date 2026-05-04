import json

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


def d1_unfollow_user(db_type, conn, follower_id, followed_id, explain=False):
    """Scenario D1: Removing a person from friends."""
    if db_type in ['postgres', 'mysql']:
        query = "DELETE FROM followers WHERE follower_id = %s AND followed_id = %s"
        return _execute_sql_write(db_type, conn, query, (follower_id, followed_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"delete": "followers", "deletes": [{"q": {"follower_id": follower_id, "followed_id": followed_id}, "limit": 1}]},
                verbosity="executionStats"
            )
        conn.followers.delete_one({"follower_id": follower_id, "followed_id": followed_id})

    elif db_type == 'neo4j':
        base_query = "MATCH (a:User {id: $fid})-[r:FOLLOWS]->(b:User {id: $tid}) DELETE r"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, fid=follower_id, tid=followed_id)
            if explain:
                return result.consume().profile


def d2_remove_post_like(db_type, conn, post_id, user_id, explain=False):
    """Scenario D2: Deleting a like from a post."""
    if db_type in ['postgres', 'mysql']:
        query = "DELETE FROM post_likes WHERE post_id = %s AND user_id = %s"
        return _execute_sql_write(db_type, conn, query, (post_id, user_id), explain)

    elif db_type == 'mongodb':
        if explain:
            return conn.command(
                "explain", 
                {"delete": "post_likes", "deletes": [{"q": {"post_id": post_id, "user_id": user_id}, "limit": 1}]},
                verbosity="executionStats"
            )
        conn.post_likes.delete_one({"post_id": post_id, "user_id": user_id})
        
    elif db_type == 'neo4j':
        base_query = "MATCH (u:User {id: $uid})-[r:LIKES_POST]->(p:Post {id: $pid}) DELETE r"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, uid=user_id, pid=post_id)
            if explain:
                return result.consume().profile


def d3_delete_comment(db_type, conn, comment_id, explain=False):
    """Scenario D3: Deleting a comment and its associated likes."""
    if db_type in ['postgres', 'mysql']:
        q1 = "DELETE FROM comment_likes WHERE comment_id = %s"
        q2 = "DELETE FROM comments WHERE id = %s"
        if explain:
            return {
                "plan_comment_likes": _execute_sql_write(db_type, conn, q1, (comment_id,), explain=True),
                "plan_comments": _execute_sql_write(db_type, conn, q2, (comment_id,), explain=True)
            }
        else:
            cur = conn.cursor()
            cur.execute(q1, (comment_id,))
            cur.execute(q2, (comment_id,))
            conn.commit()

    elif db_type == 'mongodb':
        if explain:
            return {
                "plan_comment_likes": conn.command("explain", {"delete": "comment_likes", "deletes": [{"q": {"comment_id": comment_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_comments": conn.command("explain", {"delete": "comments", "deletes": [{"q": {"id": comment_id}, "limit": 1}]}, verbosity="executionStats")
            }
        conn.comment_likes.delete_many({"comment_id": comment_id})
        conn.comments.delete_one({"id": comment_id})

    elif db_type == 'neo4j':
        base_query = "MATCH (c:Comment {id: $id}) DETACH DELETE c"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=comment_id)
            if explain:
                return result.consume().profile


def d4_delete_post_recursive(db_type, conn, post_id, explain=False):
    """Scenario D4: Deleting a post with all likes, tags, comments, and comment likes."""
    if db_type in ['postgres', 'mysql']:
        queries = [
            ("DELETE FROM post_likes WHERE post_id = %s", (post_id,)),
            ("DELETE FROM post_tags WHERE post_id = %s", (post_id,)),
            ("DELETE FROM comment_likes WHERE comment_id IN (SELECT id FROM comments WHERE post_id = %s)", (post_id,)),
            ("DELETE FROM comments WHERE post_id = %s", (post_id,)),
            ("DELETE FROM posts WHERE id = %s", (post_id,))
        ]
        if explain:
            return {f"plan_{i}": _execute_sql_write(db_type, conn, q, params, explain=True) for i, (q, params) in enumerate(queries)}
        else:
            cur = conn.cursor()
            for q, params in queries:
                cur.execute(q, params)
            conn.commit()

    elif db_type == 'mongodb':
        # Pobieranie ID wykonuje się niezależnie od trybu explain
        comment_ids = [c['id'] for c in conn.comments.find({"post_id": post_id}, {"id": 1})]
        
        if explain:
            return {
                "plan_find_comments": conn.comments.find({"post_id": post_id}, {"id": 1}).explain("executionStats"),
                "plan_del_comment_likes": conn.command("explain", {"delete": "comment_likes", "deletes": [{"q": {"comment_id": {"$in": comment_ids}}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_comments": conn.command("explain", {"delete": "comments", "deletes": [{"q": {"post_id": post_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_post_likes": conn.command("explain", {"delete": "post_likes", "deletes": [{"q": {"post_id": post_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_post_tags": conn.command("explain", {"delete": "post_tags", "deletes": [{"q": {"post_id": post_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_posts": conn.command("explain", {"delete": "posts", "deletes": [{"q": {"id": post_id}, "limit": 1}]}, verbosity="executionStats")
            }
        conn.comment_likes.delete_many({"comment_id": {"$in": comment_ids}})
        conn.comments.delete_many({"post_id": post_id})
        conn.post_likes.delete_many({"post_id": post_id})
        conn.post_tags.delete_many({"post_id": post_id})
        conn.posts.delete_one({"id": post_id})

    elif db_type == 'neo4j':
        base_query = """
        MATCH (p:Post {id: $pid})
        OPTIONAL MATCH (p)<-[:ON_POST]-(c:Comment)
        DETACH DELETE p, c
        """
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, pid=post_id)
            if explain:
                return result.consume().profile


def d5_delete_group(db_type, conn, group_id, explain=False):
    """Scenario D5: Deleting a group and all its memberships."""
    if db_type in ['postgres', 'mysql']:
        q1 = "DELETE FROM group_members WHERE group_id = %s"
        q2 = 'DELETE FROM "groups" WHERE id = %s' if db_type == 'postgres' else "DELETE FROM `groups` WHERE id = %s"
        
        if explain:
            return {
                "plan_group_members": _execute_sql_write(db_type, conn, q1, (group_id,), explain=True),
                "plan_groups": _execute_sql_write(db_type, conn, q2, (group_id,), explain=True)
            }
        else:
            cur = conn.cursor()
            cur.execute(q1, (group_id,))
            cur.execute(q2, (group_id,))
            conn.commit()

    elif db_type == 'mongodb':
        if explain:
            return {
                "plan_group_members": conn.command("explain", {"delete": "group_members", "deletes": [{"q": {"group_id": group_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_groups": conn.command("explain", {"delete": "groups", "deletes": [{"q": {"id": group_id}, "limit": 1}]}, verbosity="executionStats")
            }
        conn.group_members.delete_many({"group_id": group_id})
        conn.groups.delete_one({"id": group_id})

    elif db_type == 'neo4j':
        base_query = "MATCH (g:Group {id: $id}) DETACH DELETE g"
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, id=group_id)
            if explain:
                return result.consume().profile


def d6_nuke_user(db_type, conn, user_id, explain=False):
    """Scenario D6: Total account deletion (Cascade everything)."""
    if db_type in ['postgres', 'mysql']:
        group_query1 = 'DELETE FROM group_members WHERE group_id IN (SELECT id FROM "groups" WHERE owner_id = %s)' if db_type == 'postgres' else "DELETE FROM group_members WHERE group_id IN (SELECT id FROM `groups` WHERE owner_id = %s)"
        group_query2 = 'DELETE FROM "groups" WHERE owner_id = %s' if db_type == 'postgres' else "DELETE FROM `groups` WHERE owner_id = %s"
        
        queries = [
            ("""DELETE FROM comment_likes WHERE user_id = %s OR comment_id IN (SELECT id FROM comments WHERE user_id = %s) OR comment_id IN (SELECT c.id FROM comments c JOIN posts p ON c.post_id = p.id WHERE p.user_id = %s)""", (user_id, user_id, user_id)),
            ("DELETE FROM post_likes WHERE user_id = %s OR post_id IN (SELECT id FROM posts WHERE user_id = %s)", (user_id, user_id)),
            ("DELETE FROM post_tags WHERE post_id IN (SELECT id FROM posts WHERE user_id = %s)", (user_id,)),
            ("DELETE FROM comments WHERE user_id = %s OR post_id IN (SELECT id FROM posts WHERE user_id = %s)", (user_id, user_id)),
            ("DELETE FROM posts WHERE user_id = %s", (user_id,)),
            ("DELETE FROM followers WHERE follower_id = %s OR followed_id = %s", (user_id, user_id)),
            (group_query1, (user_id,)),
            ("DELETE FROM group_members WHERE user_id = %s", (user_id,)),
            (group_query2, (user_id,)),
            ("DELETE FROM users WHERE id = %s", (user_id,))
        ]

        if explain:
            return {f"plan_{i}": _execute_sql_write(db_type, conn, q, params, explain=True) for i, (q, params) in enumerate(queries)}
        else:
            cur = conn.cursor()
            for q, params in queries:
                cur.execute(q, params)
            conn.commit()

    elif db_type == 'mongodb':
        user_posts = [p['id'] for p in conn.posts.find({"user_id": user_id}, {"id": 1})]
        user_groups = [g['id'] for g in conn.groups.find({"owner_id": user_id}, {"id": 1})]
        post_comments = [c['id'] for c in conn.comments.find({"post_id": {"$in": user_posts}}, {"id": 1})]
        user_comments = [c['id'] for c in conn.comments.find({"user_id": user_id}, {"id": 1})]
        all_relevant_comments = list(set(post_comments + user_comments))

        if explain:
            return {
                "plan_del_comment_likes": conn.command("explain", {"delete": "comment_likes", "deletes": [{"q": {"$or": [{"user_id": user_id}, {"comment_id": {"$in": all_relevant_comments}}]}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_comments": conn.command("explain", {"delete": "comments", "deletes": [{"q": {"id": {"$in": all_relevant_comments}}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_post_likes": conn.command("explain", {"delete": "post_likes", "deletes": [{"q": {"$or": [{"user_id": user_id}, {"post_id": {"$in": user_posts}}]}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_post_tags": conn.command("explain", {"delete": "post_tags", "deletes": [{"q": {"post_id": {"$in": user_posts}}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_posts": conn.command("explain", {"delete": "posts", "deletes": [{"q": {"user_id": user_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_followers": conn.command("explain", {"delete": "followers", "deletes": [{"q": {"$or": [{"follower_id": user_id}, {"followed_id": user_id}]}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_group_members": conn.command("explain", {"delete": "group_members", "deletes": [{"q": {"$or": [{"user_id": user_id}, {"group_id": {"$in": user_groups}}]}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_groups": conn.command("explain", {"delete": "groups", "deletes": [{"q": {"owner_id": user_id}, "limit": 0}]}, verbosity="executionStats"),
                "plan_del_users": conn.command("explain", {"delete": "users", "deletes": [{"q": {"id": user_id}, "limit": 1}]}, verbosity="executionStats")
            }

        conn.post_likes.delete_many({"$or": [{"user_id": user_id}, {"post_id": {"$in": user_posts}}]})
        conn.comment_likes.delete_many({"$or": [{"user_id": user_id}, {"comment_id": {"$in": all_relevant_comments}}]})
        conn.comments.delete_many({"id": {"$in": all_relevant_comments}})
        conn.post_tags.delete_many({"post_id": {"$in": user_posts}})
        conn.posts.delete_many({"user_id": user_id})
        conn.followers.delete_many({"$or": [{"follower_id": user_id}, {"followed_id": user_id}]})
        conn.group_members.delete_many({"$or": [{"user_id": user_id}, {"group_id": {"$in": user_groups}}]})
        conn.groups.delete_many({"owner_id": user_id})
        conn.users.delete_one({"id": user_id})

    elif db_type == 'neo4j':
        base_query = """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[:POSTED]->(p:Post)
        OPTIONAL MATCH (p)<-[:ON_POST]-(c:Comment)
        OPTIONAL MATCH (u)-[:OWNS_GROUP]->(g:Group)
        DETACH DELETE u, p, c, g
        """
        query = (NEO4J_EXPLAIN_PREFIX + base_query) if explain else base_query
        with conn.session() as session:
            result = session.run(query, uid=user_id)
            if explain:
                return result.consume().profile