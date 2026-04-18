def d1_unfollow_user(db_type, conn, follower_id, followed_id):
    """Scenario D1: Removing a person from friends."""
    if db_type in ['postgres', 'mysql']:
        query = "DELETE FROM followers WHERE follower_id = %s AND followed_id = %s"
        cur = conn.cursor()
        cur.execute(query, (follower_id, followed_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.followers.delete_one({"follower_id": follower_id, "followed_id": followed_id})

    elif db_type == 'neo4j':
        query = "MATCH (a:User {id: $fid})-[r:FOLLOWS]->(b:User {id: $tid}) DELETE r"
        with conn.session() as session:
            session.run(query, fid=follower_id, tid=followed_id)

def d2_remove_post_like(db_type, conn, post_id, user_id):
    """Scenario D2: Deleting a like from a post."""
    if db_type in ['postgres', 'mysql']:
        query = "DELETE FROM post_likes WHERE post_id = %s AND user_id = %s"
        cur = conn.cursor()
        cur.execute(query, (post_id, user_id))
        conn.commit()

    elif db_type == 'mongodb':
        conn.post_likes.delete_one({"post_id": post_id, "user_id": user_id})
        
    elif db_type == 'neo4j':
        query = "MATCH (u:User {id: $uid})-[r:LIKES_POST]->(p:Post {id: $pid}) DELETE r"
        with conn.session() as session:
            session.run(query, uid=user_id, pid=post_id)

def d3_delete_comment(db_type, conn, comment_id):
    """Scenario D3: Deleting a comment and its associated likes."""
    if db_type in ['postgres', 'mysql']:
        cur = conn.cursor()
        cur.execute("DELETE FROM comment_likes WHERE comment_id = %s", (comment_id,))
        cur.execute("DELETE FROM comments WHERE id = %s", (comment_id,))
        conn.commit()

    elif db_type == 'mongodb':
        conn.comment_likes.delete_many({"comment_id": comment_id})
        conn.comments.delete_one({"id": comment_id})

    elif db_type == 'neo4j':
        query = "MATCH (c:Comment {id: $id}) DETACH DELETE c"
        with conn.session() as session:
            session.run(query, id=comment_id)

def d4_delete_post_recursive(db_type, conn, post_id):
    """Scenario D4: Deleting a post with all likes, tags, comments, and comment likes."""
    if db_type in ['postgres', 'mysql']:
        cur = conn.cursor()
        cur.execute("DELETE FROM post_likes WHERE post_id = %s", (post_id,))
        cur.execute("DELETE FROM post_tags WHERE post_id = %s", (post_id,))
        cur.execute("DELETE FROM comment_likes WHERE comment_id IN (SELECT id FROM comments WHERE post_id = %s)", (post_id,))
        cur.execute("DELETE FROM comments WHERE post_id = %s", (post_id,))
        cur.execute("DELETE FROM posts WHERE id = %s", (post_id,))
        conn.commit()

    elif db_type == 'mongodb':
        comment_ids = [c['id'] for c in conn.comments.find({"post_id": post_id}, {"id": 1})]
        conn.comment_likes.delete_many({"comment_id": {"$in": comment_ids}})
        conn.comments.delete_many({"post_id": post_id})
        conn.post_likes.delete_many({"post_id": post_id})
        conn.post_tags.delete_many({"post_id": post_id})
        conn.posts.delete_one({"id": post_id})

    elif db_type == 'neo4j':
        query = """
        MATCH (p:Post {id: $pid})
        OPTIONAL MATCH (p)<-[:ON_POST]-(c:Comment)
        DETACH DELETE p, c
        """
        with conn.session() as session:
            session.run(query, pid=post_id)

def d5_delete_group(db_type, conn, group_id):
    """Scenario D5: Deleting a group and all its memberships."""
    if db_type == 'postgres':
        cur = conn.cursor()
        cur.execute("DELETE FROM group_members WHERE group_id = %s", (group_id,))
        cur.execute('DELETE FROM "groups" WHERE id = %s', (group_id,))
        conn.commit()

    elif db_type == 'mysql':
        cur = conn.cursor()
        cur.execute("DELETE FROM group_members WHERE group_id = %s", (group_id,))
        cur.execute("DELETE FROM `groups` WHERE id = %s", (group_id,))
        conn.commit()

    elif db_type == 'mongodb':
        conn.group_members.delete_many({"group_id": group_id})
        conn.groups.delete_one({"id": group_id})

    elif db_type == 'neo4j':
        query = "MATCH (g:Group {id: $id}) DETACH DELETE g"
        with conn.session() as session:
            session.run(query, id=group_id)

def d6_nuke_user(db_type, conn, user_id):
    """Scenario D6: Total account deletion (Cascade everything)."""
    if db_type in ['postgres', 'mysql']:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM comment_likes 
            WHERE user_id = %s 
            OR comment_id IN (SELECT id FROM comments WHERE user_id = %s)
            OR comment_id IN (SELECT c.id FROM comments c JOIN posts p ON c.post_id = p.id WHERE p.user_id = %s)
        """, (user_id, user_id, user_id))
        cur.execute("DELETE FROM post_likes WHERE user_id = %s OR post_id IN (SELECT id FROM posts WHERE user_id = %s)", (user_id, user_id))
        cur.execute("DELETE FROM post_tags WHERE post_id IN (SELECT id FROM posts WHERE user_id = %s)", (user_id,))
        cur.execute("DELETE FROM comments WHERE user_id = %s OR post_id IN (SELECT id FROM posts WHERE user_id = %s)", (user_id, user_id))
        cur.execute("DELETE FROM posts WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM followers WHERE follower_id = %s OR followed_id = %s", (user_id, user_id))
        if db_type == 'postgres': cur.execute('DELETE FROM group_members WHERE group_id IN (SELECT id FROM "groups" WHERE owner_id = %s)', (user_id,))
        else: cur.execute("DELETE FROM group_members WHERE group_id IN (SELECT id FROM `groups` WHERE owner_id = %s)", (user_id,))
        cur.execute("DELETE FROM group_members WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM groups WHERE owner_id = %s", (user_id,))
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()

    elif db_type == 'mongodb':
        user_posts = [p['id'] for p in conn.posts.find({"user_id": user_id}, {"id": 1})]
        user_groups = [g['id'] for g in conn.groups.find({"owner_id": user_id}, {"id": 1})]
        conn.post_likes.delete_many({"$or": [{"user_id": user_id}, {"post_id": {"$in": user_posts}}]})
        post_comments = [c['id'] for c in conn.comments.find({"post_id": {"$in": user_posts}}, {"id": 1})]
        user_comments = [c['id'] for c in conn.comments.find({"user_id": user_id}, {"id": 1})]
        all_relevant_comments = list(set(post_comments + user_comments))
        conn.comment_likes.delete_many({"$or": [{"user_id": user_id}, {"comment_id": {"$in": all_relevant_comments}}]})
        conn.comments.delete_many({"id": {"$in": all_relevant_comments}})
        conn.post_tags.delete_many({"post_id": {"$in": user_posts}})
        conn.posts.delete_many({"user_id": user_id})
        conn.followers.delete_many({"$or": [{"follower_id": user_id}, {"followed_id": user_id}]})
        conn.group_members.delete_many({"$or": [{"user_id": user_id}, {"group_id": {"$in": user_groups}}]})
        conn.groups.delete_many({"owner_id": user_id})
        conn.users.delete_one({"id": user_id})

    elif db_type == 'neo4j':
        query = """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[:POSTED]->(p:Post)
        OPTIONAL MATCH (p)<-[:ON_POST]-(c:Comment)
        OPTIONAL MATCH (u)-[:OWNS_GROUP]->(g:Group)
        DETACH DELETE u, p, c, g
        """
        with conn.session() as session:
            session.run(query, uid=user_id)
