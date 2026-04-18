import json

def r1_friends_of_friends(db_type, conn, user_id):
    """Scenario R1: Retrieving the full friend list for each friend of a given user."""

    if db_type in ['postgres', 'mysql']:
        query = """
            SELECT DISTINCT f2.followed_id 
            FROM followers f1 
            JOIN followers f2 ON f1.followed_id = f2.follower_id 
            WHERE f1.follower_id = %s;
        """
        cur = conn.cursor()
        cur.execute(query, (user_id,))
        return cur.fetchall()

    elif db_type == 'mongodb':
        pipeline = [
            {"$match": {"follower_id": user_id}},
            {"$lookup": {
                "from": "followers",
                "localField": "followed_id",
                "foreignField": "follower_id",
                "as": "fof"
            }},
            {"$unwind": "$fof"},
            {"$project": {"followed_id": "$fof.followed_id"}}
        ]
        return list(conn.followers.aggregate(pipeline))

    elif db_type == 'neo4j':
        query = "MATCH (u:User {id: $id})-[:FOLLOWS]->()-[:FOLLOWS]->(fof) RETURN DISTINCT fof.id"
        with conn.session() as session:
            return session.run(query, id=user_id).data()


def r2_json_filtering(db_type, conn):
    """Scenario R2: Finding the IDs of users who have dark mode enabled."""
    if db_type == 'postgres':
        query = "SELECT id FROM users WHERE settings_json->>'theme' = 'dark' LIMIT 100;"
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()

    elif db_type == 'mysql':
        query = "SELECT id FROM users WHERE settings_json->'$.theme' = 'dark' LIMIT 100;"
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()

    elif db_type == 'mongodb':
        return list(conn.users.find({"settings_json.theme": "dark"}, {"id": 1}).limit(100))

    elif db_type == 'neo4j':
        query = "MATCH (u:User) WHERE u.settings_json CONTAINS '\"theme\": \"dark\"' RETURN u.id LIMIT 100"
        with conn.session() as session:
            return session.run(query).data()


def r3_post_engagement(db_type, conn, post_id):
    """Scenario R3: Retrieving likes, comments, and comment likes for a given post."""
    if db_type in ['postgres', 'mysql']:
        query = """
            SELECT p.id, 
                   (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) as total_likes,
                   c.content,
                   (SELECT COUNT(*) FROM comment_likes WHERE comment_id = c.id) as comment_likes
            FROM posts p
            LEFT JOIN comments c ON p.id = c.post_id
            WHERE p.id = %s;
        """
        cur = conn.cursor()
        cur.execute(query, (post_id,))
        return cur.fetchall()

    elif db_type == 'mongodb':
        pipeline = [
            {"$match": {"id": post_id}},
            {"$lookup": {"from": "post_likes", "localField": "id", "foreignField": "post_id", "as": "likes"}},
            {"$lookup": {"from": "comments", "localField": "id", "foreignField": "post_id", "as": "coms"}},
            {"$project": {
                "like_count": {"$size": "$likes"},
                "comments": "$coms"
            }}
        ]
        return list(conn.posts.aggregate(pipeline))

    elif db_type == 'neo4j':
        query = """
            MATCH (p:Post {id: $id})
            OPTIONAL MATCH (p)<-[l:LIKES_POST]-()
            OPTIONAL MATCH (p)<-[:ON_POST]-(c:Comment)
            OPTIONAL MATCH (c)<-[cl:LIKES_COMMENT]-()
            RETURN p.id, count(DISTINCT l) as likes, c.content, count(DISTINCT cl) as comment_likes
        """
        with conn.session() as session:
            return session.run(query, id=post_id).data()


def r4_tagged_posts(db_type, conn, tag_name):
    """Scenario R4: Retrieving the latest 10 posts with specified tag."""
    if db_type in ['postgres', 'mysql']:
        query = """
            SELECT p.content, p.created_at 
            FROM posts p
            JOIN post_tags pt ON p.id = pt.post_id
            JOIN tags t ON pt.tag_id = t.id
            WHERE t.name = %s
            ORDER BY p.created_at DESC LIMIT 10;
        """
        cur = conn.cursor()
        cur.execute(query, (tag_name,))
        return cur.fetchall()

    elif db_type == 'mongodb':
        pipeline = [
            {"$match": {"name": tag_name}},
            {"$lookup": {"from": "post_tags", "localField": "id", "foreignField": "tag_id", "as": "links"}},
            {"$unwind": "$links"},
            {"$lookup": {"from": "posts", "localField": "links.post_id", "foreignField": "id", "as": "post"}},
            {"$unwind": "$post"},
            {"$sort": {"post.created_at": -1}},
            {"$limit": 10}
        ]
        return list(conn.tags.aggregate(pipeline))

    elif db_type == 'neo4j':
        query = """
            MATCH (t:Tag {name: $tag_name})<-[:HAS_TAG]-(p:Post)
            RETURN p.content, p.created_at 
            ORDER BY p.created_at DESC LIMIT 10
        """
        with conn.session() as session:
            return session.run(query, tag_name=tag_name).data()


def r5_social_feed(db_type, conn, user_id):
    """Scenario R5: Feed from friends and mutual group members."""
    if db_type in ['postgres', 'mysql']:
        query = """
            SELECT DISTINCT p.id, p.content, p.created_at
            FROM posts p
            WHERE p.user_id IN (SELECT followed_id FROM followers WHERE follower_id = %s)
               OR p.user_id IN (
                   SELECT gm2.user_id 
                   FROM group_members gm1 
                   JOIN group_members gm2 ON gm1.group_id = gm2.group_id 
                   WHERE gm1.user_id = %s
               )
            ORDER BY p.created_at DESC LIMIT 20;
        """
        cur = conn.cursor()
        cur.execute(query, (user_id, user_id))
        return cur.fetchall()

    elif db_type == 'mongodb':
        pipeline = [
            {"$match": {"id": user_id}},
            {"$lookup": {
                "from": "followers",
                "localField": "id",
                "foreignField": "follower_id",
                "as": "followed_docs"
            }},
            {"$lookup": {
                "from": "group_members",
                "localField": "id",
                "foreignField": "user_id",
                "as": "my_groups"
            }},
            {"$lookup": {
                "from": "group_members",
                "let": {"g_ids": "$my_groups.group_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$in": ["$group_id", "$$g_ids"]}}},
                    {"$project": {"user_id": 1, "_id": 0}}
                ],
                "as": "mutual_group_members"
            }},
            {"$project": {
                "relevant_user_ids": {
                    "$setUnion": [
                        "$followed_docs.followed_id",
                        "$mutual_group_members.user_id"
                    ]
                }
            }},
            {"$lookup": {
                "from": "posts",
                "localField": "relevant_user_ids",
                "foreignField": "user_id",
                "as": "feed"
            }},
            {"$unwind": "$feed"},
            {"$replaceRoot": {"newRoot": "$feed"}},
            {"$sort": {"created_at": -1}},
            {"$limit": 20}
        ]
        
        return list(conn.users.aggregate(pipeline))

    elif db_type == 'neo4j':
        query = """
            MATCH (u:User {id: $id})
            MATCH (u)-[:FOLLOWS|MEMBER_OF*1..2]-(other:User)-[:POSTED]->(p:Post)
            WHERE other <> u
            RETURN DISTINCT p.id, p.content, p.created_at
            ORDER BY p.created_at DESC LIMIT 20
        """
        with conn.session() as session:
            return session.run(query, id=user_id).data()


def r6_most_popular_users(db_type, conn):
    """Scenario R6: Finding the top 10 most popular users (most followers)."""
    if db_type in ['postgres', 'mysql']:
        query = "SELECT followed_id, COUNT(*) as cnt FROM followers GROUP BY followed_id ORDER BY cnt DESC LIMIT 10;"
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()

    elif db_type == 'mongodb':
        pipeline = [
            {"$group": {"_id": "$followed_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        return list(conn.followers.aggregate(pipeline))

    elif db_type == 'neo4j':
        query = "MATCH (u:User)<-[:FOLLOWS]-() RETURN u.id, count(*) as cnt ORDER BY cnt DESC LIMIT 10"
        with conn.session() as session:
            return session.run(query).data()
