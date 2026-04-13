import os
import csv
import json
import time
from datetime import datetime
from mysql.connector import connect as mysql_connect
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        print(f"[{func.__name__}] {time.time() - start:.3f}s")
        return res
    return wrapper

SQL_TABLES = {
    "users": "id INT, username VARCHAR(255), email VARCHAR(255), password VARCHAR(255), created_at TIMESTAMP, settings_json JSON",
    "posts": "id INT, user_id INT, content TEXT, created_at TIMESTAMP",
    "comments": "id INT, post_id INT, user_id INT, content TEXT, created_at TIMESTAMP",
    "post_likes": "post_id INT, user_id INT, created_at TIMESTAMP",
    "comment_likes": "comment_id INT, user_id INT, created_at TIMESTAMP",
    "followers": "follower_id INT, followed_id INT, created_at TIMESTAMP",
    "groups": "id INT, name VARCHAR(255), description TEXT, owner_id INT",
    "group_members": "group_id INT, user_id INT, joined_at TIMESTAMP",
    "tags": "id INT, name VARCHAR(255)",
    "post_tags": "post_id INT, tag_id INT"
}

FIELD_MAP = {
    "users": ["id", "username", "email", "password", "created_at", "settings_json"],
    "posts": ["id", "user_id", "content", "created_at"],
    "comments": ["id", "post_id", "user_id", "content", "created_at"],
    "post_likes": ["post_id", "user_id", "created_at"],
    "comment_likes": ["comment_id", "user_id", "created_at"],
    "followers": ["follower_id", "followed_id", "created_at"],
    "groups": ["id", "name", "description", "owner_id"],
    "group_members": ["group_id", "user_id", "joined_at"],
    "tags": ["id", "name"],
    "post_tags": ["post_id", "tag_id"]
}

CONSTRAINTS = ["User", "Post", "Comment", "Group", "Tag"]

NODE_QUERIES = {
    "users.csv": "CREATE (:User {id: toInteger(row.id), username: row.username, email: row.email})",
    "posts.csv": "CREATE (:Post {id: toInteger(row.id), content: row.content, created_at: row.created_at})",
    "comments.csv": "CREATE (:Comment {id: toInteger(row.id), content: row.content, created_at: row.created_at})",
    "groups.csv": "CREATE (:Group {id: toInteger(row.id), name: row.name, description: row.description})",
    "tags.csv": "CREATE (:Tag {id: toInteger(row.id), name: row.name})"
}

REL_QUERIES = {
    "followers.csv": "MATCH (a:User {id: toInteger(row.follower_id)}), (b:User {id: toInteger(row.followed_id)}) CREATE (a)-[:FOLLOWS {created_at: row.created_at}]->(b)",
    "posts.csv": "MATCH (u:User {id: toInteger(row.user_id)}), (p:Post {id: toInteger(row.id)}) CREATE (u)-[:POSTED]->(p)",
    "comments.csv": "MATCH (p:Post {id: toInteger(row.post_id)}), (c:Comment {id: toInteger(row.id)}), (u:User {id: toInteger(row.user_id)}) CREATE (u)-[:COMMENTED]->(c), (c)-[:ON_POST]->(p)",
    "post_tags.csv": "MATCH (p:Post {id: toInteger(row.post_id)}), (t:Tag {id: toInteger(row.tag_id)}) CREATE (p)-[:HAS_TAG]->(t)",
    "post_likes.csv": "MATCH (p:Post {id: toInteger(row.post_id)}), (u:User {id: toInteger(row.user_id)}) CREATE (u)-[:LIKES_POST {created_at: row.created_at}]->(p)",
    "comment_likes.csv": "MATCH (c:Comment {id: toInteger(row.comment_id)}), (u:User {id: toInteger(row.user_id)}) CREATE (u)-[:LIKES_COMMENT {created_at: row.created_at}]->(c)",
    "group_members.csv": "MATCH (g:Group {id: toInteger(row.group_id)}), (u:User {id: toInteger(row.user_id)}) CREATE (u)-[:MEMBER_OF {joined_at: row.joined_at}]->(g)",
    "groups.csv": "MATCH (u:User {id: toInteger(row.owner_id)}), (g:Group {id: toInteger(row.id)}) CREATE (u)-[:OWNS_GROUP]->(g)"
}

class ImportOrchestrator:
    def __init__(self, dataset_size):
        self.size = dataset_size # 'small', 'medium', or 'big'
        self.data_path = f"/data/{dataset_size}" # Path inside the container

    def import_all(self, drop_indexes_after=True):
        self.import_postgres()
        self.import_mysql()
        self.import_mongo()
        self.import_neo4j(drop_indexes_after)

    def load_csv(self, name):
        with open(f"data/{self.size}/{name}.csv", 'r', encoding='utf-8') as f:
            return list(csv.DictReader(f))

    @timer
    def import_postgres(self):
        conn = psycopg2.connect(dbname="benchmark_db", user="user", password="password", host="postgres")
        cur = conn.cursor()
        
        for table, schema in SQL_TABLES.items():
            data = self.load_csv(table)
            if not data: continue
            
            pg_schema = schema.replace("JSON", "JSONB")
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            cur.execute(f"CREATE TABLE {table} ({pg_schema});")
            
            cols = data[0].keys()
            placeholders = ", ".join([f"%({c})s" for c in cols])
            cur.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})", data)
            
        conn.commit()
        cur.close()
        conn.close()

    @timer
    def import_mysql(self):
        conn = mysql_connect(host='mysql', user='root', password='password', database='benchmark_db')
        cur = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        for table, schema in SQL_TABLES.items():
            data = self.load_csv(table)
            if not data: continue
            
            cur.execute(f"DROP TABLE IF EXISTS `{table}`;")
            cur.execute(f"CREATE TABLE `{table}` ({schema});")
            
            cols = data[0].keys()
            placeholders = ", ".join(["%s"] * len(cols))
            
            tuples = [tuple(d[c] for c in cols) for d in data]
            cur.executemany(f"INSERT INTO `{table}` ({', '.join(cols)}) VALUES ({placeholders})", tuples)
            
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        conn.commit()
        cur.close()
        conn.close()

    @timer
    def import_mongo(self):
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["benchmark_db"]

        for table in ["users", "posts", "comments", "post_likes", "comment_likes", "followers", "groups", "group_members", "tags", "post_tags"]:
            path = f"/app/data/{self.size}/{table}.csv"

            db[table].drop()
            documents = []

            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    doc = {}
                    
                    for key, value in row.items():
                        if key.endswith('_id') or key == 'id':
                            doc[key] = int(value)
                        elif key.endswith('_at'):
                            doc[key] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                        elif key == 'settings_json':
                            doc[key] = json.loads(value)
                        else:
                            doc[key] = value

                    documents.append(doc)
                    
                    if len(documents) >= 10000:
                        db[table].insert_many(documents)
                        documents = []
                        
            if documents:
                db[table].insert_many(documents)

    @timer
    def import_neo4j(self, drop_indexes_after):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
        
        with driver.session() as session:
            for label in CONSTRAINTS:
                session.run(f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")

            for file_name, cypher in NODE_QUERIES.items():
                query = f"LOAD CSV WITH HEADERS FROM 'file:///{self.size}/{file_name}' AS row CALL (row) {{ {cypher} }} IN TRANSACTIONS OF 10000 ROWS"
                session.run(query)

            for file_name, cypher in REL_QUERIES.items():
                query = f"LOAD CSV WITH HEADERS FROM 'file:///{self.size}/{file_name}' AS row CALL (row) {{ {cypher} }} IN TRANSACTIONS OF 5000 ROWS"
                session.run(query)

            if drop_indexes_after:
                for label in CONSTRAINTS:
                    session.run(f"DROP CONSTRAINT {label.lower()}_id")

if __name__ == "__main__":
    importer = ImportOrchestrator("small")
    importer.import_all()
