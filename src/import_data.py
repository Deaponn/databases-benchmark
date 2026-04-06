import os
import time
import subprocess
from mysql.connector import connect as mysql_connect
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase

class ImportOrchestrator:
    def __init__(self, dataset_size):
        self.size = dataset_size # 'small', 'medium', or 'big'
        self.data_path = f"/data/{dataset_size}" # Path inside the container

    def import_all(self, drop_indexes_after=True):
        self.import_postgres()
        self.import_mysql()
        self.import_mongo()
        self.import_neo4j(drop_indexes_after)

    def import_postgres(self):
        print(f"Postgres: Importing {self.size} (No Indexes)...")
        conn = psycopg2.connect(host="postgres", user="user", password="password", dbname="benchmark_db")
        cur = conn.cursor()

        # Schema without PRIMARY KEY or UNIQUE constraints
        tables = {
            "users": "id INT, username VARCHAR(255), email VARCHAR(255), password VARCHAR(255), created_at TIMESTAMP, settings_json JSONB",
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

        start_time = time.time()
        
        for table, schema in tables.items():
            print(f"Importing {table}...")
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            cur.execute(f"CREATE TABLE {table} ({schema});")
            
            # Use COPY for high speed
            csv_path = f"/var/lib/postgresql/csv_data/{self.size}/{table}.csv"
            with open(f"/app/data/{self.size}/{table}.csv", 'r') as f:
                cur.copy_from(f, table, sep=',', null='')

            print(f"This took {time.time() - start_time}")
            start_time = time.time()
        
        conn.commit()
        cur.close()
        conn.close()

    def import_mysql(self):
        print(f"MySQL: Importing {self.size} (No Indexes)...")
        conn = mysql_connect(
            host="mysql", 
            user="root", 
            password="password", 
            database="benchmark_db", 
            allow_local_infile=True
        )
        cur = conn.cursor()
        
        # Schema without PRIMARY KEY or UNIQUE constraints
        tables = {
            "users": "id INT, username VARCHAR(255), email VARCHAR(255), password VARCHAR(255), created_at DATETIME, settings_json JSON",
            "posts": "id INT, user_id INT, content TEXT, created_at DATETIME",
            "comments": "id INT, post_id INT, user_id INT, content TEXT, created_at DATETIME",
            "post_likes": "post_id INT, user_id INT, created_at DATETIME",
            "comment_likes": "comment_id INT, user_id INT, created_at DATETIME",
            "followers": "follower_id INT, followed_id INT, created_at DATETIME",
            "groups": "id INT, name VARCHAR(255), description TEXT, owner_id INT",
            "group_members": "group_id INT, user_id INT, joined_at DATETIME",
            "tags": "id INT, name VARCHAR(255)",
            "post_tags": "post_id INT, tag_id INT"
        }

        start_time = time.time()

        for table, schema in tables.items():
            print(f"Importing {table}...")
            cur.execute(f"DROP TABLE IF EXISTS {table};")
            cur.execute(f"CREATE TABLE {table} ({schema});")
            
            # MySQL LOAD DATA INFILE
            path = f"/var/lib/mysql-files/{self.size}/{table}.csv"
            query = f"""
                LOAD DATA INFILE '{path}' 
                INTO TABLE {table} 
                FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED BY '"' 
                LINES TERMINATED BY '\n'
            """
            cur.execute(query)

            print(f"This took {time.time() - start_time}")
            start_time = time.time()
            
        conn.commit()
        cur.close()
        conn.close()

    def import_mongo(self):
        print(f"MongoDB: Importing {self.size}...")
        field_map = {
            "users": "id,username,email,password,created_at,settings_json",
            "posts": "id,user_id,content,created_at",
            "comments": "id,post_id,user_id,content,created_at",
            "post_likes": "post_id,user_id,created_at",
            "comment_likes": "comment_id,user_id,created_at",
            "followers": "follower_id,followed_id,created_at",
            "groups": "id,name,description,owner_id",
            "group_members": "group_id,user_id,joined_at",
            "tags": "id,name",
            "post_tags": "post_id,tag_id"
        }

        start_time = time.time()

        for table, fields in field_map.items():
            print(f"Importing {table}...")
            path = f"/app/data/{self.size}/{table}.csv"
            if not os.path.exists(path): continue
            
            cmd = [
                "mongoimport", "--host", "mongodb", "--db", "benchmark_db",
                "--collection", table, "--type", "csv", "--file", path,
                "--fields", fields, "--drop" # --drop clears the collection before import
            ]
            subprocess.run(cmd)

            print(f"This took {time.time() - start_time}")
            start_time = time.time()

    def import_neo4j(self, drop_indexes_after):
        print(f"Neo4j: Importing {self.size}...")
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
        
        # 1. Map of Node creation queries
        node_queries = {
            "users.csv": "CREATE (:User {id: toInteger(row[0]), username: row[1], email: row[2]})",
            "posts.csv": "CREATE (:Post {id: toInteger(row[0]), content: row[2], created_at: row[3]})",
            "comments.csv": "CREATE (:Comment {id: toInteger(row[0]), content: row[3], created_at: row[4]})",
            "groups.csv": "CREATE (:Group {id: toInteger(row[0]), name: row[1], description: row[2]})",
            "tags.csv": "CREATE (:Tag {id: toInteger(row[0]), name: row[1]})"
        }

        # 2. Map of Relationship creation queries (Requires MATCH)
        rel_queries = {
            "followers.csv": "MATCH (a:User {id: toInteger(row[0])}), (b:User {id: toInteger(row[1])}) CREATE (a)-[:FOLLOWS {created_at: row[2]}]->(b)",
            "posts.csv": "MATCH (u:User {id: toInteger(row[1])}), (p:Post {id: toInteger(row[0])}) CREATE (u)-[:POSTED]->(p)",
            "comments.csv": "MATCH (p:Post {id: toInteger(row[1])}), (c:Comment {id: toInteger(row[0])}), (u:User {id: toInteger(row[2])}) CREATE (u)-[:COMMENTED]->(c), (c)-[:ON_POST]->(p)",
            "post_tags.csv": "MATCH (p:Post {id: toInteger(row[0])}), (t:Tag {id: toInteger(row[1])}) CREATE (p)-[:HAS_TAG]->(t)",
            "post_likes.csv": "MATCH (p:Post {id: toInteger(row[0])}), (u:User {id: toInteger(row[1])}) CREATE (u)-[:LIKES_POST {created_at: row[2]}]->(p)",
            "comment_likes.csv": "MATCH (c:Comment {id: toInteger(row[0])}), (u:User {id: toInteger(row[1])}) CREATE (u)-[:LIKES_COMMENT {created_at: row[2]}]->(c)",
            "group_members.csv": "MATCH (g:Group {id: toInteger(row[0])}), (u:User {id: toInteger(row[1])}) CREATE (u)-[:MEMBER_OF {joined_at: row[2]}]->(g)",
            "groups.csv": "MATCH (u:User {id: toInteger(row[3])}), (g:Group {id: toInteger(row[0])}) CREATE (u)-[:OWNS_GROUP]->(g)"
        }

        with driver.session() as session:
            # A. PRE-IMPORT: Unique Constraints (Vital for MATCH performance)
            constraints = ["User", "Post", "Comment", "Group", "Tag"]
            for label in constraints:
                session.run(f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")

            start_time = time.time()
            # B. IMPORT NODES
            for file_name, cypher in node_queries.items():
                print(f"Importing {file_name}...")
                query = f"LOAD CSV FROM 'file:///{self.size}/{file_name}' AS row CALL {{ WITH row {cypher} }} IN TRANSACTIONS OF 10000 ROWS"
                session.run(query)
                print(f"This took {time.time() - start_time}")
                start_time = time.time()

            # C. IMPORT RELATIONSHIPS
            for file_name, cypher in rel_queries.items():
                print(f"Importing {file_name}...")
                query = f"LOAD CSV FROM 'file:///{self.size}/{file_name}' AS row CALL {{ WITH row {cypher} }} IN TRANSACTIONS OF 5000 ROWS"
                session.run(query)
                print(f"This took {time.time() - start_time}")
                start_time = time.time()

            # D. POST-IMPORT: Cleanup if benchmarking "No Indexes"
            if drop_indexes_after:
                print("  - Dropping constraints for benchmark...")
                for label in constraints:
                    session.run(f"DROP CONSTRAINT {label.lower()}_id")
        
        driver.close()

if __name__ == "__main__":
    # Example usage
    importer = ImportOrchestrator("small")
    importer.import_all()
