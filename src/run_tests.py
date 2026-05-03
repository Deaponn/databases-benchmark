import os
import csv
import time
import random
import psycopg2
import mysql.connector
from pymongo import MongoClient
from neo4j import GraphDatabase
from datetime import datetime

# Import all scenarios
from scenarios.read import *
from scenarios.create import *
from scenarios.update import *
from scenarios.delete import *

class BenchmarkOrchestrator:
    def __init__(self, size_preset='small'):
        self.size = size_preset
        
        random.seed(42)
        
        # Connections
        self.pg_conn = psycopg2.connect(host="postgres", user="user", password="password", dbname="benchmark_db")
        self.pg_conn.autocommit = True  
        self.my_conn = mysql.connector.connect(host="mysql", user="root", password="password", database="benchmark_db")
        self.mongo_client = MongoClient("mongodb://mongodb:27017")
        self.mongo_db = self.mongo_client["benchmark_db"]
        self.neo4j_driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))

        self.databases = {
            'postgres': self.pg_conn,
            'mysql': self.my_conn,
            'mongodb': self.mongo_db,
            'neo4j': self.neo4j_driver
        }

        # Dynamically fetch max IDs from Postgres (assuming all DBs have identical imported data)
        self._fetch_max_ids()
        
        # Track IDs used in Deletes to prevent deleting the same record in Phase 1 and Phase 2
        self.used_delete_uids = set()
        self.used_delete_pids = set()
        self.used_delete_cids = set()
        self.used_delete_gids = set()

    def _fetch_max_ids(self):
        cur = self.pg_conn.cursor()
        cur.execute("SELECT MAX(id) FROM users")
        self.max_user_id = cur.fetchone()[0] or 0
        cur.execute("SELECT MAX(id) FROM posts")
        self.max_post_id = cur.fetchone()[0] or 0
        cur.execute("SELECT MAX(id) FROM comments")
        self.max_comment_id = cur.fetchone()[0] or 0
        cur.execute("SELECT MAX(id) FROM groups")
        self.max_group_id = cur.fetchone()[0] or 0
        cur.close()

    def _get_unique_target(self, max_val, used_set):
        """Picks a random ID from 1 to max_val that hasn't been deleted yet."""
        while True:
            val = random.randint(1, max_val)
            if val not in used_set:
                used_set.add(val)
                return val

    def get_random_params(self, scenario_name):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if scenario_name.startswith('r'):
            return {
                'r1': [random.randint(1, self.max_user_id)],
                'r2': [],
                'r3': [random.randint(1, self.max_post_id)],
                'r4': ['tech'],
                'r5': [random.randint(1, self.max_user_id)],
                'r6': []
            }.get(scenario_name.split('_')[0], [])
            
        elif scenario_name.startswith('c'):
            self.max_user_id += 1; self.max_post_id += 1; self.max_comment_id += 1; self.max_group_id += 1
            return {
                'c1': [(self.max_user_id, f"test_user_{self.max_user_id}", "test@test.com", "pass", now, '{"theme": "dark"}')],
                'c2': [(self.max_post_id, 1, "Benchmark Post", now), [1, 2, 3]],
                'c3': [self.max_user_id, 1],
                'c4': [(self.max_comment_id, 1, 1, "Benchmark Comment", now)],
                'c5': [(self.max_group_id, f"Group {self.max_group_id}", "Desc", 1)],
                'c6': [1, self.max_user_id] 
            }.get(scenario_name.split('_')[0], [])
            
        elif scenario_name.startswith('u'):
            uid = random.randint(1, self.max_user_id)
            return {
                'u1': [uid, f"updated_name_{random.randint(1,999)}"],
                'u2': [uid, f"new_pass_{random.randint(1,999)}"],
                'u3': [uid, random.choice(['light', 'dark'])],
                'u4': [uid, "[CENSORED BY ADMIN]"],
                'u5': [1, f"tech_{random.randint(1,999)}"],
                'u6': [1, "New Group Name", "New Desc", uid]
            }.get(scenario_name.split('_')[0], [])
            
        elif scenario_name.startswith('d'):
            target_uid = self._get_unique_target(self.max_user_id, self.used_delete_uids)
            target_pid = self._get_unique_target(self.max_post_id, self.used_delete_pids)
            target_cid = self._get_unique_target(self.max_comment_id, self.used_delete_cids)
            target_gid = self._get_unique_target(self.max_group_id, self.used_delete_gids)
            return {
                'd1': [1, target_uid], 
                'd2': [target_pid, 1], 
                'd3': [target_cid],
                'd4': [target_pid],
                'd5': [target_gid],
                'd6': [target_uid]
            }.get(scenario_name.split('_')[0], [])

    def execute_scenario(self, db_type, conn, func, params):
        # The timer only surrounds the database query execution
        start = time.perf_counter()
        func(db_type, conn, *params)
        end = time.perf_counter()
        return end - start

    def run_benchmarks(self, output_file):
        print(f"\n--- Starting Benchmarks -> {output_file} ---")
        
        scenarios = [
            (r1_friends_of_friends, 'r1_friends_of_friends'), (r2_json_filtering, 'r2_json_filtering'),
            (r3_post_engagement, 'r3_post_engagement'), (r4_tagged_posts, 'r4_tagged_posts'),
            (r5_social_feed, 'r5_social_feed'), (r6_most_popular_users, 'r6_most_popular_users'),
            
            (c1_register_user, 'c1_register_user'), (c2_create_post_with_tags, 'c2_create_post_with_tags'),
            (c3_follow_user, 'c3_follow_user'), (c4_add_comment, 'c4_add_comment'),
            (c5_create_group, 'c5_create_group'), (c6_join_group, 'c6_join_group'),
            
            (u1_update_username, 'u1_update_username'), (u2_update_password, 'u2_update_password'),
            (u3_update_user_settings, 'u3_update_user_settings'), (u4_user_censorship_bulk, 'u4_user_censorship_bulk'),
            (u5_update_tag_text, 'u5_update_tag_text'), (u6_update_group_info, 'u6_update_group_info'),
            
            (d1_unfollow_user, 'd1_unfollow_user'), (d2_remove_post_like, 'd2_remove_post_like'),
            (d3_delete_comment, 'd3_delete_comment'), (d4_delete_post_recursive, 'd4_delete_post_recursive'),
            (d5_delete_group, 'd5_delete_group'), (d6_nuke_user, 'd6_nuke_user')
        ]

        os.makedirs("results", exist_ok=True)
        with open(f"results/{output_file}", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["database", "scenario", "entry_index", "exec_time_seconds"])

            for func, name in scenarios:
                print(f"Executing {name}...")
                scenario_batch = [self.get_random_params(name) for _ in range(5)]
                
                for db_type, conn in self.databases.items():
                    for entry_idx, params in enumerate(scenario_batch, 1):
                        exec_time = self.execute_scenario(db_type, conn, func, params)
                        writer.writerow([db_type, name, entry_idx, f"{exec_time:.6f}"])

    def create_all_indexes(self):
        """Applies primary keys and indexes for the 'Indexed' benchmark phase."""
        print("\n--- Applying Indexes to all Databases ---")
        
        # 1. PostgreSQL Indexes
        pg_cur = self.pg_conn.cursor()
        pg_queries = [
            "ALTER TABLE users ADD PRIMARY KEY (id);",
            "ALTER TABLE posts ADD PRIMARY KEY (id);",
            "ALTER TABLE comments ADD PRIMARY KEY (id);",
            'ALTER TABLE "groups" ADD PRIMARY KEY (id);',
            "ALTER TABLE tags ADD PRIMARY KEY (id);",
            "CREATE INDEX idx_post_user ON posts(user_id);",
            "CREATE INDEX idx_comment_post ON comments(post_id);",
            "CREATE INDEX idx_followers_fid ON followers(follower_id);",
            "CREATE INDEX idx_followers_tid ON followers(followed_id);",
            "CREATE INDEX idx_post_tags_tid ON post_tags(tag_id);",
            "CREATE INDEX idx_group_members_uid ON group_members(user_id);",
            "CREATE INDEX idx_user_settings ON users USING GIN (settings_json);" 
        ]
        for q in pg_queries: 
            try: pg_cur.execute(q)
            except: print(f"Indexing query failed: {q}")

        # 2. MySQL Indexes
        my_cur = self.my_conn.cursor()
        my_queries = [
            "ALTER TABLE users ADD PRIMARY KEY (id);",
            "ALTER TABLE posts ADD PRIMARY KEY (id);",
            "ALTER TABLE comments ADD PRIMARY KEY (id);",
            "ALTER TABLE `groups` ADD PRIMARY KEY (id);",
            "ALTER TABLE tags ADD PRIMARY KEY (id);",
            "CREATE INDEX idx_post_user ON posts(user_id);",
            "CREATE INDEX idx_comment_post ON comments(post_id);",
            "CREATE INDEX idx_followers_fid ON followers(follower_id);",
            "CREATE INDEX idx_followers_tid ON followers(followed_id);",
            "CREATE INDEX idx_post_tags_tid ON post_tags(tag_id);",
            "CREATE INDEX idx_group_members_uid ON group_members(user_id);"
        ]
        for q in my_queries:
            try: my_cur.execute(q)
            except: print(f"Indexing query failed: {q}")

        # 3. MongoDB Indexes
        self.mongo_db.users.create_index("id", unique=True)
        self.mongo_db.posts.create_index("id", unique=True)
        self.mongo_db.posts.create_index("user_id")
        self.mongo_db.comments.create_index("id", unique=True)
        self.mongo_db.comments.create_index("post_id")
        self.mongo_db.followers.create_index("follower_id")
        self.mongo_db.followers.create_index("followed_id")
        self.mongo_db.group_members.create_index("user_id")
        self.mongo_db.post_tags.create_index("tag_id")
        self.mongo_db.users.create_index("settings_json.theme")

        # 4. Neo4j Constraints
        neo4j_queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Comment) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Group) REQUIRE g.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tag) REQUIRE t.id IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (t:Tag) ON (t.name)"
        ]
        with self.neo4j_driver.session() as session:
            for q in neo4j_queries:
                session.run(q)

        print("Indexes applied successfully. Waiting 5 seconds for engines to stabilize...")
        time.sleep(5)

    def close(self):
        self.pg_conn.close()
        self.my_conn.close()
        self.mongo_client.close()
        self.neo4j_driver.close()


if __name__ == "__main__":
    orchestrator = BenchmarkOrchestrator('small')
    try:
        orchestrator.run_benchmarks("results_not_indexed.csv")
        orchestrator.create_all_indexes()
        orchestrator.run_benchmarks("results_indexed.csv")
    finally:
        orchestrator.close()
        print("\nBenchmarking Complete. Check the /results folder.")
