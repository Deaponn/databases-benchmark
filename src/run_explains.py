import os
import json
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

class ExplainOrchestrator:
    def __init__(self):
        random.seed(420)
        
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

        # Dynamically fetch max IDs
        self._fetch_max_ids()
        
        # Track IDs used in Deletes
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

    def format_plan(self, plan):
        """Pomocnicza funkcja do formatowania wyników (JSON / String) na ładny Markdown"""
        if plan is None:
            return "Brak danych / Operacja nie zwróciła planu."
        try:
            # Jeśli plan jest w stringu z MySQL, robimy z niego dict
            if isinstance(plan, str):
                try:
                    plan = json.loads(plan)
                except Exception:
                    pass # zostaje jako string
                    
            # Dumps z default=str poradzi sobie z niestandardowymi obiektami z Neo4j
            return json.dumps(plan, indent=2, default=str)
        except Exception as e:
            return f"Nie udało się sformatować planu. Zwrócony obiekt: {str(plan)}\nBłąd: {e}"

    def generate_report(self, output_file):
        print(f"\n--- Generating EXPLAIN plans -> {output_file} ---")
        
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
        filepath = f"results/{output_file}"
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("# Raport EXPLAIN dla Scenariuszy CRUD\n\n")
            f.write("Poniżej znajdują się plany wykonania (Execution Plans) dla poszczególnych baz danych.\n\n")

            for func, name in scenarios:
                print(f"Pobieranie planów dla: {name}...")
                f.write(f"## Scenariusz: `{name}`\n\n")
                
                # Bierzemy jeden zestaw parametrów do analizy
                params = self.get_random_params(name)
                f.write(f"**Użyte parametry:** `{params}`\n\n")
                
                for db_type, conn in self.databases.items():
                    f.write(f"### Baza: {db_type.upper()}\n")
                    f.write("```json\n")
                    
                    try:
                        # WYWOŁANIE Z FLAGĄ EXPLAIN=TRUE
                        plan = func(db_type, conn, *params, explain=True)
                        formatted = self.format_plan(plan)
                        f.write(formatted)
                    except Exception as e:
                        f.write(f"Błąd podczas pobierania planu:\n{e}")
                        
                    f.write("\n```\n\n")
                
                f.write("---\n\n")

    def close(self):
        self.pg_conn.close()
        self.my_conn.close()
        self.mongo_client.close()
        self.neo4j_driver.close()


if __name__ == "__main__":
    orchestrator = ExplainOrchestrator()
    try:
        orchestrator.generate_report("explain_report_indexed.md")
    finally:
        orchestrator.close()
        print("\nGenerowanie planów zakończone. Sprawdź folder /results.")
