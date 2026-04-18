import time
import psycopg2
import mysql.connector
from pymongo import MongoClient
from neo4j import GraphDatabase

# Import your orchestrators
from import_data import ImportOrchestrator
from run_tests import BenchmarkOrchestrator

def wipe_databases():
    print("  -> Wiping PostgreSQL (Dropping Schema)...")
    try:
        pg = psycopg2.connect(host="postgres", user="user", password="password", dbname="benchmark_db")
        pg.autocommit = True
        pg.cursor().execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        pg.close()
    except Exception as e:
        print(f"     PG Wipe Error: {e}")

    print("  -> Wiping MySQL (Recreating Database)...")
    try:
        my = mysql.connector.connect(host="mysql", user="root", password="password")
        my_cur = my.cursor()
        my_cur.execute("DROP DATABASE IF EXISTS benchmark_db;")
        my_cur.execute("CREATE DATABASE benchmark_db;")
        my.close()
    except Exception as e:
        print(f"     MySQL Wipe Error: {e}")

    print("  -> Wiping MongoDB (Dropping Database)...")
    try:
        mongo = MongoClient("mongodb://mongodb:27017")
        mongo.drop_database("benchmark_db")
        mongo.close()
    except Exception as e:
        print(f"     Mongo Wipe Error: {e}")

    print("  -> Wiping Neo4j (Nodes, Relationships, Constraints, Indexes)...")
    try:
        neo = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
        with neo.session() as s:
            # 1. Delete all data in batches to prevent out-of-memory errors on 'Big' dataset
            s.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")
            
            # 2. Dynamically find and drop all constraints
            constraints = s.run("SHOW CONSTRAINTS YIELD name").data()
            for c in constraints:
                s.run(f"DROP CONSTRAINT {c['name']}")
                
            # 3. Dynamically find and drop all user-created indexes
            indexes = s.run("SHOW INDEXES YIELD name").data()
            for i in indexes:
                # Ignore Neo4j's internal default token lookup indexes
                if "lookup" not in i['name'].lower():
                    try:
                        s.run(f"DROP INDEX {i['name']}")
                    except:
                        pass
        neo.close()
    except Exception as e:
        print(f"     Neo4j Wipe Error: {e}")


def main():
    # Make sure you have generated the data first!
    datasets = ['small', 'medium', 'big']
    
    for size in datasets:
        print(f"\n===========================================================")
        print(f"             STARTING PIPELINE FOR: {size.upper()}")
        print(f"===========================================================")
        
        # 1. WIPE
        print(f"\n[1/5] WIPING DATABASES FOR CLEAN SLATE")
        wipe_databases()
        time.sleep(3) # Give engines a moment to free up memory/disk locks
        
        # 2. IMPORT
        print(f"\n[2/5] IMPORTING {size.upper()} DATASET (NO INDEXES)")
        importer = ImportOrchestrator(size)
        importer.import_all(drop_indexes_after=True) 
        
        # 3. BENCHMARK (NO INDEXES)
        print(f"\n[3/5] RUNNING BENCHMARKS: UNINDEXED")
        # Instantiate orchestrator AFTER import so it successfully fetches the new MAX(id)s
        tester = BenchmarkOrchestrator(size)
        tester.run_benchmarks(f"results_not_indexed_{size}.csv")
        
        # 4. ADD INDEXES
        print(f"\n[4/5] APPLYING INDEXES & CONSTRAINTS")
        tester.create_all_indexes()
        
        # 5. BENCHMARK (INDEXED)
        print(f"\n[5/5] RUNNING BENCHMARKS: INDEXED")
        tester.run_benchmarks(f"results_indexed_{size}.csv")
        
        # Cleanup connections for this dataset pass
        tester.close()
        print(f"\n>>> FINISHED PIPELINE FOR {size.upper()} <<<")

if __name__ == "__main__":
    main()