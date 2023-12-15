import os

SQLITE_DB_NAME = 'sqlite_vss.db'
SQLITE_TABLE_NAME = os.getenv("SQLITE_COLLECTION_NAME", "benchmark")
