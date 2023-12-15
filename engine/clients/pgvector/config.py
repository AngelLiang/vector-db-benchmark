import os

PGVECTOR_DATABASE_NAME = os.getenv("PGVECTOR_DATABASE_NAME", "benchmark")
PGVECTOR_TABLE_NAME = os.getenv("PGVECTOR_TABLE_NAME", "benchmark")
PGVECTOR_USERNAME = os.getenv("PGVECTOR_USERNAME", "postgres")
PGVECTOR_PASSWORD = os.getenv("PGVECTOR_PASSWORD", "postgres")
