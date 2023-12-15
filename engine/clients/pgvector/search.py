import multiprocessing as mp
from typing import List, Tuple

from engine.base_client.search import BaseSearcher
from engine.clients.pgvector.config import PGVECTOR_TABLE_NAME, PGVECTOR_DATABASE_NAME, PGVECTOR_USERNAME, PGVECTOR_PASSWORD

import psycopg2
from pgvector.psycopg2 import register_vector
import numpy as np


class PGVectorSearcher(BaseSearcher):
    search_params = {}
    client = None

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.db_config = {
            "host": host,
            "database": PGVECTOR_DATABASE_NAME,
            "user": PGVECTOR_USERNAME,
            "password": PGVECTOR_PASSWORD,
        }
        cls.search_params = search_params

    @classmethod
    def search_one(cls, vector, meta_conditions, top) -> List[Tuple[int, float]]:
        with psycopg2.connect(**cls.db_config) as conn:
            with conn.cursor() as cursor:
                register_vector(cursor)
                embedding = np.array(vector)
                cursor.execute(
                    f"""SELECT id, embedding <-> %s AS distance FROM {PGVECTOR_TABLE_NAME} ORDER BY distance LIMIT {top};""", (embedding, ))
                # https://stackoverflow.com/a/37966281
                res = cursor.fetchall()
                return [(hit[0], hit[1]) for hit in res]
