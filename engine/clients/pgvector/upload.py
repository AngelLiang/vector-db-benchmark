import time
from typing import List, Optional
import numpy as np

from engine.base_client.upload import BaseUploader
from engine.clients.pgvector.config import PGVECTOR_TABLE_NAME, PGVECTOR_DATABASE_NAME, PGVECTOR_USERNAME, PGVECTOR_PASSWORD

import psycopg2
from pgvector.psycopg2 import register_vector


class PGVectorUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.db_config = {
            "host": host,
            "database": PGVECTOR_DATABASE_NAME,
            "user": PGVECTOR_USERNAME,
            "password": PGVECTOR_PASSWORD,
        }
        cls.client = psycopg2.connect(**cls.db_config)

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        with psycopg2.connect(**cls.db_config) as conn:
            register_vector(conn)
            with conn.cursor() as cursor:
                for i in range(len(ids)):
                    idx = ids[i]
                    vec = vectors[i]
                    meta = metadata[i] if metadata and metadata[i] else {}
                    payload = {
                        k: v
                        for k, v in meta.items()
                        if v is not None and not isinstance(v, dict)
                    }
                    id = idx + 1
                    # sql = f"INSERT INTO {PGVECTOR_TABLE_NAME} (id, embedding) VALUES({id}, '{np.array(vec)}');"
                    # cursor.execute(sql)
                    embedding = np.array(vec)
                    cursor.execute(
                        f'INSERT INTO {PGVECTOR_TABLE_NAME} VALUES (%s,%s)', (id, embedding))
