import multiprocessing as mp
from typing import List, Tuple
import numpy as np

from engine.base_client.search import BaseSearcher
from engine.clients.sqlite_vss.config import SQLITE_TABLE_NAME


class SqliteVssSearcher(BaseSearcher):
    search_params = {}
    client = None

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        from .configure import client
        cls.client = client
        cls.search_params = search_params

    @classmethod
    def search_one(cls, vector, meta_conditions, top) -> List[Tuple[int, float]]:
        sql = f'SELECT rowid, distance FROM {SQLITE_TABLE_NAME}_vss \
            WHERE vss_search(content_embedding, ?) LIMIT ?;'
        vec = np.array(vector).astype(np.float32).tobytes()
        res = cls.client.execute(sql, (vec, top)).fetchall()
        return [(hit[0], hit[1]) for hit in res]