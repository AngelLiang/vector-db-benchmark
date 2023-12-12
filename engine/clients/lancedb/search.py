import multiprocessing as mp
from typing import List, Tuple

import lancedb

from engine.base_client.search import BaseSearcher
from engine.clients.lancedb.config import LANCEDB_COLLECTION_NAME


class LancedbSearcher(BaseSearcher):
    search_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        path = connection_params.pop('path')
        cls.client = lancedb.connect(path, **connection_params)

    @classmethod
    def search_one(cls, vector, meta_conditions, top) -> List[Tuple[int, float]]:
        table = cls.client.open_table(LANCEDB_COLLECTION_NAME)
        res = table.search(vector).limit(top).to_list()

        # res = cls.client.search(
        #     collection_name=LANCEDB_COLLECTION_NAME,
        #     query_vector=vector,
        #     query_filter=cls.parser.parse(meta_conditions),
        #     limit=top,
        #     search_params=rest.SearchParams(
        #         **cls.search_params.get("search_params", {})
        #     ),
        # )

        return [(hit.id, hit.score) for hit in res]
