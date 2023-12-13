import multiprocessing as mp
from typing import List, Tuple


from engine.base_client.search import BaseSearcher
from engine.clients.sqlite_vss.config import SQLITE_COLLECTION_NAME


class SqliteVssSearcher(BaseSearcher):
    search_params = {}
    client = None

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        # 原始代码
        # cls.client: QdrantClient = QdrantClient(
        #     host,
        #     prefer_grpc=True,
        #     limits=httpx.Limits(max_connections=None, max_keepalive_connections=0),
        #     **connection_params
        # )

        # 方案1
        # path = connection_params.pop('path')
        # cls.client: QdrantClient = QdrantClient(
        #     path=path,
        #     prefer_grpc=True,
        #     limits=httpx.Limits(max_connections=None, max_keepalive_connections=0),
        #     **connection_params
        # )

        # 方案2
        from .configure import client
        cls.client = client
        cls.search_params = search_params

    # Uncomment for gRPC
    # @classmethod
    # def get_mp_start_method(cls):
    #     return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def search_one(cls, vector, meta_conditions, top) -> List[Tuple[int, float]]:
        pass
