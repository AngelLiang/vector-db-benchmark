import time
from typing import List, Optional


from engine.base_client.upload import BaseUploader
from engine.clients.sqlite_vss.config import SQLITE_COLLECTION_NAME


class SqliteVssUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        # 原始代码
        # cls.client = QdrantClient(host=host, prefer_grpc=True, **connection_params)

        # 方案1
        # path = connection_params.pop('path')
        # cls.client = QdrantClient(path=path, prefer_grpc=True, **connection_params)

        # 方案2
        from .configure import client
        cls.client = client

        cls.upload_params = upload_params

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        pass

    @classmethod
    def post_upload(cls, _distance):
        return {}

    @classmethod
    def delete_client(cls):
        if cls.client is not None:
            del cls.client
