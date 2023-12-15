import time
from typing import List, Optional
import numpy as np

from engine.base_client.upload import BaseUploader
from engine.clients.sqlite_vss.config import SQLITE_TABLE_NAME


class SqliteVssUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        from .configure import client
        cls.client = client
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        for i in range(len(ids)):
            idx = ids[i]
            vec = vectors[i]
            meta = metadata[i] if metadata and metadata[i] else {}
            payload = {
                k: v
                for k, v in meta.items()
                if v is not None and not isinstance(v, dict)
            }

            sql = f'INSERT INTO {SQLITE_TABLE_NAME} VALUES(?, ?, ?);'
            id = idx + 1
            cls.client.execute(sql, (id, str(payload), np.array(vec).astype(np.float32).tobytes()))
            insert_sql = f"""INSERT INTO {SQLITE_TABLE_NAME}_vss(rowid, content_embedding) 
                SELECT id, content_embedding FROM {SQLITE_TABLE_NAME} WHERE id=?;"""
            cls.client.execute(insert_sql, (id,))
        cls.client.commit()

    @classmethod
    def post_upload(cls, _distance):
        return {}

    @classmethod
    def delete_client(cls):
        if cls.client is not None:
            del cls.client
