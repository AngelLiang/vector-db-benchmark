from typing import List, Optional

import numpy as np

from engine.base_client.upload import BaseUploader

import lancedb
from engine.clients.lancedb.config import LANCEDB_COLLECTION_NAME
from engine.clients.lancedb.configure import client, metric


class LancedbUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        path = connection_params.pop('path')
        # cls.client = lancedb.connect(uri=path, **connection_params)
        cls.client = client
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        table = cls.client.open_table(LANCEDB_COLLECTION_NAME)

        def generate():
            for i in range(len(ids)):
                idx = ids[i]
                vec = vectors[i]
                # meta = metadata[i] if metadata and metadata[i] else {}
                # payload = {
                #     k: v
                #     for k, v in meta.items()
                #     if v is not None and not isinstance(v, dict)
                # }
                # Redis treats geopoints differently and requires putting them as
                # a comma-separated string with lat and lon coordinates
                # geopoints = {
                #     k: ",".join(
                #         map(str, v["lon"], v["lat"]))
                #     for k, v in meta.items()
                #     if isinstance(v, dict)
                # }
                # print(idx, len(vec))
                # table.add({
                #     'id': idx,
                #     "vector": vec,
                #     # 'payload': payload,
                #     # 'geopoints': geopoints,
                # })
                yield [{
                    'id': idx,
                    "vector": vec,
                    # 'payload': payload,
                    # 'geopoints': geopoints,
                }]
        generator = generate()
        table.add(generator)
        # metric = self.DISTANCE_MAPPING[dataset.config.distance]
        # table.create_index(vector_column_name='vector', num_partitions=len(ids))

    @classmethod
    def post_upload(cls, _distance):
        return {}
