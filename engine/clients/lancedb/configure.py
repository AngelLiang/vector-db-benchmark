import lancedb
import pyarrow as pa

from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.lancedb.config import LANCEDB_COLLECTION_NAME

client = None
metric = None

class LancedbConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: "L2",
        Distance.COSINE: "cosine",
        Distance.DOT: "dot",
    }
    FIELD_MAPPING = {
        "int": int,
        "keyword": str,
        "text": str,
        "float": float,
        # "geo": str,
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        path = connection_params.pop('path')
        self.client = lancedb.connect(path, **connection_params)
        global client
        client = self.client

    def clean(self):
        try:
            self.client.drop_table(LANCEDB_COLLECTION_NAME)
        except FileNotFoundError:
            pass

    def recreate(self, dataset: Dataset, collection_params):
        self.clean()
        size = dataset.config.vector_size
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("vector", lancedb.vector(size)),
            # pa.field('payload', pa.dictionary()),
        ])
        global metric
        metric = self.DISTANCE_MAPPING[dataset.config.distance]
        table = self.client.create_table(LANCEDB_COLLECTION_NAME, schema=schema, **collection_params)
        # table.create_index(metric=metric, vector_column_name='vector')


if __name__ == "__main__":
    pass
