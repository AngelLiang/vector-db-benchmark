import lancedb
from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.lancedb.config import LANCEDB_COLLECTION_NAME


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
        "geo": str,
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        path = connection_params.pop('path')
        self.client = lancedb.connect(path, **connection_params)

    def clean(self):
        try:
            self.client.drop_table(LANCEDB_COLLECTION_NAME)
        except FileNotFoundError:
            pass

    def recreate(self, dataset: Dataset, collection_params):
        self.clean()
        size = dataset.config.vector_size
        schema = [
            {
                'id': 1,
                "vector": [i for i in range(size)],
            }
        ]
        # for field_name, field_type in dataset.config.schema.items():
        #     field_schema = self.DTYPE_MAPPING.get(field_type)
        #     schema.append({field_name: field_schema('0')})
        metric = self.DISTANCE_MAPPING[dataset.config.distance]
        table = self.client.create_table(LANCEDB_COLLECTION_NAME, schema)
        table.create_index(metric=metric, vector_column_name='vector')


if __name__ == "__main__":
    pass
