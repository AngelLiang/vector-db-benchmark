import sqlite3
import sqlite_vss


from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.sqlite_vss.config import SQLITE_COLLECTION_NAME


client = sqlite3.connect('sqlite_vss.db')
client.enable_load_extension(True)
sqlite_vss.load(client)
cursor = client.cursor()


class SqliteVssConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: 'l2',
        Distance.COSINE: 'cosine',
        Distance.DOT: 'dot',
    }
    # INDEX_TYPE_MAPPING = {
    #     "int": rest.PayloadSchemaType.INTEGER,
    #     "keyword": rest.PayloadSchemaType.KEYWORD,
    #     "text": rest.PayloadSchemaType.TEXT,
    #     "float": rest.PayloadSchemaType.FLOAT,
    #     "geo": rest.PayloadSchemaType.GEO,
    # }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)

        path = connection_params.pop('path')
        self.client = client
        self.cursor = cursor

    def clean(self):
        self.cursor.execute(f"DROP TABLE {SQLITE_COLLECTION_NAME};")

    def recreate(self, dataset: Dataset, collection_params):
        self.clean()
        self.cursor.execute(f'create virtual table {SQLITE_COLLECTION_NAME} using vss0( a(2) factory="Flat,IDMap2", b(1)  factory="Flat,IDMap2");')
        
