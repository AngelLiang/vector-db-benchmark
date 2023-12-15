import os
import sqlite3
import sqlite_vss


from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.sqlite_vss.config import SQLITE_TABLE_NAME, SQLITE_DB_NAME


def init_client():
    client = sqlite3.connect(SQLITE_DB_NAME)
    client.enable_load_extension(True)
    sqlite_vss.load(client)
    return client

client = init_client()

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

        self.path = connection_params.pop('path', SQLITE_DB_NAME)
        self.client = client

    def clean(self):
        global client
        try:
            self.client.close()
            os.remove(SQLITE_DB_NAME)
        except FileNotFoundError:
            pass
        finally:
            client = init_client()
            self.client = client

    def recreate(self, dataset: Dataset, collection_params):
        self.clean()
        sql = f"""
CREATE TABLE {SQLITE_TABLE_NAME} (
	id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, content_embedding BLOB);
"""
        self.client.execute(sql)
        size = dataset.config.vector_size
        self.client.execute(f'create virtual table {SQLITE_TABLE_NAME}_vss using vss0(content_embedding({size}));')
        self.client.commit()
