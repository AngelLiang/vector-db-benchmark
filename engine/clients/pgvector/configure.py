from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.pgvector.config import PGVECTOR_TABLE_NAME, PGVECTOR_USERNAME, PGVECTOR_PASSWORD, PGVECTOR_DATABASE_NAME
import psycopg2


class PGVectorConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: 'l2',
        Distance.COSINE: 'conine',
        Distance.DOT: 'dot',
    }
    INDEX_TYPE_MAPPING = {
        # "int": rest.PayloadSchemaType.INTEGER,
        # "keyword": rest.PayloadSchemaType.KEYWORD,
        # "text": rest.PayloadSchemaType.TEXT,
        # "float": rest.PayloadSchemaType.FLOAT,
        # "geo": rest.PayloadSchemaType.GEO,
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        self.db_config = {
            "host": host,
            "database": PGVECTOR_DATABASE_NAME,
            "user": PGVECTOR_USERNAME,
            "password": PGVECTOR_PASSWORD,
        }
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                cursor.execute('CREATE EXTENSION IF NOT EXISTS vector;')
        # 连接到默认数据库
        # https://stackoverflow.com/questions/74899785/psycopg2-errors-activesqltransaction-create-database-cannot-run-inside-a-transa
        # https://stackoverflow.com/questions/39028663/unable-to-set-psycopg2-autocommit-after-shp2pgsql-import/67418518#67418518
        # conn = psycopg2.connect(**self.db_config)
        # conn.autocommit = True
        # with conn, conn.cursor() as cursor:
        #     cursor.execute(f'CREATE DATABASE {PGVECTOR_DATABASE_NAME}')

    def clean(self):
        # with psycopg2.connect(**self.db_config) as conn:
        #     with conn.cursor() as cursor:
        #         conn.autocommit = True
        #         cursor.execute(f"DROP DATABASE {PGVECTOR_TABLE_NAME};")

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                sql = f'DROP TABLE {PGVECTOR_TABLE_NAME};'
                try:
                    cursor.execute(sql)
                except psycopg2.errors.UndefinedTable:
                    pass

    def recreate(self, dataset: Dataset, collection_params):
        self.clean()
        size = dataset.config.vector_size
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cursor:
                sql = f'CREATE TABLE {PGVECTOR_TABLE_NAME} (id bigserial PRIMARY KEY, embedding vector({size}))'
                cursor.execute(sql)
