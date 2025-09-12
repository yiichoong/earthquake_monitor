import logging
import os

from psycopg2 import sql

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresLoadOperator(BaseOperator):
    def __init__(
        self,
        conn_id: str,
        file_path: str,
        schema: str,
        table_name: str,
        trunc_table: bool | None = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.file_path = file_path
        self.schema = schema
        self.table_name = table_name
        self.trunc_table = trunc_table

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        conn = hook.get_conn()
        cursor = conn.cursor()

        logging.info(f"File path: {self.file_path}")
        logging.info(f"Table name: {self.table_name}")

        try:
            if self.trunc_table:
                cursor.execute(f"DELETE from {self.schema}.{self.table_name}")

            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"File {self.file_path} does not exist")

            query = sql.SQL(
                """
                COPY {}
                FROM STDIN
                WITH CSV
                DELIMITER ','
                HEADER
                """
            ).format(sql.Identifier(self.schema, self.table_name))

            with open(self.file_path, "r") as file:
                cursor.copy_expert(query, file)

            conn.commit()
        except Exception as e:
            logging.error(f"Failed to load into {self.table_name}: {e}", exc_info=True)
            raise
        finally:
            cursor.close()
            conn.close()
