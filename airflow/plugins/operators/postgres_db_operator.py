import logging

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresLoadOperator(BaseOperator):
    def __init__(
        self,
        conn_id: str,
        file_path: str,
        table_name: str,
        trunc_table: bool | None = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.file_path = file_path
        self.table_name = table_name
        self.trunc_table = trunc_table

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        try:
            conn = hook.get_conn()
            logging.info(f"Conn: {conn}")
            cursor = conn.cursor()
            logging.info(f"Cursor: {cursor}")

            # if self.trunc_table:
            #     cursor.execute(f"DELETE {self.table_name}")
            #
            # cursor.copy_from(self.file_path, self.table_name)
            #
            # conn.commit()
        except Exception as e:
            logging.error(f"Insertion failed: {e}")
        finally:
            cursor.close()
            conn.close()
