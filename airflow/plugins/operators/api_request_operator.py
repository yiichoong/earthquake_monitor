import json

from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook


class ApiRequestOperators(BaseOperator):
    def __init__(
        self,
        endpoint: str,
        http_conn_id: str,
        output_dir: str,
        output_filename: str,
        method: str = "GET",
        headers: dict | None = None,
        data: dict | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.method = method.upper()
        self.headers = headers or {}
        self.data = data

    def save_to_local(self, response):
        file_path = self.output_dir / self.output_filename
        file_path.write_text(json.dumps(response))
        self.log.info(
            f"\nOutput Dir {self.output_dir}\nOutput File Name: {self.output_filename}\nFile Path: {file_path}"
        )
        self.log.info(f"Saved API response to {self.output_filename}")

        return str(file_path)

    def execute(self, context):
        # Initialize hook
        http_hook = HttpHook(method=self.method, http_conn_id=self.http_conn_id)

        self.log.info(f"Calling API: {self.method} {self.endpoint}")
        response = http_hook.run(
            endpoint=self.endpoint, data=self.data, headers=self.headers
        )

        # Raise error if status != 200
        response.raise_for_status()

        try:
            result = response.json()
            self.log.info("Response JSON: %s", result)
        except Exception:
            self.log.warning("Response is not JSON, returning text.")
            result = response.text

        # Save to local file
        file_path = self.save_to_local(result)

        return file_path
