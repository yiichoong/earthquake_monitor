from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook


class ApiRequestOperators(BaseOperator):
    def __init__(
        self,
        endpoint: str,
        http_conn_id: str,
        method: str = "GET",
        headers: dict | None = None,
        data: dict | None = None,
        *args,
        **kwargs,
    ):
        super.__init__(*args, **kwargs)
        self.endpoint = self.endpoint
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.headers = headers or {}
        self.data = data

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
            return result
        except Exception:
            self.log.warning("Response is not JSON, returning text.")
            return response.text
