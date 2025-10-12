"""
HTTP Operator for Airflow

This operator provides functionality for making HTTP requests in Airflow DAGs.
"""

import json
from typing import Any, Dict, Optional
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException


class HttpPostOperator(BaseOperator):
    """
    HTTP POST Operator for making POST requests.

    :param url: URL to make the request to (if not using connection)
    :param http_conn_id: HTTP connection ID (optional)
    :param endpoint: Endpoint path (used with http_conn_id)
    :param headers: HTTP headers
    :param data: Request body data (will be JSON serialized)
    :param timeout: Request timeout in seconds
    """

    template_fields = ('url', 'data', 'headers', 'endpoint')
    ui_color = '#4CAF50'

    def __init__(
            self,
            url: Optional[str] = None,
            http_conn_id: Optional[str] = None,
            endpoint: Optional[str] = None,
            headers: Optional[Dict[str, str]] = None,
            data: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.url = url
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data
        self.timeout = timeout

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute HTTP POST request."""
        try:
            # Check if payloads is empty and skip request if so
            if self.data and "payloads" in self.data:
                payloads = self.data.get("payloads", [])
                if not payloads:
                    self.log.info("Empty payloads detected, skipping HTTP request")
                    return {"status": "skipped", "reason": "empty_payloads", "payloads_count": 0}

            # Prepare headers
            request_headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            request_headers.update(self.headers)

            # Prepare data
            request_data = json.dumps(self.data) if self.data else None

            # Make request
            if self.http_conn_id:
                hook = HttpHook(method='POST', http_conn_id=self.http_conn_id)
                response = hook.run(
                    endpoint=self.endpoint or "/",
                    data=request_data,
                    headers=request_headers
                )
            else:
                if not self.url:
                    raise AirflowException("Either url or http_conn_id must be provided")
                import requests
                response = requests.post(
                    self.url,
                    data=request_data,
                    headers=request_headers,
                    timeout=self.timeout
                )
                response.raise_for_status()

            # Parse response
            try:
                result = response.json() if hasattr(response, 'json') else response
            except Exception:
                result = {
                    "status_code": getattr(response, 'status_code', 200),
                    "text": str(response)[:500]
                }

            self.log.info(f"HTTP POST request completed successfully")
            return result

        except Exception as e:
            self.log.error(f"HTTP POST request failed: {str(e)}")
            raise AirflowException(f"HTTP POST request failed: {str(e)}")


class HttpGetOperator(BaseOperator):
    """
    HTTP GET Operator for making GET requests.

    :param url: URL to make the request to
    :param headers: HTTP headers
    :param params: Query parameters
    :param timeout: Request timeout in seconds
    :param http_conn_id: HTTP connection ID (optional)
    """

    template_fields = ('url', 'params', 'headers')
    ui_color = '#2196F3'

    def __init__(
            self,
            url: str,
            headers: Optional[Dict[str, str]] = None,
            params: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            http_conn_id: Optional[str] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.url = url
        self.headers = headers or {}
        self.params = params
        self.timeout = timeout
        self.http_conn_id = http_conn_id

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute HTTP GET request."""
        try:
            # Make request
            if self.http_conn_id:
                hook = HttpHook(method='GET', http_conn_id=self.http_conn_id)
                response = hook.run(
                    endpoint=self.url,
                    headers=self.headers,
                    data=self.params
                )
            else:
                import requests
                response = requests.get(
                    self.url,
                    headers=self.headers,
                    params=self.params,
                    timeout=self.timeout
                )
                response.raise_for_status()

            # Parse response
            try:
                result = response.json() if hasattr(response, 'json') else response
            except Exception:
                result = {
                    "status_code": getattr(response, 'status_code', 200),
                    "text": str(response)[:500]
                }

            self.log.info(f"HTTP GET request completed successfully")
            return result

        except Exception as e:
            self.log.error(f"HTTP GET request failed: {str(e)}")
            raise AirflowException(f"HTTP GET request failed: {str(e)}")
