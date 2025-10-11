import json
import time
import base64
import bcrypt
from typing import Any, Dict, Optional, Union, List
from datetime import datetime

import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class NaverStoreClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def get_access_token(self, client_id: str, timestamp_ms: int, client_secret_sign_b64: str) -> str:
        url = f"{self.base_url}/v1/oauth2/token"
        payload = {
            "client_id": client_id,
            "timestamp": timestamp_ms,
            "grant_type": "client_credentials",
            "client_secret_sign": client_secret_sign_b64,
            "type": "SELF",
        }
        resp = self.session.post(url, data=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("access_token", "") or ""

    def make_request(
        self,
        method: str,
        endpoint: str,
        auth_header: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Union[str, Dict[str, Any]]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """범용 API 요청 메서드"""
        url = f"{self.base_url}{endpoint}"
        request_headers = {"Authorization": auth_header, "Accept": "application/json"}
        if headers:
            request_headers.update(headers)
        
        if isinstance(data, dict):
            data = json.dumps(data)
            request_headers.setdefault('Content-Type', 'application/json')

        resp = self.session.request(
            method=method.upper(),
            url=url,
            headers=request_headers,
            params=params,
            data=data,
            timeout=self.timeout
        )
        
        if not resp.ok:
            error_msg = f"API request failed with status {resp.status_code}"
            try:
                error_body = resp.json()
                error_msg += f": {json.dumps(error_body, ensure_ascii=False)}"
            except:
                error_msg += f": {resp.text}"
            raise requests.exceptions.HTTPError(error_msg)
        
        return resp.json()


class NaverStoreApi:
    def __init__(self, client: NaverStoreClient, client_id: str, client_secret_bcrypt_salt: str):
        self.client = client
        self.client_id = client_id
        self.client_secret = client_secret_bcrypt_salt
        self._cached_token: Optional[str] = None
        self._token_scope_key: Optional[str] = None

    def _make_client_secret_sign(self, timestamp_ms: int) -> str:
        password = f"{self.client_id}_{timestamp_ms}"
        hashed = bcrypt.hashpw(password.encode("utf-8"), self.client_secret.encode("utf-8"))
        return base64.standard_b64encode(hashed).decode("utf-8")

    def get_access_token(self) -> str:
        ts_ms = int(time.time() * 1000)
        sign = self._make_client_secret_sign(ts_ms)
        return self.client.get_access_token(self.client_id, ts_ms, sign)

    def ensure_token_for_scope(self, scope_key: str) -> str:
        if (self._cached_token is None) or (self._token_scope_key != scope_key):
            self._cached_token = self.get_access_token()
            self._token_scope_key = scope_key
        return self._cached_token

    @staticmethod
    def make_auth_header(token: str) -> str:
        return f"Bearer {token}"


class NaverApiOperator(BaseOperator):
    template_fields = ('endpoint', 'data', 'parameters', 'headers')
    template_ext = ()

    def __init__(
            self,
            endpoint: str,
            method: str = 'GET',
            data: Optional[Union[str, Dict[str, Any]]] = None,
            parameters: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, str]] = None,
            fetch_all: bool = False,
            pagination_config: Optional[Dict[str, Any]] = None,
            response_check: Optional[callable] = None,
            response_filter: Optional[callable] = None,
            log_response: bool = True,
            timeout: int = 30,
            conn_id: str = "naver_api_connection",
            **kwargs,
    ):
        super().__init__(**kwargs)

        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data
        self.parameters = parameters or {}
        self.headers = headers or {}
        self.fetch_all = fetch_all
        self.pagination_config = pagination_config or {}
        self.response_check = response_check
        self.response_filter = response_filter
        self.log_response = log_response
        self.timeout = timeout
        self.conn_id = conn_id

    def _get_naver_api_client(self) -> NaverStoreApi:
        try:
            conn = BaseHook.get_connection(self.conn_id)
            base_url = conn.host
            client_id = conn.login  # connection의 login 필드에 client_id 저장
            client_secret = conn.password  # connection의 password 필드에 client_secret 저장
            
            if not base_url or not client_id or not client_secret:
                raise AirflowException(f"Connection '{self.conn_id}' missing required fields: host, login, or password")
            
            client = NaverStoreClient(base_url, self.timeout)
            return NaverStoreApi(client, client_id, client_secret)
            
        except Exception as e:
            raise AirflowException(f"Failed to get connection '{self.conn_id}': {str(e)}")

    def _make_single_request(self, api: NaverStoreApi, auth_header: str) -> Dict[str, Any]:
        self.log.info(f"Making {self.method} request to {self.endpoint}")
        self.log.info(f"Parameters: {self.parameters}")
        self.log.info(f"Data: {self.data}")

        response_data = api.client.make_request(
            method=self.method,
            endpoint=self.endpoint,
            auth_header=auth_header,
            params=self.parameters,
            data=self.data,
            headers=self.headers
        )

        if self.log_response:
            self.log.info(f"Response: {json.dumps(response_data, ensure_ascii=False)[:1000]}...")

        if self.response_check:
            if not self.response_check(response_data):
                raise AirflowException("Response check failed")

        if self.response_filter:
            response_data = self.response_filter(response_data)

        return response_data

    def _make_paginated_requests(self, api: NaverStoreApi, auth_header: str) -> List[Dict[str, Any]]:
        self.log.info(f"Making paginated {self.method} requests to {self.endpoint}")

        page_param = self.pagination_config.get('page_param', 'page')
        page_size_param = self.pagination_config.get('page_size_param', 'pageSize')
        page_size = self.pagination_config.get('page_size', 100)
        start_page = self.pagination_config.get('start_page', 1)
        has_next_key = self.pagination_config.get('has_next_key', 'data.pagination.hasNext')
        contents_key = self.pagination_config.get('contents_key', 'data.contents')
        
        all_results = []
        page = start_page
        has_next = True
        
        while has_next:
            current_params = self.parameters.copy()
            current_params[page_param] = page
            current_params[page_size_param] = page_size
            
            self.log.info(f"Fetching page {page} with {page_size} items")
            
            try:
                response_data = api.client.make_request(
                    method=self.method,
                    endpoint=self.endpoint,
                    auth_header=auth_header,
                    params=current_params,
                    data=self.data,
                    headers=self.headers
                )
            except requests.exceptions.HTTPError as e:
                self.log.error(f"Page {page} API request failed: {str(e)}")
                raise
            
            if self.log_response:
                self.log.info(f"Page {page} response: {json.dumps(response_data, ensure_ascii=False)[:500]}...")

            if self.response_check:
                if not self.response_check(response_data):
                    raise AirflowException("Response check failed")

            has_next = self._get_nested_value(response_data, has_next_key, False)

            contents = self._get_nested_value(response_data, contents_key, [])
            if contents:
                all_results.extend(contents)
                self.log.info(f"Page {page}: {len(contents)} items collected, total: {len(all_results)}")
            else:
                self.log.info(f"Page {page}: No items found")
                has_next = False
            
            page += 1
            
            # 무한 루프 방지
            if page > 1000:
                self.log.warning("Maximum page limit reached (1000), stopping pagination")
                break
        
        self.log.info(f"Pagination completed: {len(all_results)} total items collected")
        
        if self.response_filter:
            all_results = self.response_filter(all_results)
        
        return all_results

    def _get_nested_value(self, data: Dict[str, Any], key_path: str, default: Any = None) -> Any:
        keys = key_path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current

    def execute(self, context: Any) -> Any:
        api = self._get_naver_api_client()

        execution_date = context.get('execution_date', datetime.now())
        if isinstance(execution_date, str):
            execution_date = datetime.fromisoformat(execution_date.replace('Z', '+00:00'))
        
        scope_key = execution_date.strftime("%Y-%m-%d")
        token = api.ensure_token_for_scope(scope_key)
        auth_header = api.make_auth_header(token)


        if self.fetch_all:
            result = self._make_paginated_requests(api, auth_header)
        else:
            result = self._make_single_request(api, auth_header)
        
        self.log.info(f"API call completed successfully")
        return result