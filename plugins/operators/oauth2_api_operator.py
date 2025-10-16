"""
OAuth2 API Operator for Airflow

This operator handles OAuth2 client credentials flow and makes authenticated API calls.
It retrieves tokens automatically and handles token refresh as needed.
"""

import json
import logging
from typing import Any, Dict, Optional, Union
from datetime import datetime, timedelta
import pytz

from lazy_object_proxy import Proxy
import requests
from requests.auth import AuthBase
from airflow.models import BaseOperator, Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class OAuth2ClientCreds(AuthBase):
    """
    OAuth2 Client Credentials authentication handler.

    This class handles OAuth2 token retrieval and automatic token refresh.
    The token URL is automatically retrieved from the Airflow connection's host field.
    """

    def __init__(self, conn_id: str, scope: str = None, token_expiry_buffer: int = 60):
        """
        Initialize OAuth2 client credentials handler.

        :param conn_id: Airflow connection ID containing client_id, client_secret, and host (token URL)
        :param scope: Optional scope for the token request
        :param token_expiry_buffer: Buffer time in seconds before token expiry to refresh
        """
        self.conn_id = conn_id
        self.scope = scope
        self.token_expiry_buffer = token_expiry_buffer
        self._token = None
        self._token_expires_at = None
        self.logger = logging.getLogger(__name__)

        # Retrieve token URL from Airflow connection host field
        try:
            conn = BaseHook.get_connection(self.conn_id)
            self.token_url = conn.host
            if not self.token_url:
                raise AirflowException(f"Connection '{self.conn_id}' does not have a host field set")
        except Exception as e:
            raise AirflowException(f"Failed to retrieve token URL from connection '{self.conn_id}': {str(e)}")

    def _get_token(self) -> str:
        """Retrieve OAuth2 access token using client credentials flow."""
        try:
            # Get connection details from Airflow
            conn = BaseHook.get_connection(self.conn_id)
            username = conn.login
            password = conn.password

            if not username or not password:
                raise AirflowException(f"Connection {self.conn_id} missing client_id or client_secret")

            # Prepare token request data
            data = {
                "grant_type": "password",
                "client_id": "commerce-os",
                "username": username,
                "password": password,
            }
            if self.scope:
                data["scope"] = self.scope

            self.logger.info(f"Requesting OAuth2 token from {self.token_url}")


            # Make token request
            response = requests.post(
                self.token_url,
                data=data,
                timeout=30
            )
            response.raise_for_status()

            token_data = response.json()
            self._token = token_data["access_token"]

            # Calculate token expiry time
            expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in - self.token_expiry_buffer)

            self.logger.info(f"OAuth2 token retrieved successfully, expires at {self._token_expires_at}")
            return self._token

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to retrieve OAuth2 token: {str(e)}")
            raise AirflowException(f"OAuth2 token request failed: {str(e)}")
        except KeyError as e:
            self.logger.error(f"Invalid token response format: {str(e)}")
            raise AirflowException(f"Invalid OAuth2 token response: {str(e)}")

    def _is_token_expired(self) -> bool:
        """Check if the current token is expired or about to expire."""
        if not self._token or not self._token_expires_at:
            return True
        return datetime.now() >= self._token_expires_at

    def __call__(self, request):
        """Add Authorization header to the request."""
        if self._is_token_expired():
            self._get_token()

        request.headers["Authorization"] = f"Bearer {self._token}"
        return request


class OAuth2APIOperator(BaseOperator):
    """
    Operator that makes authenticated API calls using OAuth2 client credentials.

    This operator handles OAuth2 token retrieval and makes HTTP requests with
    proper authentication headers.

    :param oauth_conn_id: Airflow connection ID for OAuth2 credentials
    :param api_url: Base URL for API calls
    :param endpoint: API endpoint path
    :param method: HTTP method (GET, POST, PUT, DELETE, etc.)
    :param headers: Additional headers to include in the request
    :param data: Request body data (for POST/PUT requests)
    :param parameters: Query parameters
    :param scope: OAuth2 scope
    :param response_check: Function to validate response
    :param response_filter: Function to filter response data
    :param log_response: Whether to log the full response
    """

    template_fields = ('data', 'parameters', 'headers')
    template_ext = ()

    def __init__(
        self,
        api_url: str,
        endpoint: str,
        method: str = 'GET',
        oauth_conn_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Union[str, Dict[str, Any]]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        scope: Optional[str] = None,
        response_check: Optional[callable] = None,
        response_filter: Optional[callable] = None,
        log_response: bool = True,
        timeout: int = 30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.oauth_conn_id = oauth_conn_id or None
        self.api_url = api_url.rstrip('/')
        self.endpoint = endpoint
        self.method = method.upper()
        self.headers = headers or {}
        self.data = data
        self.parameters = parameters or {}
        self.scope = scope
        self.response_check = response_check
        self.response_filter = response_filter
        self.log_response = log_response
        self.timeout = timeout

        # Initialize OAuth2 authentication
        if self.oauth_conn_id:
            self.oauth_auth = OAuth2ClientCreds(
                conn_id=self.oauth_conn_id,
                scope=self.scope
            )
        else:
            self.oauth_auth = None

    def execute(self, context: Any) -> Any:
        """Execute the API call with OAuth2 authentication."""
        self.log.info(f"Making {self.method} request to {self.api_url}{self.endpoint}")
        self.log.info(f"Parameters: {self.parameters}")
        self.log.info(f"Data: {self.data}")

        # Prepare request URL
        url = f"{self.api_url}{self.endpoint}"

        # Prepare headers
        headers = self.headers.copy()
        headers.setdefault('Content-Type', 'application/json')

        # Prepare request data
        request_data = self.data
        if isinstance(request_data, dict):
            request_data = json.dumps(request_data)

        try:

            # Make the authenticated request
            response = requests.request(
                method=self.method,
                url=url,
                headers=headers,
                data=request_data,
                params=self.parameters,
                auth=self.oauth_auth,
                timeout=self.timeout
            )

            self.log.info(f"Response status: {response.status_code}")

            # Log response if enabled
            if self.log_response:
                self.log.info(f"Response headers: {dict(response.headers)}")
                if response.text:
                    self.log.info(f"Response body: {response.text[:1000]}...")  # Limit log size

            # Check response status
            if not response.ok:
                raise AirflowException(f"API request failed with status {response.status_code}: {response.text}")

            # Apply response check if provided
            if self.response_check:
                if not self.response_check(response):
                    raise AirflowException("Response check failed")

            # Parse response
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = response.text

            # Apply response filter if provided
            if self.response_filter:
                response_data = self.response_filter(response_data)

            self.log.info(f"API call completed successfully")
            return response_data

        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {str(e)}")
            raise AirflowException(f"API request failed: {str(e)}")


class BlackSellerDownloadOperator(OAuth2APIOperator):
    """
    Specialized operator for downloading black seller data via OAuth2 API.

    This operator is specifically designed for the black seller CSV download endpoint.
    All configuration (API URL, OAuth connection, token URL) is automatically retrieved from Airflow Variables.
    """

    template_fields = ('parameters',)

    def __init__(
        self,
        oauth_conn_id: str,
        company_id: Union[str, int],
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        # Retrieve configuration from Airflow Variables
        try:
            api_url = Variable.get('black_seller_host')
        except Exception as e:
            raise AirflowException(f"Failed to retrieve configuration from Airflow Variables: {str(e)}")

        # Build the endpoint for black seller CSV download
        endpoint = f"/api/v1/cosCompanies/{company_id}/black-sellers/download-csv"

        # Store company_id for later use in execute method
        self.company_id = company_id
        self.log.info(f"Parameters: {parameters}")

        super().__init__(
            oauth_conn_id=oauth_conn_id,
            api_url=api_url,
            endpoint=endpoint,
            method='GET',
            parameters=parameters or {},
            response_check=lambda response: response.status_code == 200,
            **kwargs
        )

    def execute(self, context):
        # Update the parameters for the API call
        self.parameters = self.validate_parameters(self.parameters)

        # Call the parent execute method
        return super().execute(context)

    def validate_parameters(self, parameters: Dict[str, Union[str, Proxy]]) -> Dict[str, Any]:
        """
        Validate the parameters.
        """
        if 'sortType' not in parameters:
            parameters['sortType'] = 'BLACK_SELLER_CANDIDATE_UPDATED_AT'

        if 'startDateTime' in parameters and 'endDateTime' in parameters:
            if not parameters['startDateTime']:
                self.log.info(f"Start date is None, setting to 2025-08-27T00:00:00")
                parameters['startDateTime'] = '2025-08-27T00:00:00'  # 블랙셀러 데이터 최초 수집일자

            parameters['startDateTime'] = self._normalize_date(parameters['startDateTime'])
            parameters['endDateTime'] = self._normalize_date(parameters['endDateTime'])

            if parameters['startDateTime'] > parameters['endDateTime']:
                raise AirflowException("Start date must be before end date")

        return parameters

    def _normalize_date(self, datetime_input: Union[str, Proxy]) -> Union[str, Proxy]:
        """
        Normalize datetime input to yyyy-mm-dd HH:MM:SS format in KST.

        :param datetime_input: Datetime string in various formats
        :return: Datetime string in yyyy-mm-dd HH:MM:SS format in KST
        """

        KST = pytz.timezone('Asia/Seoul')

        if not datetime_input:
            raise AirflowException("Date input cannot be empty")

        if isinstance(datetime_input, Proxy):
            datetime_input = datetime_input.isoformat()

        try:
            dt = datetime.strptime(datetime_input, "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError:
            # Try without microseconds
            try:
                dt = datetime.strptime(datetime_input, "%Y-%m-%dT%H:%M:%S%z")
            except ValueError:
                # Try naive datetime (no tzinfo) - treat as UTC
                try:
                    dt = datetime.strptime(datetime_input, "%Y-%m-%dT%H:%M:%S.%f")
                except ValueError:
                    dt = datetime.strptime(datetime_input, "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=pytz.UTC)

        # Convert to Asia/Seoul (KST)
        dt_kst = dt.astimezone(KST)
        return dt_kst.strftime('%Y-%m-%d %H:%M:%S')


class PriceTrackerDownloadOperator(OAuth2APIOperator):
    """
    Specialized operator for downloading price tracker data via OAuth2 API.

    This operator is specifically designed for the price tracker products download endpoint.
    It makes a POST request to trigger the download process.
    All configuration (API URL, OAuth connection, token URL) is automatically retrieved from Airflow Variables.
    """

    def __init__(
        self,
        oauth_conn_id: str,
        company_id: Union[str, int],
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        # Retrieve configuration from Airflow Variables
        try:
            api_url = Variable.get('price_tracker_host')
        except Exception as e:
            raise AirflowException(f"Failed to retrieve configuration from Airflow Variables: {str(e)}")

        # Build the endpoint for price tracker products download
        endpoint = f"/api/v1/cosCompanies/{company_id}/price-tracker/products/download"

        # Store company_id for later use in execute method
        self.company_id = company_id

        super().__init__(
            oauth_conn_id=oauth_conn_id,
            api_url=api_url,
            endpoint=endpoint,
            method='POST',
            parameters=parameters or {},
            response_check=lambda response: response.status_code == 200,
            **kwargs
        )

class SalesDownloadOperator(OAuth2APIOperator):
    """
    Specialized operator for downloading sales data via OAuth2 API.

    This operator is specifically designed for the sales download endpoint.
    It makes a POST request to trigger the download process.
    All configuration (API URL, OAuth connection, token URL) is automatically retrieved from Airflow Variables.
    """

    template_fields = ('data',)

    def __init__(
        self,
        company_str: str,
        type: str, # total, category, brand, sku_daily, sku_monthly
        data: Optional[Union[str, Dict[str, Any]]] = None,
        oauth_conn_id: Optional[str] = None,
        **kwargs,
    ):

        try:
            api_url = Variable.get('sales_host')
        except Exception as e:
            raise AirflowException(f"Failed to retrieve configuration from Airflow Variables: {str(e)}")

        # Build the endpoint for sales download
        match type:
            case 'total':
                endpoint = f"/{company_str}/v1/dashboard/sales"
            case 'category':
                endpoint = f"/{company_str}/v1/dashboard/sales/group"
                data.update({'type': 'CATEGORY'})
            case 'brand':
                endpoint = f"/{company_str}/v1/dashboard/sales/group"
                data.update({'type': 'BRAND'})
            case 'sku_daily':
                endpoint = f"/{company_str}/v1/dashboard/sales/daily/product"
            case 'sku_monthly':
                endpoint = f"/{company_str}/v1/dashboard/sales/daily"
            case _:
                raise AirflowException(f"Invalid type: {type}")

        super().__init__(
            oauth_conn_id=oauth_conn_id or None,
            api_url=api_url,
            endpoint=endpoint,
            method='POST',
            data=data,
            response_check=lambda response: response.status_code == 200,
            **kwargs
        )

    def execute(self, context):
        """
        Execute the sales download with proper date normalization.

        This method normalizes the date after Airflow template rendering,
        ensuring that template variables like {{ ds_nodash }} are properly resolved.
        """
        # Get the rendered data from the parent class
        rendered_data = self.data

        # Store normalized date for later use
        normalized_date = None

        # Normalize the date if it exists
        if 'date' in rendered_data and rendered_data['date']:
            normalized_date = self._normalize_date(rendered_data['date'])
            rendered_data['date'] = normalized_date
            self.log.info(f"Normalized date to: {normalized_date}")

        # Update the data for the API call
        self.data = rendered_data

        # Call the parent execute method to get API response
        api_response = super().execute(context)

        # Add the normalized date to the response if available
        if normalized_date:
            api_response['_date'] = normalized_date

        return api_response


    def _normalize_date(self, date_input: str) -> str:
        """
        Normalize date input to yyyymmdd format.

        Handles various input formats:
        - yyyy-mm-dd -> yyyymmdd
        - yyyymmdd -> yyyymmdd (already correct)
        - yymmdd -> 20yymmdd (assumes 20xx century)
        - yyyy-mm-dd HH:MM:SS -> yyyymmdd (extracts date part)

        :param date_input: Date string in various formats
        :return: Date string in yyyymmdd format
        :raises AirflowException: If date format is not recognized
        """
        if not date_input:
            raise AirflowException("Date input cannot be empty")

        date_str = str(date_input).strip()

        try:
            # Handle yyyy-mm-dd format
            if '-' in date_str:
                # Extract date part if there's time component
                date_part = date_str.split(' ')[0]
                parsed_date = datetime.strptime(date_part, '%Y-%m-%d')
                return parsed_date.strftime('%Y%m%d')

            # Handle yyyymmdd format (already correct)
            elif len(date_str) == 8 and date_str.isdigit():
                # Validate it's a real date
                datetime.strptime(date_str, '%Y%m%d')
                return date_str

            # Handle yymmdd format (assume 20xx century)
            elif len(date_str) == 6 and date_str.isdigit():
                year = int('20' + date_str[:2])
                month = int(date_str[2:4])
                day = int(date_str[4:6])
                parsed_date = datetime(year, month, day)
                return parsed_date.strftime('%Y%m%d')

            # Handle other formats by trying to parse as datetime
            else:
                # Try common datetime formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%Y%m%d')
                    except ValueError:
                        continue

                raise ValueError(f"Unrecognized date format: {date_str}")

        except ValueError as e:
            raise AirflowException(f"Invalid date format '{date_input}': {str(e)}")
        except Exception as e:
            raise AirflowException(f"Error processing date '{date_input}': {str(e)}")


class StockDownloadOperator(OAuth2APIOperator):
    """
    Specialized operator for downloading stock data via OAuth2 API.

    This operator is specifically designed for the stock download endpoint.
    It makes a POST request to trigger the download process.
    All configuration (API URL, OAuth connection, token URL) is automatically retrieved from Airflow Variables.
    """

    template_fields = ('data',)

    def __init__(
            self,
            company_str: str,
            type: str,  # qty, amt, vf, total_qty, sku_daily
            data: Optional[Union[str, Dict[str, Any]]] = None,
            oauth_conn_id: Optional[str] = None,
            **kwargs,
    ):

        try:
            api_url = Variable.get('stock_host')
        except Exception as e:
            raise AirflowException(f"Failed to retrieve configuration from Airflow Variables: {str(e)}")

        # Build the endpoint for stock download
        match type:
            case 'qty':
                endpoint = f"/{company_str}/v1/dashboard/stocks"
                data.update({'type': 'QTY'})
            case 'amt':
                endpoint = f"/{company_str}/v1/dashboard/stocks"
                data.update({'type': 'AMT'})
            case 'vf':
                endpoint = f"/{company_str}/v1/dashboard/stocks"
                data.update({'type': 'VF'})
            case 'total_qty':
                endpoint = f"/{company_str}/v1/dashboard/stocks"
                data.update({'type': 'TOTAL_QTY'})
            case 'sku_daily':
                endpoint = f"/{company_str}/v1/dashboard/stocks/product"
            case _:
                raise AirflowException(f"Invalid type: {type}")

        super().__init__(
            oauth_conn_id=oauth_conn_id or None,
            api_url=api_url,
            endpoint=endpoint,
            method='POST',
            data=data,
            response_check=lambda response: response.status_code == 200,
            **kwargs
        )

    def execute(self, context):
        """
        Execute the stock download with proper date normalization.

        This method normalizes the date after Airflow template rendering,
        ensuring that template variables like {{ ds_nodash }} are properly resolved.
        """
        # Get the rendered data from the parent class
        rendered_data = self.data

        # Store normalized date for later use
        normalized_date = None

        # Normalize the date if it exists
        if 'date' in rendered_data and rendered_data['date']:
            normalized_date = self._normalize_date(rendered_data['date'])
            rendered_data['date'] = normalized_date
            self.log.info(f"Normalized date to: {normalized_date}")

        # Update the data for the API call
        self.data = rendered_data

        # Call the parent execute method to get API response
        api_response = super().execute(context)

        # Add the normalized date to the response if available
        if normalized_date:
            api_response['_date'] = normalized_date

        return api_response

    def _normalize_date(self, date_input: str) -> str:
        """
        Normalize date input to yyyymmdd format.

        Handles various input formats:
        - yyyy-mm-dd -> yyyymmdd
        - yyyymmdd -> yyyymmdd (already correct)
        - yymmdd -> 20yymmdd (assumes 20xx century)
        - yyyy-mm-dd HH:MM:SS -> yyyymmdd (extracts date part)

        :param date_input: Date string in various formats
        :return: Date string in yyyymmdd format
        :raises AirflowException: If date format is not recognized
        """
        if not date_input:
            raise AirflowException("Date input cannot be empty")

        date_str = str(date_input).strip()

        try:
            # Handle yyyy-mm-dd format
            if '-' in date_str:
                # Extract date part if there's time component
                date_part = date_str.split(' ')[0]
                parsed_date = datetime.strptime(date_part, '%Y-%m-%d')
                return parsed_date.strftime('%Y%m%d')

            # Handle yyyymmdd format (already correct)
            elif len(date_str) == 8 and date_str.isdigit():
                # Validate it's a real date
                datetime.strptime(date_str, '%Y%m%d')
                return date_str

            # Handle yymmdd format (assume 20xx century)
            elif len(date_str) == 6 and date_str.isdigit():
                year = int('20' + date_str[:2])
                month = int(date_str[2:4])
                day = int(date_str[4:6])
                parsed_date = datetime(year, month, day)
                return parsed_date.strftime('%Y%m%d')

            # Handle other formats by trying to parse as datetime
            else:
                # Try common datetime formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%Y%m%d')
                    except ValueError:
                        continue

                raise ValueError(f"Unrecognized date format: {date_str}")

        except ValueError as e:
            raise AirflowException(f"Invalid date format '{date_input}': {str(e)}")
        except Exception as e:
            raise AirflowException(f"Error processing date '{date_input}': {str(e)}")


class CoupangDPSDownloadOperator(OAuth2APIOperator):
    """
    Specialized operator for downloading Coupang DPS data via OAuth2 API.

    This operator is specifically designed for the Coupang DPS download endpoint.
    It makes a POST request to trigger the download process.
    All configuration (API URL, OAuth connection, token URL) is automatically retrieved from Airflow Variables.
    """

    def __init__(
        self,
        oauth_conn_id: str,
        company_id: Union[str, int],
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        try:
            api_url = Variable.get('coupang_dps_host')
        except Exception as e:
            raise AirflowException(f"Failed to retrieve configuration from Airflow Variables: {str(e)}")

        endpoint = f"/api/v1/cosCompanies/{company_id}/price-changes"

        self.company_id = company_id

        super().__init__(
            oauth_conn_id=oauth_conn_id,
            api_url=api_url,
            endpoint=endpoint,
            method='GET',
            parameters=parameters or {},
            response_check=lambda response: response.status_code == 200,
            **kwargs
        )