"""
S3 Data Processing Operator for Airflow

This operator provides functionality to process data from S3 files,
including JSON parsing, data transformation, and temporary file management.
"""

import json
import gzip
import tempfile
import pickle
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException


@dataclass
class ReviewDto:
    platform: str
    review_id: str
    review_writer: str
    review_score: float
    review_content: str
    attach_urls: List[str]
    product_id: str
    product_name: str
    is_best_review: bool
    created_at: str
    created_day: str
    helpful_count: int
    sentiment: str = "NEUTRAL"
    sentiment_score: float = 0.0
    category_id: Optional[str] = None

class S3NaverReviewProcessorOperator(BaseOperator):
    """
    S3 Data Processing Operator for handling S3 files and data transformation.

    :param aws_conn_id: AWS connection ID
    :param bucket_name: S3 bucket name
    :param s3_keys: List of S3 keys to process
    :param data_transformer: Function to transform raw data to DTOs
    :param output_format: Output format ('pickle', 'json')
    """

    template_fields = ('s3_keys', 'bucket_name')
    ui_color = '#FF9800'

    def __init__(
            self,
            aws_conn_id: str,
            bucket_name: str,
            s3_keys: List[str],
            data_transformer: Optional[callable] = None,
            output_format: str = 'pickle',
            **kwargs
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.s3_keys = s3_keys
        self.data_transformer = data_transformer or self._default_transformer
        self.output_format = output_format

    def _default_transformer(self, content: dict) -> ReviewDto:
        """Default transformer for review data."""
        attaches = content.get("reviewAttaches") or []
        if isinstance(attaches, dict):
            attaches = [attaches]
        attach_urls = [a.get("attachUrl") for a in attaches if isinstance(a, dict) and a.get("attachUrl")]

        create_date = content.get("createDate") or ""
        created_day = create_date[:10] if len(create_date) >= 10 else ""

        return ReviewDto(
            platform="NAVER_SMART_STORE",
            review_id=str(content.get("id", "")),
            review_writer=content.get("maskedWriterId", ""),
            review_score=float(content.get("reviewScore") or 0.0),
            review_content=content.get("reviewContent", ""),
            attach_urls=attach_urls,
            product_id=str(content.get("productNo", "")),
            product_name=content.get("productName", ""),
            is_best_review=bool(content.get("bestReview") or False),
            created_at=create_date,
            created_day=created_day,
            helpful_count=int(content.get("helpCount") or 0),
        )

    def _read_s3_object(self, hook: S3Hook, key: str) -> str:
        """Read S3 object and return as string."""
        obj = hook.get_key(key=key, bucket_name=self.bucket_name)
        if obj is None:
            raise FileNotFoundError(f"S3 object not found: s3://{self.bucket_name}/{key}")
        
        body = obj.get()["Body"].read()
        if key.endswith(".gz"):
            return gzip.decompress(body).decode("utf-8")
        return body.decode("utf-8")

    def execute(self, context: Dict[str, Any]) -> str:
        """Execute S3 data processing."""
        if not self.s3_keys:
            self.log.warning("No S3 keys provided for processing")
            return None

        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        dtos: List[ReviewDto] = []
        processed_files = 0

        for key in self.s3_keys:
            if not key.endswith(".json"):
                continue

            try:
                payload_str = self._read_s3_object(hook, key)
                data = json.loads(payload_str)

                contents = data.get("contents")
                if not isinstance(contents, list):
                    continue

                for content in contents:
                    if isinstance(content, dict):
                        dto = self.data_transformer(content)
                        dtos.append(dto)

                processed_files += 1

            except Exception as e:
                self.log.error(f"Error processing {key}: {e}")
                continue

        # Return data directly or save to temporary file
        if self.output_format == 'json':
            # Return data directly as list of dictionaries
            result_data = [asdict(x) for x in dtos]
            self.log.info(f"Processed {len(dtos)} reviews from {processed_files} files (total keys: {len(self.s3_keys)})")
            return result_data
        elif self.output_format == 'pickle':
            # Save to temporary file for pickle format
            tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
            with open(tmp_file.name, "wb") as f:
                pickle.dump([asdict(x) for x in dtos], f)
            self.log.info(f"Processed {len(dtos)} reviews from {processed_files} files (total keys: {len(self.s3_keys)})")
            return tmp_file.name
        else:
            raise AirflowException(f"Unsupported output format: {self.output_format}")