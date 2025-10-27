"""
JSON to MongoDB Operator for Airflow

This operator transforms JSON data from API responses into MongoDB-ready documents.
It provides a flexible base class for handling various JSON structures.
"""

from abc import abstractmethod
import json
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException, AirflowSkipException


class JSONToMongoOperator(BaseOperator):
    """
    Base operator that transforms JSON data into MongoDB-ready documents.

    This operator provides a flexible framework for converting JSON data
    from API responses into MongoDB document format.
    """

    def __init__(
        self,
        source_task_id: str,
        collection_name: str = "json_data",
        add_metadata: bool = True,
        metadata_fields: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """
        Initialize JSON to MongoDB operator.

        :param source_task_id: Task ID that provides the JSON data
        :param collection_name: MongoDB collection name for the documents
        :param add_metadata: Whether to add metadata fields
        :param metadata_fields: Custom metadata fields to add
        """
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.collection_name = collection_name
        self.add_metadata = add_metadata
        self.metadata_fields = metadata_fields or {}

    def validate_json_data(self, data: Any) -> bool:
        """
        Validate that the data is valid JSON structure.

        :param data: Data to validate
        :return: True if valid, False otherwise
        """
        try:
            if isinstance(data, str):
                json.loads(data)
            elif isinstance(data, (dict, list)):
                json.dumps(data)  # Test serialization
            else:
                return False
            return True
        except (json.JSONDecodeError, TypeError):
            return False

    def _extract_clean_data(self, data: Any, key: str = 'data') -> List[Dict[str, Any]]:
        """
        Extract and clean data from the JSON data.

        :param data: Raw JSON data
        :param key: Key to extract data from
        :return: List of MongoDB-ready documents
        """
        if isinstance(data, dict):
            # Single document
            if key in data and isinstance(data[key], list):
                # API response with data array
                documents = data[key]
            else:
                # Single document
                documents = [data]
        elif isinstance(data, list):
            # Array of documents
            all_have_specified_key = all(
                isinstance(item, dict) and key in item and isinstance(item[key], list)
                for item in data
            )

            if all_have_specified_key:
                # Extract and merge all specified key arrays from each element
                documents = []
                for item in data:
                    if isinstance(item[key], list):
                        documents.extend(item[key])
                self.log.info(f"Merged data from {len(data)} elements, total documents: {len(documents)}")
            else:
                # Array of documents (no specified key structure)
                documents = data
        else:
            raise AirflowException("Unsupported JSON structure")
        return documents

    def normalize_field_name(self, field_name: str) -> str:
        """
        Normalize field names for MongoDB compatibility.

        :param field_name: Original field name
        :return: Normalized field name
        """
        # Replace camelCase with snake_case
        import re
        # Insert underscore before uppercase letters
        normalized = re.sub('([a-z0-9])([A-Z])', r'\1_\2', field_name)
        return normalized.lower()

    def transform_value(self, value: Any) -> Any:
        """
        Transform individual values for MongoDB compatibility.

        :param value: Value to transform
        :return: Transformed value
        """
        if isinstance(value, dict):
            return self.transform_document(value)
        elif isinstance(value, list):
            return [self.transform_value(item) for item in value]
        elif isinstance(value, str):
            # Handle potential numeric strings
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            return value

    def transform_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single document for MongoDB.

        :param document: Document to transform
        :return: Transformed document
        """
        transformed = {}

        for key, value in document.items():
            normalized_key = self.normalize_field_name(key)
            transformed_value = self.transform_value(value)
            transformed[normalized_key] = transformed_value

        return transformed

    def add_document_metadata(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add metadata to a document.

        :param document: Document to add metadata to
        :return: Document with metadata
        """
        if not self.add_metadata:
            return document

        metadata = {
            "_processed_at": datetime.now(),
        }

        # Add custom metadata fields
        metadata.update(self.metadata_fields)

        # Add metadata to document
        document.update(metadata)
        return document

    def transform_json_data(self, raw_data: Any) -> List[Dict[str, Any]]:
        """
        Transform raw JSON data into MongoDB documents.

        :param raw_data: Raw JSON data
        :return: List of MongoDB-ready documents
        """
        if not self.validate_json_data(raw_data):
            raise AirflowException("Invalid JSON data format")

        # Parse JSON string if needed
        if isinstance(raw_data, str):
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                raise AirflowException(f"Failed to parse JSON: {e}")
        else:
            data = raw_data

        transformed_documents = []

        documents = self._extract_clean_data(data)

        for i, doc in enumerate(documents):
            try:
                if not isinstance(doc, dict):
                    self.log.warning(f"Skipping non-dict item at index {i}")
                    continue

                # Transform the document
                transformed_doc = self.transform_document(doc)

                # Add metadata
                final_doc = self.add_document_metadata(transformed_doc)

                transformed_documents.append(final_doc)

            except Exception as e:
                self.log.error(f"Error transforming document at index {i}: {e}")
                continue

        self.log.info(f"Successfully transformed {len(transformed_documents)} documents")
        return transformed_documents

    def execute(self, context):
        """
        Execute the JSON data transformation.

        :param context: Airflow context
        :return: Transformed JSON data documents
        """
        self.log.info(f"Starting JSON data transformation from task: {self.source_task_id}")

        # Get data from previous task
        try:
            raw_data = context['ti'].xcom_pull(task_ids=self.source_task_id)
            if raw_data is None:
                raise AirflowException(f"No data found from task: {self.source_task_id}")
        except Exception as e:
            raise AirflowException(f"Failed to retrieve data from {self.source_task_id}: {e}")

        # Transform the data
        try:
            transformed_data = self.transform_json_data(raw_data)

            if not transformed_data:
                self.log.warning("No valid JSON data found after transformation")
                raise AirflowSkipException("No valid JSON data to process")

            self.log.info(f"JSON data transformation completed. Generated {len(transformed_data)} documents")
            return transformed_data

        except AirflowSkipException:
            # Re-raise AirflowSkipException as-is
            raise
        except Exception as e:
            self.log.error(f"JSON data transformation failed: {e}")
            raise AirflowException(f"JSON data transformation failed: {e}")


class SalesJsonToMongoOperator(JSONToMongoOperator):
    """
    Specialized operator for transforming sales JSON data into MongoDB documents.

    This operator inherits from JSONToMongoOperator and adds sales-specific
    field inference and transformation logic.
    """

    def __init__(
        self,
        source_task_id: str,
        collection_name: str = "sales_data",
        **kwargs,
    ):
        """
        Initialize Sales JSON to MongoDB operator.

        :param source_task_id: Task ID that provides the sales JSON data
        :param collection_name: MongoDB collection name for sales documents
        """
        super().__init__(
            source_task_id=source_task_id,
            collection_name=collection_name,
            add_metadata=True,
            **kwargs
        )

    def transform_json_data(self, raw_data: Any) -> List[Dict[str, Any]]:
        """
        Transform raw JSON data into MongoDB documents.

        :param raw_data: Raw JSON data
        :return: List of MongoDB-ready documents
        """
        if not self.validate_json_data(raw_data):
            raise AirflowException("Invalid JSON data format")

        # Parse JSON string if needed
        if isinstance(raw_data, str):
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                raise AirflowException(f"Failed to parse JSON: {e}")
        else:
            data = raw_data

        # Initialize the list to collect all transformed documents
        all_transformed_documents = []

        # Handle different data structures
        if isinstance(data, list):
            # Handle list of items (each item may have its own date)
            for item in data:
                if not isinstance(item, dict):
                    self.log.warning("Skipping non-dict item in data list")
                    continue

                # Extract documents and date from this item
                documents = item.get('data', [])
                date = item.get('_date')

                if not isinstance(documents, list):
                    self.log.warning("Expected 'data' field to be a list")
                    continue

                # Transform documents for this item
                item_transformed = self._transform_documents_batch(documents, date)
                all_transformed_documents.extend(item_transformed)

        elif isinstance(data, dict):
            # Handle single item structure
            documents = data.get('data', [])
            date = data.get('_date')

            if not isinstance(documents, list):
                # Fallback to treating the entire dict as a single document
                documents = [data]
                date = data.get('_date')

            all_transformed_documents = self._transform_documents_batch(documents, date)
        else:
            raise AirflowException(f"Unsupported data structure: {type(data)}")

        self.log.info(f"Successfully transformed {len(all_transformed_documents)} documents")
        return all_transformed_documents

    def _transform_documents_batch(self, documents: List[Dict[str, Any]], _date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Transform a batch of documents with optional date information.

        :param documents: List of documents to transform
        :param date: Optional date to add to each document
        :return: List of transformed documents
        """

        if _date:
            date_info = {"date": self._normalize_date(_date)}
        else:
            date_info = {}

        documents = self._filter_records(documents)
        self.log.info(f"Documents: {documents}")

        transformed_documents = []

        for i, doc in enumerate(documents):
            try:
                if not isinstance(doc, dict):
                    self.log.warning(f"Skipping non-dict item at index {i}")
                    continue

                # Transform the document
                transformed_doc = self.transform_document(doc)

                # Add date information if provided
                if date := transformed_doc.get("date"):
                    date_info["date"] = self._normalize_date(date)

                transformed_doc.update(date_info)

                # Add metadata
                final_doc = self.add_document_metadata(transformed_doc)

                transformed_documents.append(final_doc)

            except Exception as e:
                self.log.error(f"Error transforming document at index {i}: {e}")
                continue

        return transformed_documents

    @abstractmethod
    def _filter_records(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter documents to only include records of the target type.

        :param documents: List of documents to filter
        :return: Filtered list containing only records of the target type
        """
        pass

    @abstractmethod
    def _normalize_date(self, date: Union[str, int]) -> str:
        """
        Normalize a date string to the format YYYY-MM-DD.

        :param date: Date string to normalize
        :return: Normalized date string
        """
        pass



class DailySalesJsonToMongoOperator(SalesJsonToMongoOperator):
    """
    Specialized operator for daily sales records.

    This operator automatically filters to only include daily records
    (8-digit date format: YYYYMMDD) and excludes monthly records.
    """

    def __init__(self, **kwargs):
        """
        Initialize Daily Sales JSON to MongoDB operator.

        :param kwargs: Arguments passed to parent SalesJsonToMongoOperator
        """
        super().__init__(**kwargs)
        self.log.info("Initialized DailySalesJsonToMongoOperator - will filter for daily records only")

    def _filter_records(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter documents to only include daily records (8-digit date format: YYYYMMDD) or records with no date field.

        :param documents: List of documents to filter
        :return: Filtered list containing only daily records
        """
        daily_documents = []
        other_documents = []

        for doc in documents:
            if not isinstance(doc, dict):
                daily_documents.append(doc)
                continue

            # Check if document has a date field
            date_value = doc.get('date')
            if not date_value:
                # If no date field, include in other documents
                daily_documents.append(doc)
                continue

            # Convert to string and check format
            date_str = str(date_value).strip()

            # Daily records: 8-digit format (YYYYMMDD)
            if len(date_str) == 8 and date_str.isdigit():
                daily_documents.append(doc)
            else:
                other_documents.append(doc)

        self.log.info(f"Daily filter: {len(documents)} documents -> {len(daily_documents)} daily records, {len(other_documents)} other records")
        return daily_documents

    def _normalize_date(self, date: Union[str, int]) -> str:
        """
        Normalize a date string to the format YYYY-MM-DD.

        :param date: Date string to normalize
        :return: Normalized date string
        """
        date = str(date)

        try:
            # Try YYYYMM format first
            if len(date) == 6 and date.isdigit():
                return datetime.strptime(date, '%Y%m').strftime('%Y-%m-%d')
            # Try YYYY-MM-DD format
            elif len(date) == 10 and date.count('-') == 2:
                return date  # Already in correct format
            # Try YYYYMMDD format
            elif len(date) == 8 and date.isdigit():
                return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
            else:
                # If format is not recognized, return as-is
                self.log.warning(f"Unrecognized date format: {date}")
                return date
        except ValueError as e:
            self.log.error(f"Error parsing date '{date}': {e}")
            return date


class MonthlySalesJsonToMongoOperator(SalesJsonToMongoOperator):
    """
    Specialized operator for monthly sales records.

    This operator automatically filters to only include monthly records
    (6-digit date format: YYYYMM or 8-digit ending with '01': YYYYMM01).
    """

    def __init__(self, **kwargs):
        """
        Initialize Monthly Sales JSON to MongoDB operator.

        :param kwargs: Arguments passed to parent SalesJsonToMongoOperator
        """
        super().__init__(**kwargs)
        self.log.info("Initialized MonthlySalesJsonToMongoOperator - will filter for monthly records only")

    def _filter_records(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter documents to only include monthly records or records with no date field.

        Monthly records are identified by:
        - 6-digit format (YYYYMM)
        - 8-digit format ending with '01' (YYYYMM01)

        :param documents: List of documents to filter
        :return: Filtered list containing only monthly records
        """
        monthly_documents = []
        other_documents = []

        for doc in documents:
            if not isinstance(doc, dict):
                monthly_documents.append(doc)
                continue

            # Check if document has a date field
            date_value = doc.get('date')
            if not date_value:
                # If no date field, include in other documents
                monthly_documents.append(doc)
                continue

            # Convert to string and check format
            date_str = str(date_value).strip()

            # Monthly records: 6-digit format (YYYYMM) or 8-digit ending with '01'
            if (len(date_str) == 6 and date_str.isdigit()) or \
               (len(date_str) == 8 and date_str.isdigit() and date_str.endswith('01')):
                monthly_documents.append(doc)
            else:
                other_documents.append(doc)

        self.log.info(f"Monthly filter: {len(documents)} documents -> {len(monthly_documents)} monthly records, {len(other_documents)} other records")
        return monthly_documents

    def _normalize_date(self, date: Union[str, int]) -> str:
        """
        Normalize a date string to the format YYYY-MM.

        Handles both %Y%m (YYYYMM) and %Y-%m-%d (YYYY-MM-DD) formats.
        For monthly records, extracts year-month from full dates.

        :param date: Date string to normalize
        :return: Normalized date string in YYYY-MM format
        """
        date = str(date)

        try:
            # Try YYYYMM format first
            if len(date) == 6 and date.isdigit():
                return datetime.strptime(date, '%Y%m').strftime('%Y-%m')
            # Try YYYY-MM-DD format - extract year-month
            elif len(date) == 10 and date.count('-') == 2:
                return date[:7]  # Extract YYYY-MM from YYYY-MM-DD
            # Try YYYYMMDD format - extract year-month
            elif len(date) == 8 and date.isdigit():
                return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m')
            else:
                # If format is not recognized, return as-is
                self.log.warning(f"Unrecognized date format: {date}")
                return date
        except ValueError as e:
            self.log.error(f"Error parsing date '{date}': {e}")
            return date


class StockJsonToMongoOperator(JSONToMongoOperator):
    """
    Specialized operator for transforming stock JSON data into MongoDB documents.

    This operator inherits from JSONToMongoOperator and adds stock-specific
    field inference and transformation logic.
    """

    def __init__(
            self,
            source_task_id: str,
            collection_name: str,
            exclude_fields: Optional[List[str]] = None,
            **kwargs,
    ):
        """
        Initialize Stock JSON to MongoDB operator.

        :param source_task_id: Task ID that provides the stock JSON data
        :param collection_name: MongoDB collection name for stock documents
        :param exclude_fields: List of field names to exclude from the output documents
        """
        super().__init__(
            source_task_id=source_task_id,
            collection_name=collection_name,
            add_metadata=True,
            **kwargs
        )
        self.exclude_fields = exclude_fields or []

    def _exclude_document_fields(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove specified fields from document.

        :param document: Document to process
        :return: Document with excluded fields removed
        """
        if not self.exclude_fields:
            return document

        filtered_doc = {}
        excluded_count = 0

        for key, value in document.items():
            if key not in self.exclude_fields:
                filtered_doc[key] = value
            else:
                excluded_count += 1
                self.log.debug(f"Excluded field '{key}' from document")

        if excluded_count > 0:
            self.log.info(f"Excluded {excluded_count} fields from document")

        return filtered_doc

    def transform_json_data(self, raw_data: Any) -> List[Dict[str, Any]]:
        """
        Transform raw JSON data into MongoDB documents.

        :param raw_data: Raw JSON data
        :return: List of MongoDB-ready documents
        """
        if not self.validate_json_data(raw_data):
            raise AirflowException("Invalid JSON data format")

        # Parse JSON string if needed
        if isinstance(raw_data, str):
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                raise AirflowException(f"Failed to parse JSON: {e}")
        else:
            data = raw_data

        # Initialize the list to collect all transformed documents
        all_transformed_documents = []

        # Handle different data structures
        if isinstance(data, list):
            # Handle list of items (each item may have its own date)
            for item in data:
                if not isinstance(item, dict):
                    self.log.warning("Skipping non-dict item in data list")
                    continue

                # Extract documents and date from this item
                documents = item.get('data', [])
                date = item.get('_date')

                if not isinstance(documents, list):
                    self.log.warning("Expected 'data' field to be a list")
                    continue

                # Transform documents for this item
                item_transformed = self._transform_documents_batch(documents, date)
                all_transformed_documents.extend(item_transformed)

        elif isinstance(data, dict):
            # Handle single item structure
            documents = data.get('data', [])
            date = data.get('_date')

            if not isinstance(documents, list):
                # Fallback to treating the entire dict as a single document
                documents = [data]
                date = data.get('_date')

            all_transformed_documents = self._transform_documents_batch(documents, date)
        else:
            raise AirflowException(f"Unsupported data structure: {type(data)}")

        self.log.info(f"Successfully transformed {len(all_transformed_documents)} documents")
        return all_transformed_documents

    def _transform_documents_batch(self, documents: List[Dict[str, Any]], date: Optional[str] = None) -> List[
        Dict[str, Any]]:
        """
        Transform a batch of documents with optional date information.

        :param documents: List of documents to transform
        :param date: Optional date to add to each document
        :return: List of transformed documents
        """
        transformed_documents = []

        for i, doc in enumerate(documents):
            try:
                if not isinstance(doc, dict):
                    self.log.warning(f"Skipping non-dict item at index {i}")
                    continue

                # Transform the document
                transformed_doc = self.transform_document(doc)

                # Exclude specified fields
                filtered_doc = self._exclude_document_fields(transformed_doc)

                # Add date information if provided, but only if 'date' field doesn't already exist
                if date and 'date' not in filtered_doc:
                    filtered_doc.update({'_date': date})

                # Convert date field to YYYY-MM-DD format if it's 8 digits (YYYYMMDD)
                if date := filtered_doc.get("date"):
                    normalized_date = self._normalize_date(date)
                    if normalized_date:
                        filtered_doc["date"] = normalized_date
                        self.log.debug(f"Converted date field from '{date}' to '{normalized_date}'")
                    else:
                        # Skip documents with non-YYYYMMDD date format
                        self.log.info(f"Skipping document at index {i} due to non-YYYYMMDD date format: '{date}'")
                        continue

                # Add metadata
                final_doc = self.add_document_metadata(filtered_doc)

                transformed_documents.append(final_doc)

            except Exception as e:
                self.log.error(f"Error transforming document at index {i}: {e}")
                continue

        return transformed_documents

    def _normalize_date(self, date: Union[str, int]) -> Optional[str]:
        """
        Normalize a date string to the format YYYY-MM-DD.
        Only processes 8-digit YYYYMMDD format, skips all other formats.

        :param date: Date string to normalize
        :return: Normalized date string in YYYY-MM-DD format, or None if not YYYYMMDD format
        """
        date = str(date)

        try:
            # Only process 8-digit YYYYMMDD format
            if len(date) == 8 and date.isdigit():
                return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
            else:
                # Skip all other formats
                self.log.debug(f"Skipping non-YYYYMMDD date format: '{date}'")
                return None
        except ValueError as e:
            self.log.error(f"Error parsing date '{date}': {e}")
            return None


class CoupangDPSJsonToMongoOperator(JSONToMongoOperator):
    """
    Specialized operator for transforming Coupang DPS JSON data into MongoDB documents.

    This operator inherits from JSONToMongoOperator and adds Coupang DPS-specific
    field inference and transformation logic.
    """

    def __init__(
        self,
        source_task_id: str,
        collection_name: str = "coupang__dps_data",
        **kwargs,
    ):
        super().__init__(
            source_task_id=source_task_id,
            collection_name=collection_name,
            add_metadata=True,
            **kwargs
        )

    def add_document_metadata(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add metadata to a document.

        :param document: Document to add metadata to
        :return: Document with metadata
        """
        if not self.add_metadata:
            return document

        metadata = {
            "_processed_at": datetime.now(timezone.utc),
            "date": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        }

        # Add custom metadata fields
        metadata.update(self.metadata_fields)

        # Add metadata to document
        document.update(metadata)
        return document