"""
Custom MongoDB Operator for Airflow

This operator provides a clean interface for MongoDB operations in Airflow DAGs.
It supports various operations like insert, update, delete, and find.
"""

import re
from typing import Any, Dict, List, Optional, Union
from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.exceptions import AirflowException, AirflowSkipException


class MongoOperator(BaseOperator):
    """
    Custom MongoDB Operator for performing database operations.

    :param conn_id: The connection ID for MongoDB
    :param database: The database name
    :param collection: The collection name
    :param operation: The operation to perform (insert, update, delete, find, aggregate, count, upsert)
    :param documents: Documents to insert/update (for insert/update/upsert operations)
    :param query: Query filter (for find/delete operations)
    :param update: Update document (for update operations)
    :param upsert: Whether to upsert for update operations
    :param many: Whether to perform operation on multiple documents
    :param pipeline: Aggregation pipeline (for aggregate operations)
    :param projection: Fields to return (for find operations)
    :param sort: Sort specification (for find operations)
    :param limit: Limit number of documents (for find operations)
    :param skip: Skip number of documents (for find operations)
    :param filter_fields: List of field names to use for building query filters (for upsert operations)
    """

    template_fields = ('documents', 'query', 'update', 'pipeline', 'filter_fields')
    ui_color = '#4CAF50'

    def __init__(
        self,
        conn_id: str,
        collection: str,
        database: Optional[str] = None,
        operation: str = 'insert',
        documents: Optional[Union[Dict, List[Dict]]] = None,
        query: Optional[Dict] = None,
        update: Optional[Dict] = None,
        upsert: bool = False,
        many: bool = True,
        pipeline: Optional[List[Dict]] = None,
        projection: Optional[Dict] = None,
        sort: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        filter_fields: Optional[List[str]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.log.info(f"MongoOperator initialized with kwargs: {kwargs}")
        self.log.info(f"MongoOperator initialized with conn_id: {conn_id}, database: {database}, collection: {collection}, operation: {operation}, documents: {documents}, query: {query}, update: {update}, upsert: {upsert}, many: {many}, pipeline: {pipeline}, projection: {projection}, sort: {sort}, limit: {limit}, skip: {skip}")

        self.conn_id = conn_id
        self.database = database
        self.collection = collection
        self.operation = operation.lower()
        self.documents = documents
        self.query = query
        self.update = update
        self.upsert = upsert
        self.many = many
        self.pipeline = pipeline
        self.projection = projection
        self.sort = sort
        self.limit = limit
        self.skip = skip
        self.filter_fields = filter_fields

        # Validate operation
        valid_operations = ['insert', 'update', 'delete', 'find', 'aggregate', 'count', 'upsert']
        if self.operation not in valid_operations:
            raise AirflowException(f"Invalid operation '{self.operation}'. Must be one of: {valid_operations}")

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the MongoDB operation.

        :param context: Airflow context
        :return: Result of the operation
        """
        self.log.info(f"Executing MongoDB operation: {self.operation}")
        # Get MongoDB hook
        mongo_hook = MongoHook(mongo_conn_id=self.conn_id)
        collection = mongo_hook.get_collection(self.collection, self.database)

        self.log.info(f"Executing MongoDB operation: {self.operation}")

        # Handle empty documents or string documents (from Jinja templating) - only for operations that need documents
        if self.operation in ['insert', 'update', 'upsert']:
            if not self.documents or self.documents == "":
                self.log.warning("No documents provided for upsert operation, returning empty result")
                raise AirflowSkipException("No documents provided for upsert operation")

        # Parse string documents (from Jinja templating)
        if isinstance(self.documents, str):
            try:
                import ast
                self.documents = ast.literal_eval(self.documents)
                self.log.info(f"Parsed {len(self.documents)} documents from string")
            except Exception as e:
                self.log.error(f"Failed to parse documents string: {e}")
                raise AirflowException(f"Failed to parse documents string: {e}")

        try:
            if self.operation == 'insert':
                return self._execute_insert(collection)
            elif self.operation == 'update':
                return self._execute_update(collection)
            elif self.operation == 'delete':
                return self._execute_delete(collection)
            elif self.operation == 'find':
                return self._execute_find(collection)
            elif self.operation == 'aggregate':
                return self._execute_aggregate(collection)
            elif self.operation == 'count':
                return self._execute_count(collection)
            elif self.operation == 'upsert':
                return self._execute_upsert(collection)
            else:
                raise AirflowException(f"Operation '{self.operation}' not implemented")

        except Exception as e:
            self.log.error(f"MongoDB operation failed: {str(e)}")
            raise AirflowException(f"MongoDB operation failed: {str(e)}")

    def _extract_field_types(self, documents: Union[Dict, List[Dict]]) -> Dict[str, Dict[str, str]]:
        """
        Extract field types from MongoDB documents.

        :param documents: Single document or list of documents
        :return: Dictionary of field names and their types
        """
        fields = {}

        # Ensure we have a list to work with
        if isinstance(documents, dict):
            docs = [documents]
        else:
            docs = documents

        # Process each document to extract field types
        for doc in docs:
            for field_name, field_value in doc.items():
                if field_name not in fields:  # Only add if not already processed
                    field_type = self._get_python_type_name(field_value)
                    fields[field_name] = {"type": field_type}

        return fields

    def _get_python_type_name(self, value: Any) -> str:
        """
        Get the Python type name for a value.

        :param value: The value to get type for
        :return: String representation of the type
        """
        if value is None:
            return "Null"
        elif isinstance(value, bool):
            return "Boolean"
        elif isinstance(value, int):
            return "Number"
        elif isinstance(value, float):
            return "Number"
        elif isinstance(value, str):
            return "String"
        elif isinstance(value, list):
            return "Array"
        elif isinstance(value, dict):
            return "Object"
        elif hasattr(value, 'date'):  # datetime objects
            return "Date"
        else:
            return "Unknown"

    def _execute_insert(self, collection) -> Dict[str, Any]:
        """Execute insert operation."""
        if not self.documents:
            raise AirflowException("Documents must be provided for insert operation")

        # Extract field types from documents
        fields = self._extract_field_types(self.documents)

        if self.many and isinstance(self.documents, list):
            result = collection.insert_many(self.documents)
            inserted_count = len(result.inserted_ids)
            cleaned_ids = [str(id) for id in result.inserted_ids]
            self.log.info(f"Inserted {inserted_count} documents")
            return {
                'operation': 'insert_many',
                'inserted_count': inserted_count,
                'inserted_ids': cleaned_ids,
                'fields': fields
            }
        else:
            # Single document insert
            if isinstance(self.documents, list):
                documents = self.documents[0]
            else:
                documents = self.documents

            result = collection.insert_one(documents)
            cleaned_id = str(result.inserted_id)
            self.log.info(f"Inserted 1 document with ID: {cleaned_id}")
            return {
                'operation': 'insert_one',
                'inserted_count': 1,
                'inserted_id': cleaned_id,
                'fields': fields
            }

    def _execute_update(self, collection) -> Dict[str, Any]:
        """Execute update operation."""
        if not self.query:
            raise AirflowException("Query must be provided for update operation")
        if not self.update:
            raise AirflowException("Update document must be provided for update operation")

        if self.many:
            result = collection.update_many(
                self.query,
                self.update,
                upsert=self.upsert
            )
            cleaned_ids = [str(id) for id in result.upserted_id]
            self.log.info(f"Updated {result.modified_count} documents")
            return {
                'operation': 'update_many',
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': cleaned_ids
            }
        else:
            result = collection.update_one(
                self.query,
                self.update,
                upsert=self.upsert
            )
            cleaned_id = str(result.upserted_id)
            self.log.info(f"Updated {result.modified_count} document")
            return {
                'operation': 'update_one',
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': cleaned_id
            }

    def _execute_delete(self, collection) -> Dict[str, Any]:
        """Execute delete operation."""
        if not self.query:
            raise AirflowException("Query must be provided for delete operation")

        if self.many:
            result = collection.delete_many(self.query)
            cleaned_ids = [str(id) for id in result.deleted_ids]
            self.log.info(f"Deleted {result.deleted_count} documents")
            return {
                'operation': 'delete_many',
                'deleted_count': result.deleted_count,
                'deleted_ids': cleaned_ids
            }
        else:
            result = collection.delete_one(self.query)
            cleaned_id = str(result.deleted_id)
            self.log.info(f"Deleted {result.deleted_count} document")
            return {
                'operation': 'delete_one',
                'deleted_count': result.deleted_count,
                'deleted_id': cleaned_id
            }

    def _execute_find(self, collection) -> List[Dict]:
        """Execute find operation."""
        cursor = collection.find(self.query or {}, self.projection)

        if self.sort:
            cursor = cursor.sort(self.sort)
        if self.skip:
            cursor = cursor.skip(self.skip)
        if self.limit:
            cursor = cursor.limit(self.limit)

        results = list(cursor)

        # Convert ObjectId to string for XCom serialization
        cleaned_results = []
        for doc in results:
            cleaned_doc = {}
            for key, value in doc.items():
                if hasattr(value, '__class__') and value.__class__.__name__ == 'ObjectId':
                    cleaned_doc[key] = str(value)
                else:
                    cleaned_doc[key] = value
            cleaned_results.append(cleaned_doc)

        self.log.info(f"Found {len(cleaned_results)} documents")
        return cleaned_results

    def _execute_aggregate(self, collection) -> List[Dict]:
        """Execute aggregate operation."""
        if not self.pipeline:
            raise AirflowException("Pipeline must be provided for aggregate operation")

        results = list(collection.aggregate(self.pipeline))
        self.log.info(f"Aggregation returned {len(results)} documents")
        return results

    def _execute_count(self, collection) -> int:
        """Execute count operation."""
        count = collection.count_documents(self.query or {})
        self.log.info(f"Count: {count} documents")
        return count

    def _execute_upsert(self, collection) -> List[Dict[str, Any]]:
        """
        Execute upsert operations for each document using filter fields.

        :param collection: MongoDB collection
        :return: List of upsert results
        """
        if not self.filter_fields:
            raise AirflowException("filter_fields must be provided for upsert operation")

        # Ensure documents is a list
        documents = self.documents if isinstance(self.documents, list) else [self.documents]

        self.log.info(f"Starting MongoDB upsert operation for {len(documents)} documents")
        self.log.info(f"Using filter fields: {self.filter_fields}")

        results = []

        for i, document in enumerate(documents):
            try:
                # Build query filter from specified fields
                query_filter = {}
                for field in self.filter_fields:
                    if field in document:
                        query_filter[field] = document[field]
                    else:
                        self.log.warning(f"Filter field '{field}' not found in document {i}")

                if not query_filter:
                    self.log.error(f"No valid filter fields found in document {i}, skipping")
                    continue

                # Perform upsert operation
                result = collection.replace_one(
                    query_filter,
                    document,
                    upsert=True
                )

                results.append({
                    'document_index': i,
                    'matched_count': result.matched_count,
                    'modified_count': result.modified_count,
                    'upserted_id': str(result.upserted_id),
                    'filter_used': query_filter
                })

                if result.upserted_id:
                    self.log.info(f"Document {i} upserted with ID: {result.upserted_id}")
                elif result.modified_count > 0:
                    self.log.info(f"Document {i} updated (matched existing document)")
                else:
                    self.log.info(f"Document {i} found but not modified")

            except Exception as e:
                self.log.error(f"Error upserting document {i}: {e}")
                raise AirflowException(f"Error upserting document {i}: {e}")

        self.log.info(f"Upsert operation completed. Processed {len(results)} documents")
        return results


class MongoInsertOperator(MongoOperator):
    """
    Simplified MongoDB Insert Operator.

    :param conn_id: The connection ID for MongoDB
    :param database: The database name
    :param collection: The collection name
    :param documents: Documents to insert
    :param many: Whether to insert multiple documents
    """

    def __init__(
        self,
        conn_id: str,
        collection: str,
        documents: Union[Dict, List[Dict]],
        database: Optional[str] = None,
        many: bool = True,
        **kwargs
    ):
        super().__init__(
            conn_id=conn_id,
            database=database,
            collection=collection,
            operation='insert',
            documents=documents,
            many=many,
            **kwargs
        )


class MongoUpsertOperator(MongoOperator):
    """
    MongoDB Upsert Operator that performs upsert operations based on filter fields.

    This operator takes a list of filter fields and performs upsert operations
    for each document, using the specified fields to build the query filter.

    :param conn_id: The connection ID for MongoDB
    :param database: The database name
    :param collection: The collection name
    :param documents: Documents to upsert
    :param filter_fields: List of field names to use for building the query filter
    :param many: Whether to upsert multiple documents
    """

    def __init__(
        self,
        conn_id: str,
        collection: str,
        documents: Union[Dict, List[Dict]],
        filter_fields: List[str],
        database: Optional[str] = None,
        many: bool = True,
        **kwargs
    ):
        super().__init__(
            conn_id=conn_id,
            database=database,
            collection=collection,
            operation='upsert',
            documents=documents,
            filter_fields=filter_fields,
            many=many,
            **kwargs
        )


class MongoFindOperator(MongoOperator):
    """
    Simplified MongoDB Find Operator.

    :param conn_id: The connection ID for MongoDB
    :param database: The database name
    :param collection: The collection name
    :param query: Query filter
    :param projection: Fields to return
    :param sort: Sort specification
    :param limit: Limit number of documents
    :param skip: Skip number of documents
    """

    def __init__(
        self,
        conn_id: str,
        collection: str,
        database: Optional[str] = None,
        query: Optional[Dict] = None,
        projection: Optional[Dict] = None,
        sort: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        **kwargs
    ):
        super().__init__(
            conn_id=conn_id,
            database=database,
            collection=collection,
            operation='find',
            query=query,
            projection=projection,
            sort=sort,
            limit=limit,
            skip=skip,
            **kwargs
        )
