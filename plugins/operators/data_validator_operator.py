"""
Data Validation Operator for Airflow

This operator provides functionality to validate data in MongoDB collections,
checking data quality, completeness, and business rules.
"""

from typing import Any, Dict, List, Optional
from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.exceptions import AirflowException


class DataValidatorOperator(BaseOperator):
    """
    Data Validation Operator for checking data quality and completeness.

    :param conn_id: MongoDB connection ID
    :param collection: Collection name to validate
    :param target_date: Target date to validate (YYYY-MM-DD format)
    :param date_field: Date field to filter by (default: 'created_day')
    :param min_documents: Minimum number of documents expected
    :param validation_rules: Additional validation rules to apply
    """

    template_fields = ('target_date', 'date_field')
    ui_color = '#9C27B0'

    def __init__(
            self,
            conn_id: str,
            collection: str,
            target_date: str,
            date_field: str = 'created_day',
            min_documents: int = 1,
            validation_rules: Optional[List[Dict[str, Any]]] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.collection = collection
        self.target_date = target_date
        self.date_field = date_field
        self.min_documents = min_documents
        self.validation_rules = validation_rules or []

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        mongo_hook = MongoHook(mongo_conn_id=self.conn_id)
        coll = mongo_hook.get_collection(self.collection)
        count = coll.count_documents({self.date_field: self.target_date})

        self.log.info(f"Validation check for {self.date_field}={self.target_date}: found {count} documents")

        if count < self.min_documents:
            raise AirflowException(
                f"Insufficient documents found for {self.date_field}={self.target_date}. "
                f"Expected at least {self.min_documents}, found {count}"
            )


        validation_results = []
        for rule in self.validation_rules:
            try:
                result = self._apply_validation_rule(coll, rule)
                validation_results.append(result)
            except Exception as e:
                self.log.error(f"Validation rule failed: {rule.get('name', 'unknown')} - {e}")
                if rule.get('strict', False):
                    raise AirflowException(f"Strict validation rule failed: {rule.get('name')}")

        return {
            "validated_day": self.target_date,
            "validated_count": count,
            "validation_results": validation_results,
            "status": "success"
        }

    def _apply_validation_rule(self, collection, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a specific validation rule."""
        rule_name = rule.get('name', 'unknown')
        query = rule.get('query', {})
        expected_count = rule.get('expected_count')
        min_count = rule.get('min_count')
        max_count = rule.get('max_count')

        count = collection.count_documents(query)
        
        result = {
            'rule_name': rule_name,
            'query': query,
            'actual_count': count,
            'passed': True
        }

        if expected_count is not None and count != expected_count:
            result['passed'] = False
            result['error'] = f"Expected {expected_count}, got {count}"
        elif min_count is not None and count < min_count:
            result['passed'] = False
            result['error'] = f"Expected at least {min_count}, got {count}"
        elif max_count is not None and count > max_count:
            result['passed'] = False
            result['error'] = f"Expected at most {max_count}, got {count}"

        return result


class ReviewDataValidatorOperator(DataValidatorOperator):
    """
    Specialized validator for review data with common validation rules.
    
    :param conn_id: MongoDB connection ID
    :param collection: Collection name to validate
    :param target_date: Target date to validate (YYYY-MM-DD format)
    :param date_field: Date field to filter by (default: 'created_day')
    :param min_documents: Minimum number of documents expected
    :param min_reviews_per_product: Minimum number of reviews per product
    :param check_sentiment: Whether to check sentiment field completeness
    """

    def __init__(
            self,
            conn_id: str,
            collection: str,
            target_date: str,
            date_field: str = 'created_day',
            min_documents: int = 1,
            min_reviews_per_product: int = 1,
            check_sentiment: bool = False,
            **kwargs
    ):
        validation_rules = [
            {
                'name': 'date_filter',
                'query': {date_field: target_date},
                'min_count': min_documents
            },
            {
                'name': 'required_fields',
                'query': {
                    date_field: target_date,
                    'review_id': {'$exists': True, '$ne': ''},
                    'product_id': {'$exists': True, '$ne': ''},
                    'review_content': {'$exists': True, '$ne': ''}
                },
                'min_count': min_documents
            }
        ]

        if check_sentiment:
            validation_rules.append({
                'name': 'sentiment_completeness',
                'query': {
                    date_field: target_date,
                    'sentiment': {'$exists': True, '$ne': ''}
                },
                'min_count': min_documents
            })

        super().__init__(
            conn_id=conn_id,
            collection=collection,
            target_date=target_date,
            date_field=date_field,
            min_documents=min_documents,
            validation_rules=validation_rules,
            **kwargs
        )
