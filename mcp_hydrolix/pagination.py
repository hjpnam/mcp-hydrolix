"""Pagination utilities for MCP Hydrolix tools."""

import base64
import hashlib
import json
from dataclasses import dataclass


@dataclass
class BaseCursorData:
    """Base class for cursor data.

    Attributes:
        offset: Current offset in the result set
    """

    offset: int

    def encode(self) -> str:
        """Encode cursor to base64 string.

        Returns:
            Base64-encoded JSON string (URL-safe)

        Example:
            >>> cursor = TableListCursor(offset=50, database="test")
            >>> cursor.encode()
            'eyJkYXRhYmFzZSI6InRlc3QiLCJvZmZzZXQiOjUwfQ=='
        """
        # Convert dataclass to dict, excluding None values
        cursor_dict = {k: v for k, v in self.__dict__.items() if v is not None}
        json_str = json.dumps(cursor_dict, sort_keys=True)
        return base64.urlsafe_b64encode(json_str.encode()).decode()


@dataclass
class TableListCursor(BaseCursorData):
    """Cursor for list_tables pagination.

    Attributes:
        offset: Current offset in the result set
        database: Database name for validation
    """

    database: str = ""

    @classmethod
    def decode(cls, cursor: str) -> "TableListCursor":
        """Decode cursor string to TableListCursor.

        Args:
            cursor: Opaque cursor string from previous list_tables response

        Returns:
            TableListCursor instance

        Raises:
            ValueError: If cursor is invalid or malformed

        Example:
            >>> cursor_str = 'eyJkYXRhYmFzZSI6InRlc3QiLCJvZmZzZXQiOjUwfQ=='
            >>> cursor_obj = TableListCursor.decode(cursor_str)
            >>> cursor_obj.database
            'test'
        """
        try:
            json_str = base64.urlsafe_b64decode(cursor.encode()).decode()
            data = json.loads(json_str)
            return cls(**data)
        except Exception as e:
            raise ValueError(f"Invalid TableListCursor format: {str(e)}")

    def validate_params(self, database: str) -> None:
        """Validate cursor matches request parameters.

        Args:
            database: Expected database name

        Raises:
            ValueError: If parameters don't match

        Example:
            >>> cursor = TableListCursor(offset=50, database="test")
            >>> cursor.validate_params("test")  # OK
            >>> cursor.validate_params("prod")  # Raises ValueError
        """
        if self.database != database:
            raise ValueError(f"Cursor database mismatch: {self.database} != {database}")


@dataclass
class QueryResultCursor(BaseCursorData):
    """Cursor for run_select_query pagination.

    Attributes:
        offset: Current offset in the result set
        query_hash: SHA256 hash of query for validation
    """

    query_hash: str = ""

    @classmethod
    def decode(cls, cursor: str) -> "QueryResultCursor":
        """Decode cursor string to QueryResultCursor.

        Args:
            cursor: Opaque cursor string from previous run_select_query response

        Returns:
            QueryResultCursor instance

        Raises:
            ValueError: If cursor is invalid or malformed

        Example:
            >>> cursor_str = 'eyJvZmZzZXQiOjEwMCwicXVlcnlfaGFzaCI6ImFiYzEyMyJ9'
            >>> cursor_obj = QueryResultCursor.decode(cursor_str)
            >>> cursor_obj.offset
            100
        """
        try:
            json_str = base64.urlsafe_b64decode(cursor.encode()).decode()
            data = json.loads(json_str)
            return cls(**data)
        except Exception as e:
            raise ValueError(f"Invalid QueryResultCursor format: {str(e)}")

    def validate_query(self, query: str) -> None:
        """Validate cursor matches query.

        Args:
            query: SQL query string to validate against

        Raises:
            ValueError: If query has changed

        Example:
            >>> query = "SELECT * FROM table"
            >>> cursor = QueryResultCursor(offset=100, query_hash=hash_query(query))
            >>> cursor.validate_query(query)  # OK
            >>> cursor.validate_query("SELECT * FROM other")  # Raises ValueError
        """
        expected_hash = hash_query(query)
        if self.query_hash != expected_hash:
            raise ValueError("Query has changed since cursor was generated")


def hash_query(query: str) -> str:
    """Generate hash of query for cursor validation.

    Strips leading/trailing whitespace before hashing to ensure
    queries that differ only in whitespace produce the same hash.

    NOTE: This hashes the original query WITHOUT pagination LIMIT/OFFSET.
    The OFFSET is stored separately in the cursor and is intentionally
    NOT part of the hash. This allows the same query to be paginated
    across multiple requests while preventing query-switching attacks.

    Args:
        query: SQL query string (without pagination LIMIT/OFFSET)

    Returns:
        SHA256 hash as hex string

    Example:
        >>> hash_query("SELECT * FROM table")
        'a3b2c1...'
    """
    return hashlib.sha256(query.strip().encode()).hexdigest()
