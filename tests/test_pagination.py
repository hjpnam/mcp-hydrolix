"""Unit tests for pagination utilities."""

import base64

import pytest

from mcp_hydrolix.pagination import (
    QueryResultCursor,
    TableListCursor,
    hash_query,
)


class TestCursorEncoding:
    """Tests for cursor encoding and decoding."""

    @pytest.mark.parametrize(
        "cursor_obj",
        [
            # Table list cursor
            TableListCursor(offset=50, database="test"),
            # Query result cursor
            QueryResultCursor(offset=100, query_hash="abc123def456"),
            # Minimal cursor
            TableListCursor(offset=0, database="mydb"),
        ],
        ids=["table_list", "query_result", "minimal"],
    )
    def test_cursor_encoding_roundtrip(self, cursor_obj):
        """Test cursor can be encoded and decoded."""
        cursor = cursor_obj.encode()
        # Use type-specific decode
        if isinstance(cursor_obj, TableListCursor):
            decoded = TableListCursor.decode(cursor)
        else:
            decoded = QueryResultCursor.decode(cursor)
        assert decoded.offset == cursor_obj.offset
        assert isinstance(decoded, type(cursor_obj))

    def test_encoded_cursor_is_url_safe(self):
        """Test that encoded cursor uses URL-safe base64."""
        cursor_obj = TableListCursor(offset=50, database="test")
        cursor = cursor_obj.encode()
        # URL-safe base64 should not contain + or /
        assert "+" not in cursor.rstrip("=")
        assert "/" not in cursor.rstrip("=")

    @pytest.mark.parametrize(
        "invalid_cursor,description",
        [
            ("invalid!!!cursor", "invalid characters"),
            ("not-valid-base64!@#$", "malformed base64"),
            (base64.urlsafe_b64encode(b"not json data").decode(), "invalid json"),
            ("", "empty string"),
        ],
        ids=["invalid_chars", "malformed_base64", "invalid_json", "empty"],
    )
    def test_invalid_cursor_raises_error(self, invalid_cursor: str, description: str):
        """Test various invalid cursors raise ValueError."""
        with pytest.raises(ValueError):
            TableListCursor.decode(invalid_cursor)
        with pytest.raises(ValueError):
            QueryResultCursor.decode(invalid_cursor)


class TestQueryHashing:
    """Tests for query hashing."""

    def test_query_hash_consistency(self):
        """Test query hash is consistent."""
        query = "SELECT * FROM table"
        hash1 = hash_query(query)
        hash2 = hash_query(query)
        assert hash1 == hash2

    def test_query_hash_is_sha256(self):
        """Test query hash produces SHA256 hex string."""
        query = "SELECT * FROM table"
        result = hash_query(query)
        # SHA256 hex string is 64 characters
        assert len(result) == 64
        # Should be valid hex
        assert all(c in "0123456789abcdef" for c in result)

    @pytest.mark.parametrize(
        "query1,query2",
        [
            ("SELECT * FROM table", "  SELECT * FROM table"),
            ("SELECT * FROM table", "SELECT * FROM table  "),
            ("SELECT * FROM table", "  SELECT * FROM table  \n"),
            ("SELECT *\nFROM table\nWHERE id = 1", "  SELECT *\nFROM table\nWHERE id = 1  "),
        ],
        ids=["leading_ws", "trailing_ws", "both_ws", "multiline"],
    )
    def test_query_hash_ignores_whitespace(self, query1: str, query2: str):
        """Test query hash ignores leading/trailing whitespace."""
        assert hash_query(query1) == hash_query(query2)

    @pytest.mark.parametrize(
        "query1,query2",
        [
            ("SELECT * FROM table1", "SELECT * FROM table2"),
            ("SELECT * FROM table", "SELECT  *  FROM  table"),
        ],
        ids=["different_content", "internal_whitespace"],
    )
    def test_query_hash_sensitive_to_differences(self, query1: str, query2: str):
        """Test query hash changes with content or internal whitespace."""
        assert hash_query(query1) != hash_query(query2)


class TestCursorParameterValidation:
    """Tests for cursor parameter validation."""

    @pytest.mark.parametrize(
        "database",
        ["test", "prod", "mydb"],
        ids=["test", "prod", "mydb"],
    )
    def test_validate_table_list_cursor_success(self, database: str):
        """Test validation passes with matching database."""
        cursor = TableListCursor(offset=50, database=database)
        cursor.validate_params(database)  # Should not raise

    @pytest.mark.parametrize(
        "cursor_database,expected_database",
        [
            ("test", "prod"),
            ("mydb", "test"),
            ("database1", "database2"),
        ],
        ids=["test_vs_prod", "mydb_vs_test", "db1_vs_db2"],
    )
    def test_validate_table_list_cursor_failure(self, cursor_database: str, expected_database: str):
        """Test validation fails with mismatched database."""
        cursor = TableListCursor(offset=50, database=cursor_database)
        with pytest.raises(ValueError, match="Cursor database mismatch"):
            cursor.validate_params(expected_database)

    def test_validate_query_result_cursor_success(self):
        """Test query validation passes with same query."""
        query = "SELECT * FROM table"
        cursor = QueryResultCursor(offset=100, query_hash=hash_query(query))
        cursor.validate_query(query)  # Should not raise

    @pytest.mark.parametrize(
        "original_query,different_query",
        [
            ("SELECT * FROM table1", "SELECT * FROM table2"),
            ("SELECT * FROM users", "SELECT * FROM admin_users"),
            ("SELECT id FROM test", "SELECT name FROM test"),
        ],
        ids=["different_table", "different_table2", "different_column"],
    )
    def test_validate_query_result_cursor_failure(self, original_query: str, different_query: str):
        """Test validation fails when query changes."""
        cursor = QueryResultCursor(offset=100, query_hash=hash_query(original_query))
        with pytest.raises(ValueError, match="Query has changed"):
            cursor.validate_query(different_query)


class TestCursorDataIntegration:
    """Integration tests combining multiple cursor operations."""

    def test_full_cursor_workflow(self):
        """Test complete cursor workflow: create, encode, decode, validate."""
        # Create cursor
        cursor = TableListCursor(offset=50, database="prod")

        # Encode
        cursor_string = cursor.encode()

        # Decode
        decoded = TableListCursor.decode(cursor_string)

        # Validate structure
        assert isinstance(decoded, TableListCursor)
        assert decoded.offset == 50
        assert decoded.database == "prod"

        # Validate params
        decoded.validate_params("prod")  # Should not raise

    def test_query_result_cursor_workflow(self):
        """Test cursor workflow for query results with hash."""
        query = "SELECT * FROM large_table ORDER BY timestamp"
        query_hash_value = hash_query(query)

        # Create cursor
        cursor = QueryResultCursor(offset=10000, query_hash=query_hash_value)

        # Encode and decode
        cursor_string = cursor.encode()
        decoded = QueryResultCursor.decode(cursor_string)

        # Verify structure
        assert isinstance(decoded, QueryResultCursor)
        assert decoded.offset == 10000
        assert decoded.query_hash == query_hash_value
        assert decoded.query_hash == hash_query(query)

        # Verify hash changes if query changes
        different_query = "SELECT * FROM large_table ORDER BY id"
        assert decoded.query_hash != hash_query(different_query)

    def test_cursor_prevents_parameter_change_attack(self):
        """Test cursor validation prevents parameter tampering."""
        # Create cursor for database "test"
        cursor = TableListCursor(offset=50, database="test")
        cursor_string = cursor.encode()

        # Attacker tries to use cursor with different database
        decoded = TableListCursor.decode(cursor_string)
        assert isinstance(decoded, TableListCursor)
        with pytest.raises(ValueError, match="Cursor database mismatch"):
            decoded.validate_params("prod")

    def test_cursor_prevents_query_change_attack(self):
        """Test cursor validation prevents query tampering."""
        # Create cursor for specific query
        original_query = "SELECT * FROM users"
        cursor = QueryResultCursor(offset=100, query_hash=hash_query(original_query))
        cursor_string = cursor.encode()

        # Attacker tries to use cursor with different query
        decoded = QueryResultCursor.decode(cursor_string)
        assert isinstance(decoded, QueryResultCursor)
        different_query = "SELECT * FROM admin_users"

        # Validation should fail
        assert decoded.query_hash != hash_query(different_query)
        with pytest.raises(ValueError, match="Query has changed"):
            decoded.validate_query(different_query)
