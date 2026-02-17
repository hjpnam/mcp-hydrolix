"""Tests for SQL parser-based LIMIT/OFFSET detection in pagination."""

import pytest

from mcp_hydrolix.mcp_server import _add_pagination_to_query


class TestPaginationQueryParser:
    """Tests for robust LIMIT/OFFSET detection using sqlparse."""

    def test_simple_query_without_limit(self):
        """Test query without LIMIT gets it appended directly."""
        query = "SELECT * FROM users ORDER BY id"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        assert result == "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 0"
        # Should NOT be wrapped in subquery (most efficient)
        assert "paginated_subquery" not in result

    def test_query_with_limit_gets_wrapped(self):
        """Test query with LIMIT gets wrapped in subquery."""
        query = "SELECT * FROM users LIMIT 100"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        assert "paginated_subquery" in result
        assert (
            result
            == "SELECT * FROM (SELECT * FROM users LIMIT 100) AS paginated_subquery LIMIT 10 OFFSET 0"
        )

    def test_query_with_offset_gets_wrapped(self):
        """Test query with OFFSET gets wrapped in subquery."""
        query = "SELECT * FROM users OFFSET 50"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        assert "paginated_subquery" in result

    def test_table_name_with_limit_no_false_positive(self):
        """Test table name containing 'limit' doesn't trigger wrapping."""
        query = "SELECT * FROM my_limit_table WHERE id > 10"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should NOT be wrapped - 'limit' is part of table name, not keyword
        assert "paginated_subquery" not in result
        assert result == "SELECT * FROM my_limit_table WHERE id > 10 LIMIT 10 OFFSET 0"

    def test_column_name_with_offset_no_false_positive(self):
        """Test column name containing 'offset' doesn't trigger wrapping."""
        query = "SELECT offset_value, name FROM settings"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should NOT be wrapped - 'offset' is a column name, not keyword
        assert "paginated_subquery" not in result
        assert result == "SELECT offset_value, name FROM settings LIMIT 10 OFFSET 0"

    def test_string_literal_with_limit_no_false_positive(self):
        """Test string literal containing 'LIMIT' doesn't trigger wrapping."""
        query = "SELECT 'Check the LIMIT' as message FROM logs"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should NOT be wrapped - 'LIMIT' is in a string literal, not a keyword
        assert "paginated_subquery" not in result
        assert result == "SELECT 'Check the LIMIT' as message FROM logs LIMIT 10 OFFSET 0"

    def test_multiple_tables_with_limit_in_name(self):
        """Test complex query with 'limit' in table/column names."""
        query = """
            SELECT t1.offset_value, t2.limit_config
            FROM offset_logs t1
            JOIN limit_settings t2 ON t1.id = t2.id
            WHERE t1.status = 'active'
        """
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should NOT be wrapped - all instances are identifiers, not keywords
        assert "paginated_subquery" not in result
        assert "LIMIT 10 OFFSET 0" in result

    def test_comment_with_limit_no_false_positive(self):
        """Test SQL comment containing 'LIMIT' doesn't trigger wrapping."""
        query = "SELECT * FROM table -- TODO: add LIMIT later"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should NOT be wrapped - 'LIMIT' is in a comment
        assert "paginated_subquery" not in result

    def test_case_insensitive_limit_detection(self):
        """Test LIMIT keyword detection is case-insensitive."""
        queries = [
            "SELECT * FROM users LIMIT 100",
            "SELECT * FROM users limit 100",
            "SELECT * FROM users LiMiT 100",
        ]

        for query in queries:
            result = _add_pagination_to_query(query, limit=10, offset=0)
            assert "paginated_subquery" in result

    def test_complex_query_with_actual_limit(self):
        """Test complex query with real LIMIT keyword gets wrapped."""
        query = """
            SELECT id, name, created_at
            FROM users
            WHERE status = 'active'
            ORDER BY created_at DESC
            LIMIT 1000
        """
        result = _add_pagination_to_query(query, limit=10, offset=20)

        assert "paginated_subquery" in result
        assert result.endswith("LIMIT 10 OFFSET 20")

    def test_subquery_with_limit_in_where_clause(self):
        """Test query with LIMIT in subquery within WHERE clause."""
        query = """
            SELECT * FROM orders
            WHERE user_id IN (SELECT id FROM users LIMIT 100)
        """
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should be wrapped because inner query has LIMIT
        assert "paginated_subquery" in result

    def test_empty_query(self):
        """Test edge case with empty query."""
        query = ""
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should handle gracefully
        assert "LIMIT 10 OFFSET 0" in result

    def test_query_with_semicolon(self):
        """Test query ending with semicolon is handled correctly."""
        query = "SELECT * FROM users;"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Semicolon should be removed before adding LIMIT
        assert result == "SELECT * FROM users LIMIT 10 OFFSET 0"

    def test_both_limit_and_offset_in_original_query(self):
        """Test query with both LIMIT and OFFSET gets wrapped."""
        query = "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 50"
        result = _add_pagination_to_query(query, limit=10, offset=20)

        assert "paginated_subquery" in result
        expected = "SELECT * FROM (SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 50) AS paginated_subquery LIMIT 10 OFFSET 20"
        assert result == expected

    def test_backtick_identifiers_no_false_positive(self):
        """Test backtick-quoted identifiers containing 'limit' don't trigger wrapping."""
        query = "SELECT `limit_column` FROM `offset_table`"
        result = _add_pagination_to_query(query, limit=10, offset=0)

        # Should NOT be wrapped - these are quoted identifiers
        assert "paginated_subquery" not in result
        assert result == "SELECT `limit_column` FROM `offset_table` LIMIT 10 OFFSET 0"

    @pytest.mark.parametrize(
        "limit,offset",
        [
            (10, 0),
            (100, 50),
            (1, 999),
            (10000, 0),
        ],
    )
    def test_various_limit_offset_values(self, limit, offset):
        """Test various limit and offset values are correctly applied."""
        query = "SELECT * FROM table"
        result = _add_pagination_to_query(query, limit=limit, offset=offset)

        assert f"LIMIT {limit} OFFSET {offset}" in result
