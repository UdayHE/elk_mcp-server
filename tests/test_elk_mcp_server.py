import pytest

import elk_mcp_server as srv


class TestCheckLevel:
    def test_none_passthrough(self):
        assert srv._check_level(None) is None

    def test_lowercases(self):
        assert srv._check_level("ERROR") == "error"

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="unknown level"):
            srv._check_level("critical")


class TestLevelFilter:
    def test_none_passthrough(self):
        assert srv._level_filter(None) is None

    def test_builds_case_insensitive_term(self):
        assert srv._level_filter("error") == [
            {"term": {"level": {"value": "error", "case_insensitive": True}}}
        ]


class TestTs:
    def test_normalizes_z_suffix(self):
        assert srv._ts({"@timestamp": "2026-01-27T10:00:00.123456Z"}) == "2026-01-27T10:00:00"

    def test_normalizes_offset(self):
        assert srv._ts({"@timestamp": "2026-01-27T15:30:00+05:30"}) == "2026-01-27T10:00:00"

    def test_truncates_excess_fractional_digits(self):
        # 9-digit fractional seconds (nanoseconds) would break fromisoformat otherwise
        assert srv._ts({"@timestamp": "2026-01-27T10:00:00.123456789Z"}) == "2026-01-27T10:00:00"

    def test_missing_timestamp_returns_placeholder(self):
        assert srv._ts({}) == "?"

    def test_unparseable_falls_back_to_prefix(self):
        assert srv._ts({"@timestamp": "not-a-timestamp"}) == "not-a-timestamp"[:19]


class TestFlat:
    def test_collapses_newlines_and_tabs(self):
        assert srv._flat("line1\nline2\tcol\r") == "line1 ↵ line2  col"

    def test_strips_whitespace(self):
        assert srv._flat("  hello  ") == "hello"


class TestPattern:
    def test_normalizes_uuid(self):
        msg = "failed for user 123e4567-e89b-12d3-a456-426614174000"
        assert "<uuid>" in srv._pattern(msg)
        assert "123e4567" not in srv._pattern(msg)

    def test_normalizes_ip(self):
        assert "<ip>" in srv._pattern("connection from 10.0.0.1 refused")

    def test_normalizes_numbers(self):
        assert srv._pattern("retry attempt 42 of 5") == "retry attempt N of N"

    def test_truncates_to_200_chars(self):
        assert len(srv._pattern("x" * 500)) <= 200

    def test_same_pattern_for_similar_messages(self):
        a = srv._pattern("timeout after 30s for pod pod-123")
        b = srv._pattern("timeout after 45s for pod pod-456")
        assert a == b


class TestTotal:
    def test_exact_relation(self):
        results = {"hits": {"total": {"value": 42, "relation": "eq"}}}
        assert srv._total(results) == "42"

    def test_gte_relation(self):
        results = {"hits": {"total": {"value": 10000, "relation": "gte"}}}
        assert srv._total(results) == "10000+"

    def test_missing_total_defaults_zero(self):
        assert srv._total({}) == "0"


class TestHitLine:
    def test_basic_fields(self):
        hit = {
            "_id": "abc",
            "_index": "logs-1",
            "_source": {
                "@timestamp": "2026-01-27T10:00:00Z",
                "level": "error",
                "namespace": "tenant-abc",
                "podname": "pod-1",
                "message": "boom",
            },
        }
        line = srv._hit_line(hit)
        assert line == "2026-01-27T10:00:00\terror\ttenant-abc\tpod-1\tboom"

    def test_truncates_long_message_with_marker(self):
        hit = {
            "_id": "abc123",
            "_index": "logs-1",
            "_source": {"message": "x" * 1000},
        }
        line = srv._hit_line(hit)
        assert "elk_get_log id=abc123 index=logs-1" in line
        assert len(line) < 1000


class TestRenderHits:
    def _hit(self, i):
        return {"_id": str(i), "_index": "idx",
                "_source": {"message": f"msg-{i}", "@timestamp": "2026-01-27T10:00:00Z"}}

    def test_renders_all_under_budget(self):
        hits = [self._hit(i) for i in range(5)]
        lines, n = srv._render_hits(hits)
        assert n == 5
        assert len(lines) == 6  # header + 5 rows

    def test_truncates_over_budget(self):
        hits = [self._hit(i) for i in range(100)]
        lines, n = srv._render_hits(hits, budget=200)
        assert n < 100
        assert lines[-1].startswith("…response truncated")

    def test_always_renders_at_least_one_row(self):
        # even a single row larger than budget should still be emitted
        huge_hit = {"_id": "1", "_index": "idx", "_source": {"message": "x" * 1000}}
        lines, n = srv._render_hits([huge_hit], budget=10)
        assert n == 1


class TestHistogramInterval:
    @pytest.mark.parametrize("span_hours, expected", [
        (0.25, "1m"),
        (1, "5m"),
        (5, "15m"),
        (12, "1h"),
        (48, "4h"),
        (200, "12h"),
        (1000, "1d"),
    ])
    def test_boundaries(self, span_hours, expected):
        assert srv._histogram_interval(span_hours) == expected


class TestClientRegistry:
    def test_unknown_region_raises(self):
        with pytest.raises(ValueError, match="unknown region"):
            srv._client("mars-1")

    def test_caches_client_instance(self):
        c1 = srv._client("ap-south-1")
        c2 = srv._client("ap-south-1")
        assert c1 is c2
