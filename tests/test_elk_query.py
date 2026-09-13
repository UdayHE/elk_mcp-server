from datetime import datetime, timezone

import pytest

from elk_query import ELKQueryClient


class TestParseDatetime:
    @pytest.mark.parametrize("raw, expected", [
        ("2026-01-27 10:00:00", datetime(2026, 1, 27, 10, 0, 0, tzinfo=timezone.utc)),
        ("2026-01-27T10:00:00", datetime(2026, 1, 27, 10, 0, 0, tzinfo=timezone.utc)),
        ("2026-01-27 10:00", datetime(2026, 1, 27, 10, 0, 0, tzinfo=timezone.utc)),
        ("2026-01-27T10:00", datetime(2026, 1, 27, 10, 0, 0, tzinfo=timezone.utc)),
        ("2026-01-27", datetime(2026, 1, 27, 0, 0, 0, tzinfo=timezone.utc)),
    ])
    def test_supported_formats(self, client, raw, expected):
        assert client._parse_datetime(raw) == expected

    def test_invalid_format_raises(self, client):
        with pytest.raises(ValueError):
            client._parse_datetime("not-a-date")

    def test_result_is_utc(self, client):
        dt = client._parse_datetime("2026-01-27 10:00:00")
        assert dt.tzinfo == timezone.utc


class TestClientInit:
    def test_invalid_region_raises(self):
        with pytest.raises(ValueError, match="Invalid region"):
            ELKQueryClient(region="eu-west-1")

    def test_missing_credentials_raise(self, monkeypatch):
        monkeypatch.delenv("ES_URL_MUMBAI", raising=False)
        monkeypatch.delenv("ES_API_KEY_MUMBAI", raising=False)
        with pytest.raises(ValueError, match="Missing environment variables"):
            ELKQueryClient(region="ap-south-1")

    def test_missing_kibana_vars_warn_not_raise(self, monkeypatch, capsys):
        monkeypatch.delenv("KIBANA_URL_MUMBAI", raising=False)
        monkeypatch.delenv("KIBANA_INDEX_ID_MUMBAI", raising=False)
        c = ELKQueryClient(region="ap-south-1")
        assert c.kibana_url is None
        assert c.kibana_index_id is None
        captured = capsys.readouterr()
        assert "KIBANA_URL_MUMBAI" in captured.out
        assert "KIBANA_INDEX_ID_MUMBAI" in captured.out


class TestGenerateKibanaUrl:
    def test_returns_none_without_kibana_url(self, client, monkeypatch):
        monkeypatch.setattr(client, "kibana_url", None)
        assert client.generate_kibana_url() is None

    def test_returns_none_without_index_id(self, client, monkeypatch):
        monkeypatch.setattr(client, "kibana_index_id", None)
        assert client.generate_kibana_url() is None

    def test_contains_kibana_url_and_index(self, client):
        url = client.generate_kibana_url(namespace="tenant-abc", hours=2)
        assert url.startswith(client.kibana_url)
        assert client.kibana_index_id in url
        assert "/app/discover" in url


class TestQueryLogsRequestBuilding:
    def test_uses_all_indices_endpoint(self, client, monkeypatch):
        captured = {}

        def fake_post(url, headers, json, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json

            class FakeResponse:
                def raise_for_status(self):
                    pass

                def json(self):
                    return {"hits": {"hits": [], "total": {"value": 0}}}

            return FakeResponse()

        monkeypatch.setattr("elk_query.requests.post", fake_post)
        client.query_logs(namespace="tenant-abc", hours=1)

        assert captured["url"] == f"{client.es_url}/*/_search"
        assert captured["headers"]["Authorization"] == f"ApiKey {client.api_key}"
        must = captured["json"]["query"]["bool"]["must"]
        assert {"match": {"namespace": "tenant-abc"}} in must
