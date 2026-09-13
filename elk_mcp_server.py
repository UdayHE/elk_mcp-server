#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "mcp>=1.2.0,<2.0.0",
#   "requests>=2.31.0",
#   "python-dotenv>=1.0.0",
# ]
# ///
"""
ELK MCP Server — token-efficient log access for AI debugging agents.

Wraps Elastic Cloud clusters (ap-south-1 Mumbai, us-east-1) behind six
read-only tools designed to keep agent context small:

    elk_overview        aggregations only — when/where are the logs (call FIRST)
    elk_error_patterns  dedup repeated errors into patterns with counts
    elk_search          compact TSV log lines, field-filtered, cursor pagination
    elk_get_log         full single document (complete stack trace) by id
    elk_context         logs immediately before/after a moment for a pod
    elk_dump            bulk export to a local JSONL file + summary

Credentials come from the project .env (see .env.example).
Run directly:  uv run --script elk_mcp_server.py
"""

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(REPO_ROOT / ".env")
sys.path.insert(0, str(REPO_ROOT))

from elk_query import ELKQueryClient  # noqa: E402

from mcp.server.fastmcp import FastMCP  # noqa: E402
from mcp.types import ToolAnnotations  # noqa: E402

mcp = FastMCP("elk")

REGIONS = list(ELKQueryClient.REGION_ENV_MAPPING.keys())
VALID_LEVELS = {"info", "warn", "warning", "error", "debug", "trace", "fatal"}

BASE_FIELDS = ["@timestamp", "level", "namespace", "podname", "message"]
MSG_TRUNCATE = 500
MAX_RESPONSE_CHARS = 20_000

_clients: Dict[str, ELKQueryClient] = {}


def _client(region: str) -> ELKQueryClient:
    if region not in REGIONS:
        raise ValueError(f"unknown region '{region}' — valid: {', '.join(REGIONS)}")
    if region not in _clients:
        _clients[region] = ELKQueryClient(region=region)
    return _clients[region]


def _check_level(level: Optional[str]) -> Optional[str]:
    if level is None:
        return None
    lv = level.lower()
    if lv not in VALID_LEVELS:
        raise ValueError(f"unknown level '{level}' — valid: {', '.join(sorted(VALID_LEVELS))}")
    return lv


def _level_filter(level: Optional[str]) -> Optional[List[dict]]:
    """Case-insensitive level filter — indices mix 'error' (Go/Python) and
    'ERROR' (Java), and the keyword field is case-sensitive to exact match."""
    if level is None:
        return None
    return [{"term": {"level": {"value": level, "case_insensitive": True}}}]


_FRAC_RE = re.compile(r"(\.\d{6})\d+")


def _ts(source: dict) -> str:
    """@timestamp normalized to UTC (raw values carry mixed tz offsets)."""
    raw = str(source.get("@timestamp", ""))
    try:
        dt = datetime.fromisoformat(_FRAC_RE.sub(r"\1", raw.replace("Z", "+00:00")))
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return raw[:19] or "?"


def _flat(msg: str) -> str:
    """Collapse a message onto one line for TSV output."""
    return msg.replace("\t", "  ").replace("\r", "").replace("\n", " ↵ ").strip()


_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_HEX_RE = re.compile(r"\b[0-9a-fA-F]{8,}\b")
_IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
_NUM_RE = re.compile(r"\d+")


def _pattern(msg: str) -> str:
    """Normalize IDs/numbers so repeated messages collapse into one pattern."""
    p = msg[:200]
    p = _UUID_RE.sub("<uuid>", p)
    p = _IP_RE.sub("<ip>", p)
    p = _HEX_RE.sub("<hex>", p)
    p = _NUM_RE.sub("N", p)
    return _flat(p)


def _total(results: dict) -> str:
    t = results.get("hits", {}).get("total", {})
    v, rel = t.get("value", 0), t.get("relation", "eq")
    return f"{v}+" if rel == "gte" else str(v)


def _hit_line(hit: dict, truncate: bool = True) -> str:
    src = hit.get("_source", {})
    msg = _flat(str(src.get("message", "")))
    if truncate and len(msg) > MSG_TRUNCATE:
        msg = f"{msg[:MSG_TRUNCATE]}…[full: elk_get_log id={hit['_id']} index={hit['_index']}]"
    return "\t".join([
        _ts(src),
        str(src.get("level", "-")),
        str(src.get("namespace", "-")),
        str(src.get("podname", "-")),
        msg,
    ])


def _render_hits(hits: List[dict], budget: int = MAX_RESPONSE_CHARS) -> tuple:
    """Render hits as TSV under a character budget. Returns (lines, n_rendered)."""
    lines = ["timestamp\tlevel\tnamespace\tpodname\tmessage"]
    used = len(lines[0])
    n = 0
    for hit in hits:
        line = _hit_line(hit)
        if used + len(line) > budget and n > 0:
            lines.append(f"…response truncated at character budget after {n} rows")
            break
        lines.append(line)
        used += len(line)
        n += 1
    return lines, n


def _time_span_hours(client: ELKQueryClient, hours: int, start_time: Optional[str],
                     end_time: Optional[str]) -> float:
    if not start_time:
        return float(hours)
    t0 = client._parse_datetime(start_time)
    t1 = client._parse_datetime(end_time) if end_time else datetime.now(timezone.utc)
    return max((t1 - t0).total_seconds() / 3600.0, 0.01)


def _histogram_interval(span_hours: float) -> str:
    for limit, interval in [(0.5, "1m"), (2, "5m"), (8, "15m"), (24, "1h"),
                            (96, "4h"), (24 * 14, "12h")]:
        if span_hours <= limit:
            return interval
    return "1d"


def _terms_agg(field: str, size: int = 10) -> dict:
    return {"terms": {"field": field, "size": size}}


TIME_DOC = "Time range is UTC: either hours back from now, or start_time/end_time ('YYYY-MM-DD HH:MM:SS')."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def elk_overview(
    region: str,
    hours: int = 1,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    namespace: Optional[str] = None,
    namespace_contains: Optional[str] = None,
    level: Optional[str] = None,
    pod_name: Optional[str] = None,
    pod_name_contains: Optional[str] = None,
    cluster_name: Optional[str] = None,
    compute_resource_id: Optional[str] = None,
    message: Optional[str] = None,
    message_phrase: Optional[str] = None,
) -> str:
    """ALWAYS CALL THIS FIRST when investigating logs. Returns counts only (no log
    lines): a time histogram plus top levels/namespaces/pods matching the filters.
    Use it to find WHEN errors spiked and WHERE (which namespace/pod), then drill
    down with elk_error_patterns / elk_search. Region: ap-south-1 (Mumbai) or
    us-east-1. Time range is UTC (hours back, or start_time/end_time
    'YYYY-MM-DD HH:MM:SS')."""
    client = _client(region)
    level = _check_level(level)
    span = _time_span_hours(client, hours, start_time, end_time)
    interval = _histogram_interval(span)

    aggs = {
        "timeline": {
            "date_histogram": {"field": "@timestamp", "fixed_interval": interval,
                               "min_doc_count": 0},
            "aggs": {"levels": _terms_agg("level", 5)},
        },
        "levels": _terms_agg("level"),
        "namespaces": _terms_agg("namespace"),
        "pods": _terms_agg("podname", 15),
    }
    kwargs = dict(
        namespace=namespace, namespace_contains=namespace_contains,
        extra_filters=_level_filter(level),
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        message_contains=message, message_phrase=message_phrase,
        hours=hours, start_time=start_time, end_time=end_time,
    )
    results = client.query_logs(size=0, aggs=aggs, **kwargs)

    out = [f"total matching logs: {_total(results)} (histogram interval: {interval})"]
    ag = results.get("aggregations", {})

    out.append("\ntimeline (bucket_start_utc  count  by_level):")
    for b in ag.get("timeline", {}).get("buckets", []):
        if b["doc_count"] == 0:
            continue
        lv = " ".join(f"{x['key']}:{x['doc_count']}" for x in b["levels"]["buckets"])
        out.append(f"  {b['key_as_string'][:16]}  {b['doc_count']}  {lv}")

    for name, label in [("levels", "levels"), ("namespaces", "top namespaces"),
                        ("pods", "top pods")]:
        buckets = ag.get(name, {}).get("buckets", [])
        if buckets:
            out.append(f"\n{label}: " + ", ".join(
                f"{b['key']}={b['doc_count']}" for b in buckets))

    url = client.generate_kibana_url(
        namespace=namespace, namespace_contains=namespace_contains, log_level=level,
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        message_contains=message, message_phrase=message_phrase,
        hours=hours, start_time=start_time, end_time=end_time)
    out.append(f"\nkibana_url: {url}")
    return "\n".join(out)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def elk_error_patterns(
    region: str,
    hours: int = 1,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    namespace: Optional[str] = None,
    namespace_contains: Optional[str] = None,
    level: Optional[str] = "error",
    pod_name: Optional[str] = None,
    pod_name_contains: Optional[str] = None,
    cluster_name: Optional[str] = None,
    compute_resource_id: Optional[str] = None,
    message: Optional[str] = None,
    sample_size: int = 500,
    top: int = 30,
) -> str:
    """Deduplicate repeated log messages into patterns. Samples up to sample_size
    matching logs (default level=error; pass level=None-equivalent by setting
    level='info' etc. as needed), normalizes IDs/numbers, and returns one row per
    unique pattern with count, first/last seen, pods, and a sample doc id for
    elk_get_log. Massively cheaper than reading raw error logs. Time is UTC."""
    client = _client(region)
    level = _check_level(level)
    sample_size = min(max(sample_size, 1), 500)

    results = client.query_logs(
        namespace=namespace, namespace_contains=namespace_contains,
        extra_filters=_level_filter(level),
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        message_contains=message,
        hours=hours, start_time=start_time, end_time=end_time,
        size=sample_size, source_fields=BASE_FIELDS)

    hits = results.get("hits", {}).get("hits", [])
    groups: Dict[str, dict] = {}
    for hit in hits:
        src = hit.get("_source", {})
        key = _pattern(str(src.get("message", "")))
        g = groups.setdefault(key, {
            "count": 0, "first": _ts(src), "last": _ts(src), "pods": set(),
            "sample_id": hit["_id"], "sample_index": hit["_index"],
        })
        g["count"] += 1
        t = _ts(src)
        g["first"], g["last"] = min(g["first"], t), max(g["last"], t)
        g["pods"].add(str(src.get("podname", "-")))

    out = [f"total matching: {_total(results)}; sampled {len(hits)} logs "
           f"-> {len(groups)} unique patterns"]
    out.append("count\tfirst_seen\tlast_seen\tpods\tpattern\tsample")
    ranked = sorted(groups.items(), key=lambda kv: -kv[1]["count"])[:top]
    for key, g in ranked:
        pods = ",".join(sorted(g["pods"])[:3]) + ("…" if len(g["pods"]) > 3 else "")
        out.append(f"{g['count']}\t{g['first']}\t{g['last']}\t{pods}\t{key}\t"
                   f"id={g['sample_id']} index={g['sample_index']}")

    url = client.generate_kibana_url(
        namespace=namespace, namespace_contains=namespace_contains, log_level=level,
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        message_contains=message, hours=hours, start_time=start_time, end_time=end_time)
    out.append(f"kibana_url: {url}")
    return "\n".join(out)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def elk_search(
    region: str,
    hours: int = 1,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    namespace: Optional[str] = None,
    namespace_contains: Optional[str] = None,
    level: Optional[str] = None,
    pod_name: Optional[str] = None,
    pod_name_contains: Optional[str] = None,
    cluster_name: Optional[str] = None,
    context_id: Optional[str] = None,
    compute_resource_id: Optional[str] = None,
    log_file_path_contains: Optional[str] = None,
    message: Optional[str] = None,
    message_phrase: Optional[str] = None,
    size: int = 20,
    extra_fields: Optional[List[str]] = None,
    cursor: Optional[List] = None,
    order: str = "desc",
) -> str:
    """Fetch matching log lines as compact TSV (timestamp, level, namespace, pod,
    message). Messages over 500 chars are truncated with the doc id — use
    elk_get_log for the full text (e.g. complete stack traces). Start with the
    default size=20; page with the returned next_cursor rather than large sizes.
    Prefer elk_overview/elk_error_patterns first to narrow filters. Time is UTC."""
    client = _client(region)
    level = _check_level(level)
    size = min(max(size, 1), 100)
    if order not in ("asc", "desc"):
        raise ValueError("order must be 'asc' or 'desc'")
    fields = BASE_FIELDS + [f for f in (extra_fields or []) if f not in BASE_FIELDS]

    results = client.query_logs(
        namespace=namespace, namespace_contains=namespace_contains,
        extra_filters=_level_filter(level),
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, context_id=context_id,
        compute_resource_id=compute_resource_id,
        log_file_path_contains=log_file_path_contains,
        message_contains=message, message_phrase=message_phrase,
        hours=hours, start_time=start_time, end_time=end_time,
        size=size, source_fields=fields, search_after=cursor,
        sort_order=order)

    hits = results.get("hits", {}).get("hits", [])
    out = [f"total matching: {_total(results)}; showing {len(hits)} (order: {order})"]
    lines, n = _render_hits(hits)
    out.extend(lines)

    if extra_fields:
        out.append("\nextra_fields per row:")
        for hit in hits[:n]:
            src = hit.get("_source", {})
            extras = {f: src.get(f) for f in extra_fields if src.get(f) is not None}
            if extras:
                out.append(f"  {_ts(src)}  {json.dumps(extras, default=str)}")

    if len(hits) == size and hits:
        out.append(f"next_cursor: {json.dumps(hits[n - 1].get('sort', []))}")

    url = client.generate_kibana_url(
        namespace=namespace, namespace_contains=namespace_contains, log_level=level,
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, context_id=context_id,
        compute_resource_id=compute_resource_id,
        log_file_path_contains=log_file_path_contains,
        message_contains=message, message_phrase=message_phrase,
        hours=hours, start_time=start_time, end_time=end_time)
    out.append(f"kibana_url: {url}")
    return "\n".join(out)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def elk_get_log(region: str, doc_id: str, index: str) -> str:
    """Fetch ONE complete log document (all fields, full untruncated message /
    stack trace) by doc id and index — both are given in elk_search /
    elk_error_patterns truncation markers (id=… index=…)."""
    client = _client(region)
    # tolerate ids/indices copied with stray marker punctuation
    doc_id = doc_id.strip("[]()'\" ,")
    index = index.strip("[]()'\" ,")
    import requests as _requests
    resp = _requests.post(
        f"{client.es_url}/{index}/_search",
        headers={"Authorization": f"ApiKey {client.api_key}",
                 "Content-Type": "application/json"},
        json={"query": {"ids": {"values": [doc_id]}}, "size": 1},
        timeout=60)
    resp.raise_for_status()
    hits = resp.json().get("hits", {}).get("hits", [])
    if not hits:
        return f"no document found with id={doc_id} in index={index}"
    src = hits[0]["_source"]
    message = src.pop("message", "")
    out = [f"{k}: {json.dumps(v, default=str)}" for k, v in sorted(src.items())]
    out.append(f"message:\n{message}")
    return "\n".join(out)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def elk_context(
    region: str,
    at: str,
    namespace: Optional[str] = None,
    pod_name: Optional[str] = None,
    pod_name_contains: Optional[str] = None,
    cluster_name: Optional[str] = None,
    compute_resource_id: Optional[str] = None,
    log_file_path_contains: Optional[str] = None,
    before_s: int = 60,
    after_s: int = 60,
    size: int = 40,
) -> str:
    """Show what happened immediately BEFORE and AFTER a moment for a pod/namespace
    — use after finding an interesting error to understand the sequence of events.
    'at' is a UTC timestamp ('YYYY-MM-DD HH:MM:SS'); before_s/after_s widen the
    window. Returns TSV rows with a >>> marker at 'at'."""
    client = _client(region)
    size = min(max(size, 2), 100)
    center = client._parse_datetime(at)
    t0 = (center - timedelta(seconds=before_s)).strftime("%Y-%m-%d %H:%M:%S")
    t1 = (center + timedelta(seconds=after_s)).strftime("%Y-%m-%d %H:%M:%S")
    at_s = center.strftime("%Y-%m-%d %H:%M:%S")

    common = dict(
        namespace=namespace, pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        log_file_path_contains=log_file_path_contains,
        size=size // 2, source_fields=BASE_FIELDS)
    before = client.query_logs(start_time=t0, end_time=at_s, sort_order="desc", **common)
    after = client.query_logs(start_time=at_s, end_time=t1, sort_order="asc", **common)

    before_hits = list(reversed(before.get("hits", {}).get("hits", [])))
    after_hits = after.get("hits", {}).get("hits", [])

    out = [f"context around {at_s} UTC (-{before_s}s / +{after_s}s); "
           f"before: {_total(before)} matching, after: {_total(after)} matching"]
    lines, _ = _render_hits(before_hits, budget=MAX_RESPONSE_CHARS // 2)
    out.extend(lines)
    out.append(f">>> {at_s}  <<< moment of interest")
    lines, _ = _render_hits(after_hits, budget=MAX_RESPONSE_CHARS // 2)
    out.extend(lines[1:])  # skip repeated header

    url = client.generate_kibana_url(
        namespace=namespace, pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        log_file_path_contains=log_file_path_contains,
        start_time=t0, end_time=t1)
    out.append(f"kibana_url: {url}")
    return "\n".join(out)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
def elk_dump(
    region: str,
    output_path: str,
    hours: int = 1,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    namespace: Optional[str] = None,
    namespace_contains: Optional[str] = None,
    level: Optional[str] = None,
    pod_name: Optional[str] = None,
    pod_name_contains: Optional[str] = None,
    cluster_name: Optional[str] = None,
    compute_resource_id: Optional[str] = None,
    log_file_path_contains: Optional[str] = None,
    message: Optional[str] = None,
    size: int = 2000,
) -> str:
    """Bulk-export matching logs to a local JSONL file (one log per line with
    timestamp, level, namespace, podname, message) WITHOUT loading them into
    context, then Grep/Read the file selectively. Use for haystack searches when
    filters can't narrow enough. output_path should be inside your working
    directory. Returns count, size, and top patterns. Time is UTC."""
    client = _client(region)
    level = _check_level(level)
    size = min(max(size, 1), 5000)

    results = client.query_logs(
        namespace=namespace, namespace_contains=namespace_contains,
        extra_filters=_level_filter(level),
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        log_file_path_contains=log_file_path_contains, message_contains=message,
        hours=hours, start_time=start_time, end_time=end_time,
        size=size, source_fields=BASE_FIELDS + ["contextId", "log.file.path"])

    hits = results.get("hits", {}).get("hits", [])
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for hit in hits:
            src = hit.get("_source", {})
            src["_id"], src["_index"] = hit["_id"], hit["_index"]
            f.write(json.dumps(src, default=str) + "\n")

    groups: Dict[str, int] = {}
    for hit in hits:
        key = _pattern(str(hit.get("_source", {}).get("message", "")))
        groups[key] = groups.get(key, 0) + 1
    top = sorted(groups.items(), key=lambda kv: -kv[1])[:10]

    out = [f"wrote {len(hits)} logs ({path.stat().st_size} bytes) to {path}",
           f"total matching in ELK: {_total(results)}",
           "top patterns:"]
    out += [f"  {c}\t{p}" for p, c in top]
    url = client.generate_kibana_url(
        namespace=namespace, namespace_contains=namespace_contains, log_level=level,
        pod_name=pod_name, pod_name_contains=pod_name_contains,
        cluster_name=cluster_name, compute_resource_id=compute_resource_id,
        log_file_path_contains=log_file_path_contains, message_contains=message,
        hours=hours, start_time=start_time, end_time=end_time)
    out.append(f"kibana_url: {url}")
    return "\n".join(out)


if __name__ == "__main__":
    mcp.run()
