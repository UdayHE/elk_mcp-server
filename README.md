# elk-mcp-server

An MCP (Model Context Protocol) server that lets an AI coding agent (Claude
Code or any other MCP host) read Elastic Cloud logs across multiple regions
(currently `ap-south-1` Mumbai and `us-east-1`, configured in `.env`) —
read-only, nothing can be changed or deleted. A standalone CLI is included
for humans who want to run the same queries by hand.

## Why

An agent could just shell out to the CLI script directly, but that's
expensive in tokens: it has to read the whole script to learn the flags,
then parse pretty-printed, verbose text back out of every response.

This server avoids both costs. It wraps the same querying logic in six
purpose-built MCP tools, so the agent sees a short, ready-made list of
tools instead of a script to read, and gets back compact TSV rows instead
of raw JSON. Less text in means less text (and cost) out.

## Tools

| Tool | Use it to |
|---|---|
| `elk_overview` | **Always call first.** Time histogram + top levels/namespaces/pods — counts only, no log lines. |
| `elk_error_patterns` | Deduplicate repeated errors (UUIDs/hex/IPs/numbers normalized) into ranked patterns. |
| `elk_search` | Compact TSV log rows, field-filtered, cursor-paginated. |
| `elk_get_log` | One full document (untruncated message/stack trace) by id + index. |
| `elk_context` | Logs immediately before/after a specific moment for a pod/namespace. |
| `elk_dump` | Bulk export to a local JSONL file for `grep`, without loading results into context. |

All tools are read-only (`ToolAnnotations(readOnlyHint=True)`) and cover both
`ap-south-1` (Mumbai) and `us-east-1`.

## Setup

Requires [`uv`](https://docs.astral.sh/uv/).

Copy `.env.example` to `.env` and fill in the credentials for each region you
use:

```bash
cp .env.example .env
```

```bash
# maps each region to the env var names below — only edit this if you add
# a region or rename a variable
REGION_ENV_MAPPING={"ap-south-1":{...},"us-east-1":{...}}

# ap-south-1 (Mumbai)
ES_URL_MUMBAI=https://...
ES_API_KEY_MUMBAI=...
KIBANA_URL_MUMBAI=https://...
KIBANA_INDEX_ID_MUMBAI=...

# us-east-1
ES_URL_US_EAST=https://...
ES_API_KEY_US_EAST=...
KIBANA_URL_US_EAST=https://...
KIBANA_INDEX_ID_US_EAST=...
```

You only need the credentials for the region(s) you plan to query.
`REGION_ENV_MAPPING` and the `KIBANA_INDEX_ID_*` values aren't secrets (just
config), but `.env` as a whole is gitignored — never commit it.

### Use as an MCP server (Claude Code / any MCP host)

Point your MCP host at this script, e.g. a `.mcp.json` in your project root:

```json
{
  "mcpServers": {
    "elk": {
      "command": "uv",
      "args": ["run", "--script", "/path/to/elk_mcp_server.py"]
    }
  }
}
```

`uv` resolves the pinned dependencies from the script's inline metadata, so
there's no separate install step — the `elk` server starts on demand.

### Use the CLI directly

```bash
python3 elk_query.py --region ap-south-1 --namespace tenant-abc --hours 2
python3 elk_query.py --region us-east-1 --namespace tenant-abc --level error --json -o logs.json
```

Run `python3 elk_query.py --help` for the full flag list.

## Testing

```bash
uv sync --group dev
uv run pytest
```

Tests cover the pure logic (timestamp parsing, pattern dedup, TSV rendering,
Kibana URL building, request construction) with dummy credentials — no live
Elasticsearch connection required.

## Design notes

- **Shared core** — the MCP server imports `ELKQueryClient` from `elk_query.py`
  directly; the CLI and the AI-facing tools can never drift apart.
- **Token budget** — TSV output, a hard 20,000-char response cap with a visible
  truncation marker, and regex-based pattern dedup keep multi-step
  investigations inside an LLM's context window.
- **Cursor pagination** — `elk_search` uses `search_after` with a `_doc`
  tiebreaker, stable even across same-millisecond log lines.
- **Case-insensitive level matching** — Go/Python services log lowercase
  `error`, Java services log uppercase `ERROR`, in the same indices.
- **Kibana deep links** — every tool response includes a `kibana_url` built
  from the same filters, generated locally at zero extra query cost.

## Layout

```
elk-mcp-server/
├── elk_query.py        # ELKQueryClient — core querying + Kibana URL builder, plus a CLI
├── elk_mcp_server.py   # FastMCP server exposing the 6 tools above
├── tests/              # pytest suite for the pure logic in both scripts above
├── pyproject.toml      # dependencies + dev group (pytest), for `uv sync`/`uv run pytest`
├── uv.lock             # pinned dependency versions
├── .env.example        # template for the credentials/config below (safe to commit)
└── .env                # actual credentials + config, not tracked in git (create this yourself)
```
