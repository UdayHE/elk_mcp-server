# elk-mcp-server

An MCP (Model Context Protocol) server that gives an AI coding agent
(Claude Code or any other MCP host) read-only access to Elastic Cloud logs
across two regions, plus a standalone CLI for humans.

## Why

Wrapping a CLI script directly means every agent session pays a fixed "read
the source to learn the flags" tax, then shells out and re-parses
pretty-printed text. This server exposes the same querying logic as six
purpose-built MCP tools instead: Claude sees a compact schema, never the
implementation, and gets back compact TSV rather than raw JSON.

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

Create a `.env` file next to the scripts with the credentials for each
region you use:

```bash
# ap-south-1 (Mumbai)
ES_URL_MUMBAI=https://...
ES_API_KEY_MUMBAI=...
KIBANA_URL_MUMBAI=https://...

# us-east-1
ES_URL_US_EAST=https://...
ES_API_KEY_US_EAST=...
KIBANA_URL_US_EAST=https://...
```

You only need the variables for the region(s) you plan to query. Never
commit `.env` — add it to `.gitignore`.

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
elk_mcp/
├── elk_query.py        # ELKQueryClient — core querying + Kibana URL builder, plus a CLI
├── elk_mcp_server.py   # FastMCP server exposing the 6 tools above
└── .env                # credentials, not tracked in git (create this yourself)
```
# elk_mcp-server
