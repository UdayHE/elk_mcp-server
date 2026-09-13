#!/usr/bin/env python3
"""
ELK Log Query Script
Fetches logs from Elastic Cloud with customizable filters

USAGE:
    python3 elk_query.py --region REGION [OPTIONS]

REQUIRED:
    --region REGION        Region to query: 'ap-south-1' (Mumbai) or 'us-east-1'

TIME RANGE OPTIONS:
    --hours N              Look back N hours from now (default: 1)
    --start-time TIME      Start time in UTC (format: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS')
    --end-time TIME        End time in UTC (format: 'YYYY-MM-DD HH:MM:SS', defaults to now)

    NOTE: All times are in UTC. If --start-time is provided, --hours is ignored.
    TIP: To convert IST to UTC, subtract 5 hours 30 minutes (e.g., 01:00 IST = 19:30 UTC previous day)

FILTER OPTIONS:
    --namespace NAME           Exact match by namespace
    --namespace-contains STR   Partial match by namespace
    --level LEVEL              Log level (info, error, warn)
    --pod-name NAME            Exact match by pod name
    --pod-name-contains STR    Partial match by pod name
    --context-id ID            Exact match by context ID
    --container-id ID          Exact match by container ID
    --compute-resource-id ID   Exact match by database.computeResourceId
    --log-file-path PATH       Exact match by log.file.path
    --log-file-path-contains STR  Partial match by log.file.path
    --message TEXT             Search text in message (tokenized full-text search)
    --message-phrase TEXT      Exact phrase search in message (uses match_phrase)

OUTPUT OPTIONS:
    --size N               Max number of logs (default: 100)
    --verbose, -v          Show detailed output
    --json                 Output raw JSON
    --output, -o FILE      Save output to file

ENVIRONMENT VARIABLES:
    For ap-south-1 (Mumbai):
        ES_URL_MUMBAI          Elasticsearch URL for Mumbai region (for API queries)
        ES_API_KEY_MUMBAI      API key for Mumbai region
        KIBANA_URL_MUMBAI      Kibana URL for Mumbai region (for generating browser links)
        KIBANA_INDEX_ID_MUMBAI Kibana index pattern ID for Mumbai region (for generating browser links)

    For us-east-1:
        ES_URL_US_EAST         Elasticsearch URL for US East region (for API queries)
        ES_API_KEY_US_EAST     API key for US East region
        KIBANA_URL_US_EAST     Kibana URL for US East region (for generating browser links)
        KIBANA_INDEX_ID_US_EAST Kibana index pattern ID for US East region (for generating browser links)

    All values are read from the .env file in the project root. Never commit
    credentials to this file or anywhere else in the repository.

EXAMPLES:
    # Get last 1 hour of logs from Mumbai region
    python3 elk_query.py --region ap-south-1 --namespace tenant-abc

    # Get last 24 hours of error logs from US East region
    python3 elk_query.py --region us-east-1 --namespace tenant-abc --level error --hours 24

    # Query logs between specific UTC times
    python3 elk_query.py --region ap-south-1 --start-time "2026-01-27 10:00:00" --end-time "2026-01-27 12:00:00"

    # Query from a specific UTC time until now
    python3 elk_query.py --region ap-south-1 --start-time "2026-01-27 10:00:00" --namespace tenant-abc

    # Using ISO format for times
    python3 elk_query.py --region us-east-1 --start-time "2026-01-27T10:00:00" --end-time "2026-01-27T12:00:00"

    # Query all logs from a specific date (00:00:00 UTC)
    python3 elk_query.py --region ap-south-1 --start-time "2026-01-27"

    # Save output to file
    python3 elk_query.py --region ap-south-1 --namespace tenant-abc --hours 2 --output logs.txt

    # Get JSON output
    python3 elk_query.py --region us-east-1 --namespace tenant-abc --json --output logs.json

    # IST to UTC conversion examples:
    #   01:00 IST Jan 20 = 19:30 UTC Jan 19
    #   06:00 IST Jan 20 = 00:30 UTC Jan 20
    #   12:00 IST Jan 20 = 06:30 UTC Jan 20
"""

import os
import json
import requests
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def _load_region_env_mapping() -> Dict[str, Dict[str, str]]:
    """Load the region-to-environment-variable mapping from the REGION_ENV_MAPPING
    JSON blob in the .env file."""
    raw = os.getenv('REGION_ENV_MAPPING')
    if not raw:
        raise ValueError(
            "REGION_ENV_MAPPING is not set. Please define it as a JSON object in the .env file."
        )
    return json.loads(raw)


class ELKQueryClient:
    # Region to environment variable mapping (loaded from .env)
    REGION_ENV_MAPPING = _load_region_env_mapping()

    def __init__(self, region: str):
        if region not in self.REGION_ENV_MAPPING:
            raise ValueError(f"Invalid region '{region}'. Supported regions: {list(self.REGION_ENV_MAPPING.keys())}")

        self.region = region
        region_config = self.REGION_ENV_MAPPING[region]

        self.es_url = os.getenv(region_config['es_url'])
        self.api_key = os.getenv(region_config['api_key'])
        self.kibana_url = os.getenv(region_config['kibana_url'])
        self.kibana_index_id = os.getenv(region_config['kibana_index_id'])

        if not self.es_url or not self.api_key:
            raise ValueError(
                f"Missing environment variables for region '{region}' ({region_config['display_name']}). "
                f"Please set {region_config['es_url']} and {region_config['api_key']} in .env file"
            )

        if not self.kibana_url or not self.kibana_index_id:
            missing = [
                var for var, val in (
                    (region_config['kibana_url'], self.kibana_url),
                    (region_config['kibana_index_id'], self.kibana_index_id),
                ) if not val
            ]
            print(f"Warning: {', '.join(missing)} not set. Kibana browser links will not be generated.")

    def _parse_datetime(self, dt_string: str) -> datetime:
        """
        Parse a datetime string in various formats (input is treated as UTC).

        Supported formats:
            - 'YYYY-MM-DD HH:MM:SS'
            - 'YYYY-MM-DDTHH:MM:SS'
            - 'YYYY-MM-DD HH:MM'
            - 'YYYY-MM-DDTHH:MM'
            - 'YYYY-MM-DD'

        Returns:
            datetime object in UTC timezone (for Elasticsearch query)
        """
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%dT%H:%M',
            '%Y-%m-%d',
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(dt_string, fmt)
                # Treat input as UTC directly
                dt_utc = dt.replace(tzinfo=timezone.utc)
                return dt_utc
            except ValueError:
                continue

        raise ValueError(
            f"Unable to parse datetime '{dt_string}'. "
            f"Supported formats: 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DDTHH:MM:SS', "
            f"'YYYY-MM-DD HH:MM', 'YYYY-MM-DDTHH:MM', 'YYYY-MM-DD'"
        )

    def query_logs(
        self,
        namespace: Optional[str] = None,
        namespace_contains: Optional[str] = None,
        log_level: Optional[str] = None,
        pod_name: Optional[str] = None,
        pod_name_contains: Optional[str] = None,
        context_id: Optional[str] = None,
        container_id: Optional[str] = None,
        compute_resource_id: Optional[str] = None,
        log_file_path: Optional[str] = None,
        log_file_path_contains: Optional[str] = None,
        message_contains: Optional[str] = None,
        message_phrase: Optional[str] = None,
        cluster_name: Optional[str] = None,
        hours: int = 1,
        size: int = 100,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        source_fields: Optional[List[str]] = None,
        search_after: Optional[List] = None,
        aggs: Optional[Dict] = None,
        sort_order: str = "desc",
        extra_filters: Optional[List[Dict]] = None
    ) -> Dict:
        """
        Query logs from ELK with various filters

        Args:
            namespace: Exact match filter by namespace (e.g., 'tenant-infrateam001')
            namespace_contains: Partial match filter by namespace
            log_level: Exact match filter by log level (e.g., 'error', 'info', 'warn')
            pod_name: Exact match filter by pod name
            pod_name_contains: Partial match filter by pod name
            context_id: Exact match filter by context ID
            container_id: Exact match filter by container ID
            compute_resource_id: Exact match filter by database.computeResourceId
            log_file_path: Exact match filter by log.file.path
            log_file_path_contains: Partial match filter by log.file.path
            message_contains: Search for text in log message
            cluster_name: Exact match filter by cluster_name (for shared/common services)
            hours: Number of hours to look back (default: 1, ignored if start_time is provided)
            size: Maximum number of logs to return (default: 100)
            start_time: Start time in UTC (format: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS')
            end_time: End time in UTC (format: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS', defaults to now)
            source_fields: Restrict returned _source to these fields (reduces payload size)
            search_after: Cursor from a previous response's last hit sort values, for pagination
            aggs: Elasticsearch aggregations to run; when set with size=0 no hits are fetched
            sort_order: Sort by @timestamp 'desc' (default) or 'asc'

        Returns:
            Dictionary containing the query results
        """

        # Build the query filters
        must_filters = []

        # Time range filter
        if start_time:
            # Parse start_time - support multiple formats
            time_from = self._parse_datetime(start_time)
            if end_time:
                time_to = self._parse_datetime(end_time)
            else:
                time_to = datetime.now(timezone.utc)
        else:
            # Use hours-based calculation (default behavior)
            time_from = datetime.now(timezone.utc) - timedelta(hours=hours)
            time_to = datetime.now(timezone.utc)

        must_filters.append({
            "range": {
                "@timestamp": {
                    "gte": time_from.isoformat(),
                    "lte": time_to.isoformat()
                }
            }
        })

        # Add optional filters - exact match
        if namespace:
            must_filters.append({"match": {"namespace": namespace}})

        if log_level:
            must_filters.append({"match": {"level": log_level}})

        if pod_name:
            must_filters.append({"match": {"podname": pod_name}})

        if context_id:
            must_filters.append({"match": {"contextId": context_id}})

        if container_id:
            must_filters.append({"match": {"container_id": container_id}})

        if compute_resource_id:
            must_filters.append({"match": {"database.computeResourceId": compute_resource_id}})

        if log_file_path:
            must_filters.append({"match": {"log.file.path": log_file_path}})

        if cluster_name:
            must_filters.append({"match": {"cluster_name": cluster_name}})

        # Add optional filters - partial match (contains)
        # Using wildcard for partial string matching on keyword fields
        if namespace_contains:
            must_filters.append({"wildcard": {"namespace": f"*{namespace_contains}*"}})

        if pod_name_contains:
            must_filters.append({"wildcard": {"podname": f"*{pod_name_contains}*"}})

        if log_file_path_contains:
            must_filters.append({"wildcard": {"log.file.path": f"*{log_file_path_contains}*"}})

        if message_contains:
            must_filters.append({"match": {"message": message_contains}})

        if message_phrase:
            must_filters.append({"match_phrase": {"message": message_phrase}})

        if extra_filters:
            must_filters.extend(extra_filters)


        # Build the query
        query = {
            "query": {
                "bool": {
                    "must": must_filters
                }
            },
            "size": size,
            # _doc tiebreaker keeps search_after pagination stable across
            # same-millisecond log lines
            "sort": [{"@timestamp": {"order": sort_order}}, {"_doc": {"order": "asc"}}]
        }

        if source_fields:
            query["_source"] = source_fields

        if search_after:
            query["search_after"] = search_after

        if aggs:
            query["aggs"] = aggs


        # Make the API request directly to Elasticsearch
        url = f"{self.es_url}/*/_search"
        headers = {
            "Authorization": f"ApiKey {self.api_key}",
            "Content-Type": "application/json"
        }

        # Set timeout to prevent hanging during network issues (60 seconds total)
        response = requests.post(url, headers=headers, json=query, timeout=60)
        response.raise_for_status()

        return response.json()

    def generate_kibana_url(
        self,
        namespace: Optional[str] = None,
        namespace_contains: Optional[str] = None,
        log_level: Optional[str] = None,
        pod_name: Optional[str] = None,
        pod_name_contains: Optional[str] = None,
        context_id: Optional[str] = None,
        container_id: Optional[str] = None,
        compute_resource_id: Optional[str] = None,
        log_file_path: Optional[str] = None,
        log_file_path_contains: Optional[str] = None,
        message_contains: Optional[str] = None,
        message_phrase: Optional[str] = None,
        cluster_name: Optional[str] = None,
        hours: int = 1,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> Optional[str]:
        """
        Generate a Kibana Discover URL with the same filters as the query.

        Returns:
            Kibana URL string that can be opened in a browser, or None if Kibana URL not configured
        """
        if not self.kibana_url or not self.kibana_index_id:
            return None
        # Calculate time range
        if start_time:
            time_from = self._parse_datetime(start_time)
            if end_time:
                time_to = self._parse_datetime(end_time)
            else:
                time_to = datetime.now(timezone.utc)
        else:
            time_from = datetime.now(timezone.utc) - timedelta(hours=hours)
            time_to = datetime.now(timezone.utc)

        # Format times for Kibana URL
        time_from_str = time_from.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        time_to_str = time_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # Build KQL query parts
        kql_parts = []

        # Exact match filters
        if namespace:
            kql_parts.append(f'namespace: "{namespace}"')
        if log_level:
            kql_parts.append(f'level: "{log_level}"')
        if pod_name:
            kql_parts.append(f'podname: "{pod_name}"')
        if context_id:
            kql_parts.append(f'contextId: "{context_id}"')
        if container_id:
            kql_parts.append(f'container_id: "{container_id}"')
        if compute_resource_id:
            kql_parts.append(f'database.computeResourceId: "{compute_resource_id}"')
        if log_file_path:
            kql_parts.append(f'log.file.path: "{log_file_path}"')
        if cluster_name:
            kql_parts.append(f'cluster_name: "{cluster_name}"')

        # Partial match filters (wildcard)
        if namespace_contains:
            kql_parts.append(f'namespace: *{namespace_contains}*')
        if pod_name_contains:
            kql_parts.append(f'podname: *{pod_name_contains}*')
        if log_file_path_contains:
            kql_parts.append(f'log.file.path: *{log_file_path_contains}*')

        # Message search
        if message_contains:
            # For match query (tokenized search), use unquoted terms in KQL
            # This allows Kibana to match documents containing any of the words
            kql_parts.append(f'message: {message_contains}')
        if message_phrase:
            # For match_phrase query, use quoted string in KQL for exact phrase matching
            kql_parts.append(f'message: "{message_phrase}"')

        # Combine KQL parts with AND
        kql_query = " and ".join(kql_parts) if kql_parts else "*"

        # Build the Kibana URL
        # Global state (_g): time range and refresh settings
        global_state = f"(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{time_from_str}',to:'{time_to_str}'))"

        # App state (_a): query, columns, sorting, index pattern
        # Using region-specific Kibana data view index ID
        app_state = f"(columns:!(namespace,podname,level,message),index:'{self.kibana_index_id}',query:(language:kuery,query:'{kql_query}'),sort:!(!('@timestamp',desc)))"

        # Construct full URL
        # Rison encoding requires certain characters to remain unencoded: :()!,'@
        # Only encode spaces and special non-rison characters
        rison_safe_chars = ":()!,'@*"
        kibana_url = f"{self.kibana_url}/app/discover#/?_g={quote(global_state, safe=rison_safe_chars)}&_a={quote(app_state, safe=rison_safe_chars)}"

        return kibana_url

    def print_logs(self, results: Dict, verbose: bool = False, output_file: Optional[str] = None, kibana_url: Optional[str] = None):
        """Print logs in a readable format and optionally save to file"""

        hits = results.get('hits', {}).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        # Build output content
        output_lines = []
        output_lines.append(f"\n{'='*80}")
        output_lines.append(f"Total logs found: {total}")
        output_lines.append(f"Showing: {len(hits)} logs")
        if kibana_url:
            output_lines.append(f"\nKibana URL (open in browser):")
            output_lines.append(kibana_url)
        output_lines.append(f"{'='*80}\n")

        for i, hit in enumerate(hits, 1):
            source = hit['_source']

            timestamp = source.get('@timestamp', 'N/A')
            level = source.get('level', 'N/A').upper()
            namespace = source.get('namespace', 'N/A')
            pod_name = source.get('podname', 'N/A')
            message = source.get('message', 'N/A').strip()

            output_lines.append(f"[{i}] {timestamp}")
            output_lines.append(f"    Level: {level} | Namespace: {namespace}")
            output_lines.append(f"    Pod: {pod_name}")
            output_lines.append(f"    Message: {message}")

            if verbose:
                context_id = source.get('contextId', 'N/A')
                output_lines.append(f"    ContextID: {context_id}")

                if 'database' in source:
                    db_info = source['database']
                    output_lines.append(f"    Database: {db_info.get('serviceName', 'N/A')}")

            output_lines.append(f"{'-'*80}\n")

        # Print to console
        for line in output_lines:
            print(line)

        # Save to file if specified
        if output_file:
            with open(output_file, 'w') as f:
                f.write('\n'.join(output_lines))
            print(f"\n✓ Logs saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Query logs from ELK')

    # Required region argument
    regions = list(ELKQueryClient.REGION_ENV_MAPPING.keys())
    parser.add_argument('--region', type=str, required=True,
                        choices=regions,
                        help=f"Region to query: {', '.join(regions)}")

    # Exact match filters
    parser.add_argument('--namespace', type=str, help='Exact match filter by namespace')
    parser.add_argument('--level', type=str, help='Exact match filter by log level (info, error, warn)')
    parser.add_argument('--pod-name', type=str, help='Exact match filter by pod name')
    parser.add_argument('--context-id', type=str, help='Exact match filter by context ID')
    parser.add_argument('--container-id', type=str, help='Exact match filter by container ID')
    parser.add_argument('--compute-resource-id', type=str, help='Exact match filter by database.computeResourceId')
    parser.add_argument('--log-file-path', type=str, help='Exact match filter by log.file.path')
    parser.add_argument('--cluster-name', type=str, help='Exact match filter by cluster_name (for shared/common services)')

    # Partial match filters (contains)
    parser.add_argument('--namespace-contains', type=str, help='Partial match filter by namespace')
    parser.add_argument('--pod-name-contains', type=str, help='Partial match filter by pod name')
    parser.add_argument('--log-file-path-contains', type=str, help='Partial match filter by log.file.path')

    # Message search
    parser.add_argument('--message', type=str, help='Search for text in message (tokenized full-text)')
    parser.add_argument('--message-phrase', type=str, help='Exact phrase search in message (uses match_phrase)')

    # Time range options (all times are in UTC)
    parser.add_argument('--hours', type=int, default=1, help='Hours to look back (default: 1, ignored if --start-time is provided)')
    parser.add_argument('--start-time', type=str, help="Start time in UTC (format: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS')")
    parser.add_argument('--end-time', type=str, help="End time in UTC (format: 'YYYY-MM-DD HH:MM:SS', defaults to now)")

    # Other options
    parser.add_argument('--size', type=int, default=100, help='Max number of logs (default: 100)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    parser.add_argument('--json', action='store_true', help='Output raw JSON')
    parser.add_argument('--output', '-o', type=str, help='Save output to file')

    args = parser.parse_args()

    try:
        client = ELKQueryClient(region=args.region)
        print(f"Querying ELK in region: {args.region} ({ELKQueryClient.REGION_ENV_MAPPING[args.region]['display_name']})")

        results = client.query_logs(
            namespace=args.namespace,
            namespace_contains=args.namespace_contains,
            log_level=args.level,
            pod_name=args.pod_name,
            pod_name_contains=args.pod_name_contains,
            context_id=args.context_id,
            container_id=args.container_id,
            compute_resource_id=args.compute_resource_id,
            log_file_path=args.log_file_path,
            log_file_path_contains=args.log_file_path_contains,
            message_contains=args.message,
            message_phrase=args.message_phrase,
            cluster_name=args.cluster_name,
            hours=args.hours,
            size=args.size,
            start_time=args.start_time,
            end_time=args.end_time
        )

        # Generate Kibana URL for manual verification
        kibana_url = client.generate_kibana_url(
            namespace=args.namespace,
            namespace_contains=args.namespace_contains,
            log_level=args.level,
            pod_name=args.pod_name,
            pod_name_contains=args.pod_name_contains,
            context_id=args.context_id,
            container_id=args.container_id,
            compute_resource_id=args.compute_resource_id,
            log_file_path=args.log_file_path,
            log_file_path_contains=args.log_file_path_contains,
            message_contains=args.message,
            message_phrase=args.message_phrase,
            cluster_name=args.cluster_name,
            hours=args.hours,
            start_time=args.start_time,
            end_time=args.end_time
        )

        if args.json:
            # Add kibana_url to the JSON output
            output_data = {
                "kibana_url": kibana_url,
                "results": results
            }
            output_content = json.dumps(output_data, indent=2)
            print(output_content)

            # Save JSON to file if output specified
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(output_content)
                print(f"\n✓ JSON output saved to: {args.output}")
        else:
            client.print_logs(results, verbose=args.verbose, output_file=args.output, kibana_url=kibana_url)

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
