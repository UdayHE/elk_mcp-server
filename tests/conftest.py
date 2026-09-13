import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Set dummy credentials before any module under test loads .env, so
# python-dotenv (which never overrides an already-set var) leaves these in
# place instead of picking up blank values from the real .env file.
os.environ.setdefault("REGION_ENV_MAPPING", (
    '{"ap-south-1":{"es_url":"ES_URL_MUMBAI","api_key":"ES_API_KEY_MUMBAI",'
    '"kibana_url":"KIBANA_URL_MUMBAI","kibana_index_id":"KIBANA_INDEX_ID_MUMBAI",'
    '"display_name":"Mumbai"},'
    '"us-east-1":{"es_url":"ES_URL_US_EAST","api_key":"ES_API_KEY_US_EAST",'
    '"kibana_url":"KIBANA_URL_US_EAST","kibana_index_id":"KIBANA_INDEX_ID_US_EAST",'
    '"display_name":"US East"}}'
))
os.environ.setdefault("ES_URL_MUMBAI", "https://es.mumbai.example.com")
os.environ.setdefault("ES_API_KEY_MUMBAI", "dummy-mumbai-key")
os.environ.setdefault("KIBANA_URL_MUMBAI", "https://kibana.mumbai.example.com")
os.environ.setdefault("KIBANA_INDEX_ID_MUMBAI", "8524f713-2a3e-49ce-8089-f833305f7512")
os.environ.setdefault("ES_URL_US_EAST", "https://es.useast.example.com")
os.environ.setdefault("ES_API_KEY_US_EAST", "dummy-useast-key")
os.environ.setdefault("KIBANA_URL_US_EAST", "https://kibana.useast.example.com")
os.environ.setdefault("KIBANA_INDEX_ID_US_EAST", "1086f0c0-40c4-11ed-b323-41c082c9d9de")


@pytest.fixture
def client():
    from elk_query import ELKQueryClient
    return ELKQueryClient(region="ap-south-1")
