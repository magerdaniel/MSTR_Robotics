import os
import json
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

# Path where browser-flow credentials are cached
_TOKEN_CACHE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "bq_user_token.json"
)


def _load_bq_config(config_path: str = None) -> dict:
    if config_path is None:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base, "config", "bq_config.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["bq"]


def _sa_key_abs(cfg: dict) -> str:
    sa_key_rel = cfg.get("sa_key_file", "")
    if not sa_key_rel:
        return ""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, sa_key_rel)


def _load_cached_token():
    """Load a previously saved user OAuth2 token from disk."""
    from google.oauth2.credentials import Credentials
    if os.path.exists(_TOKEN_CACHE):
        with open(_TOKEN_CACHE, "r") as f:
            data = json.load(f)
        creds = Credentials(
            token=data.get("token"),
            refresh_token=data.get("refresh_token"),
            token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
            scopes=_SCOPES,
        )
        return creds
    return None


def _save_token(creds):
    """Persist user OAuth2 token to disk so we don't re-authenticate every run."""
    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
    }
    os.makedirs(os.path.dirname(_TOKEN_CACHE), exist_ok=True)
    with open(_TOKEN_CACHE, "w") as f:
        json.dump(data, f, indent=2)


def login_browser(oauth_client_secret_file: str = None) -> bigquery.Client:
    """
    Authenticate via browser OAuth2 flow (no gcloud, no service account needed).
    Opens a browser window once; the token is cached in config/bq_user_token.json
    for future calls.

    Args:
        oauth_client_secret_file: path to the OAuth2 client secrets JSON downloaded
            from GCP Console → APIs & Services → Credentials → OAuth 2.0 Client IDs.
            Defaults to config/bq_oauth_client.json
    """
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    cfg = _load_bq_config()
    project  = cfg["project"]
    location = cfg.get("location", "US")

    if oauth_client_secret_file is None:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        oauth_client_secret_file = os.path.join(base, "config", "bq_oauth_client.json")

    # Try to reuse a cached token first
    creds = _load_cached_token()
    if creds:
        from google.auth.transport.requests import Request
        if not creds.expired:
            print("[bq_connector] using cached user token")
        elif creds.refresh_token:
            try:
                creds.refresh(Request())
                _save_token(creds)
                print("[bq_connector] token refreshed from cache")
            except Exception:
                print("[bq_connector] refresh token expired — re-opening browser login")
                creds = None   # force fresh browser login
        else:
            creds = None   # force re-login

    if creds is None:
        if not os.path.exists(oauth_client_secret_file):
            raise FileNotFoundError(
                f"\nOAuth2 client secrets file not found: {oauth_client_secret_file}\n"
                "To get it:\n"
                "  1. Open https://console.cloud.google.com/apis/credentials?project=mstr-tpc\n"
                "  2. Create an OAuth 2.0 Client ID (type: Desktop app)\n"
                "  3. Download the JSON → save as config/bq_oauth_client.json\n"
            )
        flow = InstalledAppFlow.from_client_secrets_file(
            oauth_client_secret_file, scopes=_SCOPES
        )
        creds = flow.run_local_server(port=0, open_browser=True)
        _save_token(creds)
        print("[bq_connector] browser login successful, token cached")

    client = bigquery.Client(project=project, credentials=creds, location=location)
    print(f"[bq_connector] connected to project={project}")
    return client


def get_bq_client(config_path: str = None) -> bigquery.Client:
    """
    Returns an authenticated BigQuery client.

    Auth priority:
      1. Service account key file  (config/bq_sa_key.json)
      2. Cached user OAuth2 token  (config/bq_user_token.json)
      3. Raises a clear error with instructions for both options

    Args:
        config_path: optional override path to bq_config.yml

    Returns:
        google.cloud.bigquery.Client
    """
    cfg = _load_bq_config(config_path)
    project  = cfg["project"]
    location = cfg.get("location", "US")

    # ── Option 1: service account key ────────────────────────────────────────
    sa_path = _sa_key_abs(cfg)
    if sa_path and os.path.exists(sa_path):
        credentials = service_account.Credentials.from_service_account_file(
            sa_path, scopes=_SCOPES
        )
        client = bigquery.Client(
            project=project, credentials=credentials, location=location
        )
        print(f"[bq_connector] service account: {credentials.service_account_email}")
        return client

    # ── Option 2: cached user OAuth2 token ───────────────────────────────────
    creds = _load_cached_token()
    if creds and not creds.expired:
        client = bigquery.Client(project=project, credentials=creds, location=location)
        print(f"[bq_connector] cached user token → project={project}")
        return client

    # ── No credentials found — raise with clear instructions ─────────────────
    sa_expected = sa_path or "config/bq_sa_key.json"
    raise RuntimeError(
        "\n\nNo BigQuery credentials found. Choose one of:\n"
        "\n"
        "  OPTION A — Service Account key (recommended for prod)\n"
        f"    1. https://console.cloud.google.com/iam-admin/serviceaccounts?project={project}\n"
        "    2. Create a service account → add roles: BigQuery Data Editor + BigQuery Job User\n"
        "    3. Keys tab → Add Key → JSON → download\n"
        f"    4. Save the file as:  {sa_expected}\n"
        "\n"
        "  OPTION B — Browser login (easiest for local dev, no gcloud needed)\n"
        "    1. https://console.cloud.google.com/apis/credentials?project={project}\n"
        "    2. Create OAuth 2.0 Client ID (Desktop app) → download JSON\n"
        "    3. Save as:  config/bq_oauth_client.json\n"
        "    4. In the notebook run:  bq = login_browser()\n"
    )


def get_bq_config(config_path: str = None) -> dict:
    """Returns the bq section of bq_config.yml."""
    return _load_bq_config(config_path)
