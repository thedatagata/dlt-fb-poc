
import logging
import os
import sys
import time

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, Tuple

# Third-party
import dlt
import requests
import yaml
from dlt.sources.helpers.rest_client.redaction import sanitize_url
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Local application
import _async_insights
from _rate_limit_telemetry import (
    RateLimitObserver,
    install_rate_limit_hook,
)


_log = logging.getLogger(__name__)


class MissingAdsInsightsTableError(RuntimeError):
    """Raised when scoped listings sync cannot find the upstream insights table.

    The scoped listings flow requires ads_insights to exist so it can pull the
    referenced campaign / adset / ad / creative IDs. If the table is missing,
    the upstream async fetch hasn't landed yet for this tenant — listings sync
    must fail loudly instead of silently completing with empty dimensions.
    """

# Per-request retry budget at the urllib3 Retry layer. Catches FB infra
# flakes (5xx + 429 + connection errors) before the dlt pagination loop
# sees them, so a single bad page does not restart the resource from
# page 1. backoff_factor=2.0 → sleeps grow 2s, 4s, 8s, 16s, 32s …
# capped at backoff_max.
_FB_REQUEST_MAX_ATTEMPTS = 5
_FB_REQUEST_BACKOFF_FACTOR = 2.0
_FB_REQUEST_BACKOFF_MAX = 600

# Chunk size for ads_insights. 7d at level=ad with our action_breakdowns
# stays below the query-weight threshold that drives FB sync API 500s on
# the largest known accounts. Also matches the longest configured
# attribution window (7d_click), so the most-recent chunk re-fetches
# every in-flight late-attribution row on every run.
_INSIGHTS_CHUNK_DAYS = 7

# Multi-ID Graph fetches send up to this many IDs per request via `?ids=`.
# 50 is FB's documented ceiling for multi-ID GETs on Marketing-API node
# types — going higher returns 400 Invalid IDs.
_GRAPH_IDS_BATCH_SIZE = 50

# Steady-state daily envelope. 7 days = the longest configured
# attribution window (7d_click). Insights for a day older than 7d
# cannot change, so a daily run only needs to cover the last 7.
_INSIGHTS_STEADY_STATE_DAYS = 7

# Initial / first-run envelope by environment. Used when a tenant has
# no prior successful run in dlt_facebook_ingest_runs (Layer 2). 1125 ≈
# FB Marketing API's stated insights retention (~37 months) — the
# practical "maximum" for sync-API loads.
_INSIGHTS_INITIAL_LOAD_DAYS_BY_ENV = {
    "dev": 7,
    "staging": 365,
    "prod": 1125,
}

# Past this gap, a "catch-up" looks too much like a first-run; collapse
# to first-run logic. Stops a multi-month outage from producing a
# partial-history window that misses the in-between data.
_INSIGHTS_CATCHUP_THRESHOLD_DAYS = 90

# Default backfill target depth for prod CLTV. The resolver widens the
# window toward this floor as long as coverage_min is shallower. Kept
# as a constant rather than per-tenant config until we observe a need.
_INSIGHTS_TARGET_MIN_DEPTH_DAYS = 730

ADS_FIELDS: List[str] = [
    "id",
    "adset_id",
    "campaign_id",
    "account_id",
    "name",
    "status",
    "effective_status",
    "creative",
    "tracking_specs",
    "created_time",
    "updated_time",
]

AD_SETS_FIELDS: List[str] = [
    "id",
    "campaign_id",
    "account_id",
    "name",
    "effective_status",
    "bid_strategy",
    "daily_budget",
    "lifetime_budget",
    "targeting",
    "start_time",
    "end_time",
    "created_time",
    "updated_time",
]

AD_CREATIVES_CORE_FIELDS: List[str] = [
    "id",
    "account_id",
    "name",
    "status",
    "object_story_spec",
    "degrees_of_freedom_spec",
    "image_hash",
    "image_url",
    "video_id",
    "thumbnail_url",
    "template_url",
    "product_set_id",
    "instagram_user_id",
    "effective_object_story_id",
    "object_type",
    "call_to_action_type",
    "url_tags",
]

AD_CREATIVES_ASSET_FEED_FIELDS: List[str] = [
    "id",
    "account_id",
    "asset_feed_spec",
]

CAMPAIGNS_FIELDS: List[str] = [
    "id",
    "account_id",
    "name",
    "status",
    "effective_status",
    "objective",
    "buying_type",
    "bid_strategy",
    "daily_budget",
    "lifetime_budget",
    "budget_remaining",
    "start_time",
    "stop_time",
    "created_time",
    "updated_time",
]

ADS_INSIGHTS_FIELDS: List[str] = [
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "campaign_id",
    "campaign_name",
    "account_id",
    "objective",
    "date_start",
    "date_stop",
    "impressions",
    "reach",
    "clicks",
    "unique_clicks",
    "inline_link_clicks",
    "spend",
    "cpc",
    "cpm",
    "ctr",
    "frequency",
    "quality_ranking",
    "conversion_rate_ranking",
    "engagement_rate_ranking",
    "actions",
    "action_values",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
]


@dataclass(frozen=True)
class FacebookListingIdScope:
    """IDs observed in recent ad-level insights for a listing sync run."""

    ad_ids: tuple[str, ...] = ()
    adset_ids: tuple[str, ...] = ()
    campaign_ids: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return (
            len(self.ad_ids) == 0
            and len(self.adset_ids) == 0
            and len(self.campaign_ids) == 0
        )


def resolve_insights_history_days(
    *,
    today: date,
    coverage_min: Optional[date],
    coverage_max: Optional[date],
    target_min: date,
    initial_load_days: int,
    steady_state_days: int = _INSIGHTS_STEADY_STATE_DAYS,
    catchup_threshold_days: int = _INSIGHTS_CATCHUP_THRESHOLD_DAYS,
) -> int:
    """Pick the next run's history-days window from durable coverage state.

    Pure function — no env lookups, no I/O, no clock. Caller (the
    facebook_ingest asset) is responsible for sourcing today,
    coverage_min/max (from dlt_facebook_ingest_runs), target_min (the
    backfill floor for CLTV), and initial_load_days (from environment).

    Decision tree:
      1. No prior coverage (first run for this tenant) → initial_load_days.
      2. coverage_min is still shallower than target_min → widen toward
         target_min, capped at initial_load_days. Ratchets the backfill
         deeper on each run until CLTV depth is reached.
      3. coverage_max is stale by more than catchup_threshold_days →
         fall through to initial-load. A multi-month outage's catch-up
         cost is roughly the same as a fresh load and avoids the risk
         of a partial-history window leaving a gap.
      4. coverage_max older than steady_state_days but within threshold
         → narrow catch-up: gap_days + steady_state_days buffer.
      5. Caught up → steady_state_days.

    The FB_INSIGHTS_HISTORY_DAYS env override does NOT live here — it's
    handled at the call site (so this function stays pure / testable).
    """
    # 1. First run.
    if coverage_min is None or coverage_max is None:
        return initial_load_days

    # 2. Still backfilling toward target depth.
    if coverage_min > target_min:
        gap_to_target = (today - target_min).days
        return min(gap_to_target, initial_load_days)

    gap_days = (today - coverage_max).days

    # 3. Stale → first-run shape.
    if gap_days > catchup_threshold_days:
        return initial_load_days

    # 4. Narrow catch-up.
    if gap_days > steady_state_days:
        return gap_days + steady_state_days

    # 5. Steady state.
    return steady_state_days


def resolve_initial_load_days_for_env(env: Optional[str] = None) -> int:
    """Resolve the initial-load window size for the given ENVIRONMENT tier.

    `dev=7` (fast iteration), `staging=365` (CLTV validation), `prod=1125`
    (full FB insights retention). Unknown envs fall through to 1125 — a
    misconfigured env should over-load rather than silently truncate
    history.
    """
    e = (env or os.environ.get("ENVIRONMENT", "dev")).strip().lower()
    return _INSIGHTS_INITIAL_LOAD_DAYS_BY_ENV.get(e, 1125)


def parse_history_days_env_override() -> Optional[int]:
    """Recovery escape hatch: FB_INSIGHTS_HISTORY_DAYS env var.

    When set to a positive int, the asset short-circuits the state-driven
    resolver and uses this value verbatim. Documented as the recovery
    path (replays, hand-driven backfills, debugging), not the routine
    code path. Returns None when unset / invalid / non-positive.
    """
    raw = os.environ.get("FB_INSIGHTS_HISTORY_DAYS")
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return n if n > 0 else None


def compute_insights_chunks(
    history_days: int,
    *,
    today: date,
    chunk_days: int = _INSIGHTS_CHUNK_DAYS,
) -> List[Tuple[str, str]]:
    """Walk backward from today in chunk_days windows. Most-recent first.

    Most-recent-first ordering means attribution-window-relevant data
    lands before any historical chunk has a chance to exhaust the retry
    budget. Each chunk is at most chunk_days long (inclusive on both
    ends); chunks are non-overlapping and contiguous; the oldest chunk
    may be truncated when history_days isn't a multiple of chunk_days.
    """
    chunks: List[Tuple[str, str]] = []
    remaining = history_days
    end = today
    while remaining > 0:
        days = min(chunk_days, remaining)
        start = end - timedelta(days=days - 1)
        chunks.append((start.isoformat(), end.isoformat()))
        end = start - timedelta(days=1)
        remaining -= days
    return chunks


def _build_retrying_session(
    observer: RateLimitObserver | None = None,
) -> requests.Session:
    """requests.Session with urllib3 Retry on 5xx + 429 + connection errors.

    Mounted on https:// and http://. Retries are transparent — the dlt
    pagination loop never sees a transient 500, so listing resources don't
    restart from page 1 for residual FB infra flake.
    """
    retry = Retry(
        total=_FB_REQUEST_MAX_ATTEMPTS,
        connect=_FB_REQUEST_MAX_ATTEMPTS,
        read=_FB_REQUEST_MAX_ATTEMPTS,
        backoff_factor=_FB_REQUEST_BACKOFF_FACTOR,
        backoff_max=_FB_REQUEST_BACKOFF_MAX,
        status_forcelist=frozenset({429, 500, 502, 503, 504}),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    install_rate_limit_hook(
        session,
        log=logging.getLogger(__name__),
        observer=observer,
    )
    return session


@dlt.source(name="facebook_marketing")
def facebook_marketing_source(
    ad_account_id: str,
    access_token: str,
    client_slug: str,
    *,
    insights_history_days: int,
    listing_id_scope: FacebookListingIdScope,
    observer: RateLimitObserver | None = None,
    today: Optional[date] = None,
    api_version: str = "v25.0",
) -> Any:
    """FB Marketing API listings source — one client, one run.

    Yields listing resources (campaigns, ad_sets, ads, ad_creatives,
    ad_creatives_asset_feed) scoped to the IDs referenced by the caller's
    recent insights window. The caller is responsible for resolving the
    scope from BigQuery. Insights themselves are loaded through the async
    submit / poll / fetch path in assets/facebook_ingest.

    `insights_history_days` is recorded against the run row for parity
    with the insights submit path; it does not change which IDs we fetch.

    Args:
        ad_account_id: Numeric ad account id (no act_ prefix; we add it).
        access_token: Long-lived per-client token. Passed in-process only.
        client_slug: Tenant prefix for output table names.
        insights_history_days: Days of ads_insights history to load.
            Required; positive int.
        listing_id_scope: Required scope of ad / ad_set / campaign IDs.
            Empty scope is allowed and produces zero-row resources.
        today: Anchor date for the history window. None = current UTC date.
            Pass explicitly for deterministic tests / replays.
        api_version: Graph API version. Default v25.0.
    """
    if today is None:
        today = datetime.now(timezone.utc).date()

    return _build_scoped_listing_resources(
        api_version=api_version,
        access_token=access_token,
        client_slug=client_slug,
        observer=observer,
        listing_id_scope=listing_id_scope,
    )


def _build_scoped_listing_resources(
    *,
    api_version: str,
    access_token: str,
    client_slug: str,
    observer: RateLimitObserver | None,
    listing_id_scope: FacebookListingIdScope,
) -> list[Any]:
    """Build dlt resources constrained to IDs seen in recent insights.

    Ads are fetched eagerly so we can derive `creative_ids` once before any
    resource yields. Computing the ads payload + creative ID set up front
    keeps every resource closure read-only — no shared mutable state, no
    dependency on resource execution order, no lock needed if dlt later
    parallelizes resources.
    """
    session = _build_retrying_session(observer=observer)

    ads_cache: list[dict[str, Any]] = list(
        _fetch_graph_objects_by_id(
            session=session,
            api_version=api_version,
            access_token=access_token,
            object_ids=listing_id_scope.ad_ids,
            fields=ADS_FIELDS,
        )
    )
    creative_ids: tuple[str, ...] = tuple(
        sorted(
            {
                ad["creative"]["id"]
                for ad in ads_cache
                if isinstance(ad.get("creative"), dict)
                and isinstance(ad["creative"].get("id"), str)
                and ad["creative"]["id"]
            }
        )
    )

    # Resource table names are now bare entity names (no client prefix).
    # Per-tenant namespacing comes from the GCS Hive partition `client={slug}`
    # in the filesystem layout — see build_pipeline(). Stripping the prefix
    # here keeps `entity={table_name}` in the path expanding cleanly to
    # entity=campaigns / entity=ads_insights / etc.
    @dlt.resource(
        name="campaigns",
        table_name="campaigns",
        primary_key="id",
        write_disposition="merge",
    )
    def campaigns():
        yield from _fetch_graph_objects_by_id(
            session=session,
            api_version=api_version,
            access_token=access_token,
            object_ids=listing_id_scope.campaign_ids,
            fields=CAMPAIGNS_FIELDS,
        )

    @dlt.resource(
        name="ad_sets",
        table_name="ad_sets",
        primary_key="id",
        write_disposition="merge",
    )
    def ad_sets():
        yield from _fetch_graph_objects_by_id(
            session=session,
            api_version=api_version,
            access_token=access_token,
            object_ids=listing_id_scope.adset_ids,
            fields=AD_SETS_FIELDS,
        )

    @dlt.resource(
        name="ads",
        table_name="ads",
        primary_key="id",
        write_disposition="merge",
    )
    def ads():
        yield from ads_cache

    @dlt.resource(
        name="ad_creatives",
        table_name="ad_creatives",
        primary_key="id",
        write_disposition="merge",
        max_table_nesting=2,
    )
    def ad_creatives():
        yield from _fetch_graph_objects_by_id(
            session=session,
            api_version=api_version,
            access_token=access_token,
            object_ids=creative_ids,
            fields=AD_CREATIVES_CORE_FIELDS,
        )

    @dlt.resource(
        name="ad_creatives_asset_feed",
        table_name="ad_creatives_asset_feed",
        primary_key="id",
        write_disposition="merge",
        max_table_nesting=1,
    )
    def ad_creatives_asset_feed():
        yield from _fetch_graph_objects_by_id(
            session=session,
            api_version=api_version,
            access_token=access_token,
            object_ids=creative_ids,
            fields=AD_CREATIVES_ASSET_FEED_FIELDS,
        )

    return [ad_creatives, ad_creatives_asset_feed, campaigns, ad_sets, ads]


def _fetch_graph_objects_by_id(
    *,
    session: requests.Session,
    api_version: str,
    access_token: str,
    object_ids: Iterable[str],
    fields: list[str],
) -> Iterator[dict[str, Any]]:
    """Fetch Graph nodes via the multi-ID endpoint in bounded batches.

    HTTP error messages are sanitized so the access_token query param does
    not leak into Dagster logs / exception strings on failure. Per-object
    Graph errors (`{"id": {"error": ...}}`) are logged at debug — they
    most often mean the object was deleted between insights submit and the
    listings sync fetch, and silently dropping them is the documented FB
    multi-ID semantics; debug-level lets ops correlate drops without
    spamming WARN every time.
    """
    ids = sorted({object_id for object_id in object_ids if object_id})
    for batch in _chunks(ids, _GRAPH_IDS_BATCH_SIZE):
        response = session.get(
            f"https://graph.facebook.com/{api_version}/",
            params={
                "ids": ",".join(batch),
                "fields": ",".join(fields),
                "access_token": access_token,
            },
        )
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for url: "
                f"{sanitize_url(response.url)}",
                response=response,
            )
        payload = response.json()
        if not isinstance(payload, dict):
            continue
        for object_id, item in payload.items():
            if not isinstance(item, dict):
                continue
            if "error" in item:
                _log.debug(
                    "facebook multi-id fetch dropped id=%s error=%s",
                    object_id,
                    item.get("error"),
                )
                continue
            yield item


def _chunks(values: list[str], size: int) -> Iterator[list[str]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def build_pipeline(client_slug: str, org_slug: str) -> dlt.Pipeline:
    """One pipeline_name per client = per-tenant state isolation.

    Output uses Hive-style partitioning so BigQuery/BigLake and DuckDB
    can prune by partition keys without scanning every file:

        gs://<bucket>/raw/source=facebook/client={slug}/
                       entity={table_name}/date={curr_date}/
                       <load_id>.<file_id>.parquet

    Implementation note: dlt's filesystem `layout` is RELATIVE to
    `<bucket>/<dataset_name>/` — it can't write outside that prefix. So
    `dataset_name="raw"` gives us the top-level `raw/` directory, and
    the layout fills in the Hive segments below it. dlt's state tables
    (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) live at the
    dataset root *next to* `source=facebook/` — outside the BigLake
    `hive_partition_uri_prefix`, so they don't pollute external reads.

    `client_slug` is interpolated into the layout string at pipeline-build
    time because dlt only exposes a fixed set of layout variables
    ({table_name}, {load_id}, {file_id}, {ext}, {curr_date}, {schema_name}).
    Each call here creates a fresh destination instance with the slug
    baked in.

    Auth is GOOGLE_APPLICATION_CREDENTIALS via gcsfs; bucket URL from
    .dlt/config.toml.
    """
    layout = (
        "source=facebook/"
        f"client={client_slug}/"
        "entity={table_name}/"
        "date={curr_date}/"
        "{load_id}.{file_id}.{ext}"
    )
    return dlt.pipeline(
        pipeline_name=f"facebook_{client_slug}",
        destination=dlt.destinations.filesystem(layout=layout),
        dataset_name="raw",
        progress="log",
    )


# ---------------------------------------------------------------------------
# POC runner: insights submit/poll/fetch → derive scope → run insights and
# listing resources together against the GCS destination, one client at a
# time. In prod this orchestration lives in Dagster (assets/facebook_ingest);
# the runner here is a self-contained POC entry point.
# ---------------------------------------------------------------------------

# 7d covers the longest configured attribution window (7d_click). Big enough
# for the demo to show non-trivial volume; small enough that the FB async
# insights submit returns in well under a minute per ad account.
_POC_SAMPLE_DAYS = 7

# Bound the synchronous poll loop. Insights jobs of this shape (level=ad,
# 7d window) typically finish in 10-30s; the deadline is for outliers.
_POC_POLL_INTERVAL_S = 5
_POC_POLL_TIMEOUT_S = 600

# Default API version for the runner's submit/poll/fetch path. Kept aligned
# with facebook_marketing_source's default so the POC uses one version
# end-to-end.
_POC_API_VERSION = "v25.0"


@dataclass(frozen=True)
class POCClient:
    """One row of poc_clients.yaml — identity for a POC tenant."""

    client_slug: str
    org_slug: str
    ad_account_id: str


def fetch_insights_window(
    *,
    session: requests.Session,
    ad_account_id: str,
    access_token: str,
    since: str,
    until: str,
    api_version: str = _POC_API_VERSION,
    poll_interval_s: int = _POC_POLL_INTERVAL_S,
    poll_timeout_s: int = _POC_POLL_TIMEOUT_S,
) -> list[dict[str, Any]]:
    """Submit FB async insights, poll to completion, fetch all pages.

    Returns a flat list of insight rows for the [since, until] window
    at level=ad with the standard action breakdowns + attribution
    windows defined in `_async_insights`. The runner needs the rows
    in-memory before building resources so the listing scope can be
    derived from the same insight set the insights resource will yield.
    """
    run_id = _async_insights.submit_report_run(
        session,
        api_version=api_version,
        ad_account_id=ad_account_id,
        access_token=access_token,
        fields=ADS_INSIGHTS_FIELDS,
        since=since,
        until=until,
    )
    _log.info(
        "submitted insights report_run_id=%s window=%s..%s",
        run_id, since, until,
    )

    deadline = time.monotonic() + poll_timeout_s
    while True:
        status = _async_insights.poll_report_run(
            session,
            api_version=api_version,
            report_run_id=run_id,
            access_token=access_token,
        )
        _log.info(
            "poll run=%s status=%s pct=%s",
            run_id, status.async_status, status.async_percent_completion,
        )
        if _async_insights.is_terminal_success(status.async_status):
            break
        if _async_insights.is_terminal_failure(status.async_status):
            raise RuntimeError(
                f"insights run {run_id} failed: {status.async_status} "
                f"err={status.error_user_msg or status.error_user_title}"
            )
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"insights run {run_id} did not finish in {poll_timeout_s}s"
            )
        time.sleep(poll_interval_s)

    rows: list[dict[str, Any]] = []
    for page in _async_insights.fetch_report_run_results(
        session,
        api_version=api_version,
        report_run_id=run_id,
        access_token=access_token,
        fields=ADS_INSIGHTS_FIELDS,
    ):
        rows.extend(page)
    _log.info("fetched %d insight rows for run=%s", len(rows), run_id)
    return rows


def derive_scope_from_insights(
    insights: Iterable[dict[str, Any]],
) -> FacebookListingIdScope:
    """Set of ad / adset / campaign IDs referenced by recent insights.

    The listing source needs this scope so it only fetches Graph nodes
    that actually appeared in spend during the window — keeps the POC
    bounded even on accounts with thousands of historical objects.
    """
    ad_ids: set[str] = set()
    adset_ids: set[str] = set()
    campaign_ids: set[str] = set()
    for row in insights:
        if row.get("ad_id"):
            ad_ids.add(row["ad_id"])
        if row.get("adset_id"):
            adset_ids.add(row["adset_id"])
        if row.get("campaign_id"):
            campaign_ids.add(row["campaign_id"])
    return FacebookListingIdScope(
        ad_ids=tuple(sorted(ad_ids)),
        adset_ids=tuple(sorted(adset_ids)),
        campaign_ids=tuple(sorted(campaign_ids)),
    )


def build_ads_insights_resource(
    *,
    client_slug: str,
    rows: list[dict[str, Any]],
):
    """dlt resource yielding insight rows already fetched from FB.

    The fetch happens before resource construction (see run_for_client)
    so rows are captured by closure rather than re-fetched. POC uses
    write_disposition=replace because each run reloads the same 7d
    window; production keys on (ad_id, date_start) and merges.
    """

    @dlt.resource(
        name="ads_insights",
        table_name="ads_insights",
        primary_key=["ad_id", "date_start"],
        write_disposition="replace",
        max_table_nesting=2,
    )
    def ads_insights() -> Iterator[dict[str, Any]]:
        yield from rows

    return ads_insights


def run_for_client(
    client: POCClient,
    access_token: str,
    today: date,
) -> None:
    """Drive one client's POC ingest end-to-end into GCS."""
    until = today.isoformat()
    since = (today - timedelta(days=_POC_SAMPLE_DAYS - 1)).isoformat()
    observer = RateLimitObserver(log=_log)
    session = _build_retrying_session(observer=observer)

    insights = fetch_insights_window(
        session=session,
        ad_account_id=client.ad_account_id,
        access_token=access_token,
        since=since,
        until=until,
    )
    if not insights:
        _log.warning(
            "client=%s ad_account=%s returned 0 insight rows in %s..%s — "
            "skipping (no IDs to scope listings)",
            client.client_slug, client.ad_account_id, since, until,
        )
        return

    scope = derive_scope_from_insights(insights)
    _log.info(
        "client=%s scope: %d ads, %d ad_sets, %d campaigns",
        client.client_slug,
        len(scope.ad_ids),
        len(scope.adset_ids),
        len(scope.campaign_ids),
    )

    insights_resource = build_ads_insights_resource(
        client_slug=client.client_slug,
        rows=insights,
    )
    listing_source = facebook_marketing_source(
        ad_account_id=client.ad_account_id,
        access_token=access_token,
        client_slug=client.client_slug,
        insights_history_days=_POC_SAMPLE_DAYS,
        listing_id_scope=scope,
        observer=observer,
        today=today,
        api_version=_POC_API_VERSION,
    )

    pipeline = build_pipeline(client.client_slug, client.org_slug)
    info = pipeline.run(
        [insights_resource(), *listing_source.resources.values()],
        loader_file_format="parquet",
    )
    print(info)


def load_poc_clients(path: Optional[Path] = None) -> list[POCClient]:
    """Read poc_clients.yaml — POC client roster sits next to this file."""
    cfg_path = path or (Path(__file__).resolve().parent / "poc_clients.yaml")
    cfg = yaml.safe_load(cfg_path.read_text())
    return [POCClient(**entry) for entry in cfg["clients"]]


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    load_dotenv()

    access_token = dlt.secrets.get("sources.facebook_marketing.access_token")
    if not access_token:
        print(
            "ERROR: sources.facebook_marketing.access_token is not set in "
            ".dlt/secrets.toml",
            file=sys.stderr,
        )
        return 1

    today = datetime.now(timezone.utc).date()
    clients = load_poc_clients()
    _log.info("running for %d POC clients", len(clients))
    for client in clients:
        try:
            run_for_client(client, access_token, today)
        except Exception:
            # POC: continue on per-client failure so the other client's
            # data still lands. Prod uses per-asset retry in Dagster.
            _log.exception("client=%s failed", client.client_slug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
