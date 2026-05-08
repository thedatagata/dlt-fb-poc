"""Pipeline 3: download FB creative assets (images, thumbnails, videos) to GCS.

Reads gs://<bucket>/ods/source=facebook/entity=ad_creatives/*.parquet
(Pipeline 2's Hive-partitioned output), extracts the image_url,
thumbnail_url, and video_id from each row's payload, fetches the
binaries from FB's CDN, and writes them to a Hive-style path:

    gs://<bucket>/assets/source=facebook/client=<X>/asset_kind=<Y>/<id>.<ext>

The asset_kind partition (image | thumbnail | video) plus the client
partition match the core app's expected access pattern: "show me this
client's videos" is a single prefix scan, not a metadata query.

Outputs an asset manifest parquet at:
    gs://<bucket>/manifest/source=facebook/date=<YYYY-MM-DD>/<run_ts>.parquet

The manifest is the source of truth for the Postgres pointer table — one
row per (creative_id, asset_kind) with the GCS URI, byte size, sha256,
content-type, and original FB URL. The Postgres loader (Pipeline 4)
upserts from this parquet via the DuckDB postgres extension.

Why standalone (not a dlt resource): dlt's filesystem destination is
optimized for tabular parquet writes, not arbitrary binary objects.
For 100s–1000s of asset files at well-known canonical paths, a direct
gcsfs write is simpler and gives us per-asset content-addressing
(sha256) + idempotent skip-on-existing-and-unchanged.

Run from repo root:
    uv run python pipelines/facebook_assets_to_gcs.py
"""
from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import dlt
import gcsfs
import httpx
import polars as pl
from dlt.sources.helpers.rest_client.redaction import sanitize_url
from dotenv import load_dotenv

_log = logging.getLogger("facebook_assets_to_gcs")
_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "dlt_lakehouse_poc")

# Source: unified ad_creatives parquet from Pipeline 2 (Hive layout).
_CREATIVES_GLOB = f"gs://{_BUCKET_NAME}/ods/source=facebook/entity=ad_creatives/*.parquet"

# Output prefixes (Hive-style key=value segments).
_ASSET_HIVE_PREFIX = "assets/source=facebook"  # + /client=<X>/asset_kind=<Y>/<id>.<ext>
_MANIFEST_HIVE_PREFIX = "manifest/source=facebook"  # + /date=<YYYY-MM-DD>/<run_ts>.parquet

# FB Graph API for resolving video_id → downloadable source URL.
_FB_API_VERSION = "v25.0"
_FB_GRAPH_BASE = f"https://graph.facebook.com/{_FB_API_VERSION}"

# Concurrency for binary downloads. 8 workers is well under FB's per-token
# rate limit and gives us roughly bandwidth-bound throughput on residential
# uplinks. Bump to 16-32 in a deploy environment with more bandwidth.
_DOWNLOAD_CONCURRENCY = 8

# Bounded download retry for transient CDN flakes. Smaller budget than the
# Marketing API path because CDN URLs are time-limited — if a download fails
# repeatedly, the URL probably expired and we want to fail fast and re-fetch
# the row's URL on the next ad_creatives refresh.
_DOWNLOAD_MAX_ATTEMPTS = 3
_DOWNLOAD_BACKOFF_S = 2.0
_DOWNLOAD_TIMEOUT_S = 60.0


_ASSET_KIND_IMAGE = "image"
_ASSET_KIND_THUMBNAIL = "thumbnail"
_ASSET_KIND_VIDEO = "video"


@dataclass(frozen=True)
class AssetSpec:
    """One unit of work for the downloader pool — a target asset to fetch."""

    creative_id: str
    client_uid: str
    org_uid: str
    asset_kind: str  # image | thumbnail | video
    source_url: str
    # Only set for video kind — the raw video_id from the creative; the
    # source URL above is the resolved Graph /{video_id}?fields=source URL.
    video_id: str | None = None


@dataclass(frozen=True)
class AssetResult:
    """One manifest row — what landed in GCS for a given (creative_id, kind)."""

    creative_id: str
    client_uid: str
    org_uid: str
    asset_kind: str
    gcs_uri: str
    original_url: str
    content_type: str
    byte_size: int
    sha256: str
    video_id: str | None
    downloaded_at: datetime
    skipped_existing: bool


def discover_asset_specs(creatives: pl.DataFrame, access_token: str | None) -> list[AssetSpec]:
    """Walk ad_creatives rows → AssetSpecs to download.

    The unified ad_creatives table stores the casted columns inside a JSON
    `payload` column (matches the dbt ods_mm__facebook_ad_creatives shape).
    Parse it once, emit one AssetSpec per non-null URL.

    Videos require an extra Graph call per video_id to get a downloadable
    source URL (FB doesn't include the bytes URL in ad_creatives directly).
    We resolve them in a small synchronous batch in main(); this function
    just records the video_id, leaving source_url empty for now.
    """
    specs: list[AssetSpec] = []
    for row in creatives.iter_rows(named=True):
        payload_raw = row["payload"]
        if not payload_raw:
            continue
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        creative_id = payload.get("creative_id") or row["source_id"]
        client_uid = row["client_uid"]
        org_uid = row["org_uid"]
        if image_url := payload.get("image_url"):
            specs.append(AssetSpec(
                creative_id=creative_id,
                client_uid=client_uid,
                org_uid=org_uid,
                asset_kind=_ASSET_KIND_IMAGE,
                source_url=image_url,
            ))
        if thumb_url := payload.get("thumbnail_url"):
            specs.append(AssetSpec(
                creative_id=creative_id,
                client_uid=client_uid,
                org_uid=org_uid,
                asset_kind=_ASSET_KIND_THUMBNAIL,
                source_url=thumb_url,
            ))
        if video_id := payload.get("video_id"):
            # source_url filled in by resolve_video_sources; placeholder
            # keeps the dataclass frozen-immutable contract.
            specs.append(AssetSpec(
                creative_id=creative_id,
                client_uid=client_uid,
                org_uid=org_uid,
                asset_kind=_ASSET_KIND_VIDEO,
                source_url="",
                video_id=video_id,
            ))
    return specs


def resolve_video_sources(
    specs: list[AssetSpec],
    access_token: str,
    client: httpx.Client,
) -> list[AssetSpec]:
    """For video specs, GET /{video_id}?fields=source to fill source_url.

    Done sequentially with the same retry budget as downloads — these are
    cheap Graph calls (one per unique video_id) and rarely error. If FB
    returns no `source` field (private video, deleted, etc.) we drop that
    spec and log at INFO so the operator knows it was excluded.
    """
    resolved: list[AssetSpec] = []
    seen_video_ids: dict[str, str] = {}
    for spec in specs:
        if spec.asset_kind != _ASSET_KIND_VIDEO:
            resolved.append(spec)
            continue
        cached = seen_video_ids.get(spec.video_id or "")
        if cached:
            resolved.append(_with_source_url(spec, cached))
            continue
        url = _resolve_one_video(client, spec.video_id or "", access_token)
        if not url:
            _log.info(
                "video_id=%s creative_id=%s client=%s: no source URL "
                "(private/deleted/permission) — skipping",
                spec.video_id, spec.creative_id, spec.client_uid,
            )
            continue
        seen_video_ids[spec.video_id or ""] = url
        resolved.append(_with_source_url(spec, url))
    return resolved


def _with_source_url(spec: AssetSpec, url: str) -> AssetSpec:
    return AssetSpec(
        creative_id=spec.creative_id,
        client_uid=spec.client_uid,
        org_uid=spec.org_uid,
        asset_kind=spec.asset_kind,
        source_url=url,
        video_id=spec.video_id,
    )


def _resolve_one_video(
    client: httpx.Client,
    video_id: str,
    access_token: str,
) -> str | None:
    for attempt in range(_DOWNLOAD_MAX_ATTEMPTS):
        try:
            resp = client.get(
                f"{_FB_GRAPH_BASE}/{video_id}",
                params={"fields": "source", "access_token": access_token},
                timeout=_DOWNLOAD_TIMEOUT_S,
            )
            if resp.status_code == 200:
                return resp.json().get("source")
            if resp.status_code in (400, 404, 403):
                # Permanent — don't retry. FB returns 400 for missing/deleted
                # video_ids and 403 for unauthorized.
                _log.debug(
                    "video resolve %s -> %s: %s",
                    video_id, resp.status_code,
                    sanitize_url(str(resp.request.url)),
                )
                return None
        except httpx.HTTPError:
            pass
        time.sleep(_DOWNLOAD_BACKOFF_S * (attempt + 1))
    return None


def _gcs_object_key(spec: AssetSpec, content_type: str | None) -> str:
    """Canonical Hive-partitioned GCS path for an asset.

    Path layout:
        assets/source=facebook/client=<slug>/asset_kind=<image|thumb|video>/<id>.<ext>

    Hive segments use singular `asset_kind=` (not plural `kinds`) so
    BigLake / DuckDB can auto-infer partition values without an alias
    map. File extension comes from the response's content-type when
    available; we fall back to the URL's suffix, then to a generic '.bin'.
    """
    ext = mimetypes.guess_extension(content_type or "") or ""
    if not ext:
        url_path = urlparse(spec.source_url).path
        if "." in url_path.rsplit("/", 1)[-1]:
            ext = "." + url_path.rsplit(".", 1)[-1].split("?", 1)[0]
        else:
            ext = ".bin"
    if spec.asset_kind == _ASSET_KIND_VIDEO and spec.video_id:
        stem = spec.video_id
    else:
        stem = spec.creative_id
    return (
        f"{_ASSET_HIVE_PREFIX}/"
        f"client={spec.client_uid}/"
        f"asset_kind={spec.asset_kind}/"
        f"{stem}{ext}"
    )


def _download_one(
    spec: AssetSpec,
    fs: gcsfs.GCSFileSystem,
    client: httpx.Client,
) -> AssetResult | None:
    """Fetch one binary, write to GCS, return manifest row.

    Idempotency: if the GCS path already exists with a non-zero size, skip
    the download. We can't compare sha256 without re-downloading, so the
    skip is path-existence-only — fine for FB CDN where the binary identity
    is hash-stable per creative_id (FB's URLs include the underlying file
    hash). Re-downloading would be wasteful for our re-run case.
    """
    object_key = _gcs_object_key(spec, content_type=None)  # tentative — may revise after we see content-type
    gcs_path = f"{_BUCKET_NAME}/{object_key}"
    if fs.exists(gcs_path):
        info = fs.info(gcs_path)
        if info.get("size", 0) > 0:
            return AssetResult(
                creative_id=spec.creative_id,
                client_uid=spec.client_uid,
                org_uid=spec.org_uid,
                asset_kind=spec.asset_kind,
                gcs_uri=f"gs://{gcs_path}",
                original_url=spec.source_url,
                content_type=info.get("contentType") or "",
                byte_size=int(info["size"]),
                sha256="",  # not recomputed for skipped existing
                video_id=spec.video_id,
                downloaded_at=datetime.fromtimestamp(
                    info.get("updated", 0), tz=timezone.utc,
                ) if isinstance(info.get("updated"), (int, float)) else datetime.now(timezone.utc),
                skipped_existing=True,
            )

    for attempt in range(_DOWNLOAD_MAX_ATTEMPTS):
        try:
            resp = client.get(spec.source_url, timeout=_DOWNLOAD_TIMEOUT_S, follow_redirects=True)
            if resp.status_code == 200:
                content = resp.content
                content_type = resp.headers.get("content-type", "").split(";")[0].strip()
                # Re-derive object_key now that we know the actual content-type.
                final_key = _gcs_object_key(spec, content_type=content_type)
                final_path = f"{_BUCKET_NAME}/{final_key}"
                with fs.open(final_path, "wb") as f:
                    f.write(content)
                sha = hashlib.sha256(content).hexdigest()
                return AssetResult(
                    creative_id=spec.creative_id,
                    client_uid=spec.client_uid,
                    org_uid=spec.org_uid,
                    asset_kind=spec.asset_kind,
                    gcs_uri=f"gs://{final_path}",
                    original_url=spec.source_url,
                    content_type=content_type,
                    byte_size=len(content),
                    sha256=sha,
                    video_id=spec.video_id,
                    downloaded_at=datetime.now(timezone.utc),
                    skipped_existing=False,
                )
            if resp.status_code in (403, 404, 410):
                # URL expired or asset gone — no point retrying.
                _log.info(
                    "asset fetch %s creative=%s -> %d (likely expired URL)",
                    spec.asset_kind, spec.creative_id, resp.status_code,
                )
                return None
        except httpx.HTTPError as e:
            _log.debug("attempt %d failed for creative=%s: %s", attempt, spec.creative_id, e)
        time.sleep(_DOWNLOAD_BACKOFF_S * (attempt + 1))

    _log.warning(
        "gave up after %d attempts: creative_id=%s kind=%s",
        _DOWNLOAD_MAX_ATTEMPTS, spec.creative_id, spec.asset_kind,
    )
    return None


def write_manifest(results: list[AssetResult]) -> str:
    """Write the manifest parquet to GCS at a Hive-partitioned path.

    Path layout:
        manifest/source=facebook/date=<YYYY-MM-DD>/<run_ts>.parquet

    Run timestamp in the filename + date partition lets the Postgres
    loader target the latest manifest deterministically (or, if it
    wants the change-history, glob across dates). The Postgres upsert
    on (creative_id, asset_kind) makes repeated runs idempotent.
    """
    rows = [
        {
            **asdict(r),
            "downloaded_at": r.downloaded_at.replace(tzinfo=timezone.utc),
        }
        for r in results
    ]
    df = pl.DataFrame(rows)
    now = datetime.now(timezone.utc)
    run_date = now.strftime("%Y-%m-%d")
    run_ts = now.strftime("%Y%m%dT%H%M%SZ")
    out_path = (
        f"gs://{_BUCKET_NAME}/{_MANIFEST_HIVE_PREFIX}/"
        f"date={run_date}/{run_ts}.parquet"
    )
    df.write_parquet(out_path)
    return out_path


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    # httpx logs every request URL at INFO, including FB CDN signed query
    # params — both noisy and a tiny disclosure surface. Bump to WARNING
    # so only failures surface.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    load_dotenv()

    fs = gcsfs.GCSFileSystem(
        token=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    )

    creatives = pl.read_parquet(_CREATIVES_GLOB)
    _log.info("loaded %d ad_creatives rows from %s", creatives.height, _CREATIVES_GLOB)

    specs = discover_asset_specs(creatives, access_token=None)
    by_kind = {k: 0 for k in (_ASSET_KIND_IMAGE, _ASSET_KIND_THUMBNAIL, _ASSET_KIND_VIDEO)}
    for s in specs:
        by_kind[s.asset_kind] = by_kind.get(s.asset_kind, 0) + 1
    _log.info("discovered %d asset specs: %s", len(specs), by_kind)

    access_token = dlt.secrets.get("sources.facebook_marketing.access_token")
    if not access_token:
        print(
            "ERROR: sources.facebook_marketing.access_token is not set in "
            ".dlt/secrets.toml — required for video resolution.",
            file=sys.stderr,
        )
        return 1

    with httpx.Client(http2=False) as client:
        if any(s.asset_kind == _ASSET_KIND_VIDEO for s in specs):
            specs = resolve_video_sources(specs, access_token, client)
            _log.info("after video source resolution: %d specs", len(specs))

        results: list[AssetResult] = []
        with ThreadPoolExecutor(max_workers=_DOWNLOAD_CONCURRENCY) as pool:
            futures = {pool.submit(_download_one, s, fs, client): s for s in specs}
            for fut in as_completed(futures):
                spec = futures[fut]
                try:
                    res = fut.result()
                except Exception:
                    _log.exception(
                        "download crashed creative_id=%s kind=%s",
                        spec.creative_id, spec.asset_kind,
                    )
                    continue
                if res is not None:
                    results.append(res)

    skipped = sum(1 for r in results if r.skipped_existing)
    new = len(results) - skipped
    total_bytes = sum(r.byte_size for r in results)
    _log.info(
        "complete: %d assets manifested (%d new, %d existing-skipped), %.1f MB",
        len(results), new, skipped, total_bytes / (1024 * 1024),
    )

    if not results:
        _log.warning("no results — skipping manifest write")
        return 0

    manifest_uri = write_manifest(results)
    _log.info("manifest -> %s", manifest_uri)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
