"""Pipeline 2: per-client raw parquet → unified ODS parquet (lakehouse).

Reads gs://<bucket>/raw/source=facebook/client=*/entity=*/date=*/*.parquet
(Hive-partitioned by client/entity/extract date), unifies all per-client
tables for each object_type into a single dataset keyed by tenant_skey,
and writes to gs://<bucket>/ods/source=facebook/entity=<entity>/.

The ODS layer deliberately omits a `client=` partition — that's the whole
point of unification. tenant_skey is a regular column for SQL filtering;
adding a path partition would just multiply tiny parquet files at this
scale (we have ~150 ad rows total across 2 tenants, period).

Why this exists: the prod stack today rebuilds these unified tables in
BigQuery via dbt's `lz_facebook__*` views + the `facebook_staging_merge_into_master`
post-hook MERGE. That run is ~30 min per client per dbt invocation
because it pays BQ slot time for every cast and MERGE. Same logic in
pyarrow + polars over GCS parquet runs in seconds and produces a parquet
dataset that BigQuery (via external table) and DuckDB can both read.

Output schema mirrors the existing `ods_mm__facebook_*` tables for
drop-in compatibility:
    tenant_skey         org_uid + '__' + client_uid
    org_uid             agency / parent org slug
    client_uid          tenant slug (matches client_slug from poc_clients.yaml)
    source_id           per-entity natural key (composite for ads_insights)
    source_schema_hash  md5(source_id || extracted_at_micros) — change detector
    payload             struct of cast columns, JSON-encoded for BQ compat
    _record_created_at  extracted_at (= _dlt_load_id-derived for parents)
    _record_updated_at  extracted_at
    _loaded_at          this pipeline's run timestamp

Children (ads_insights__actions etc.) get the same tenant_skey/org_uid/
client_uid plus column casts; they don't need payload/source_id because
they're row-per-list-item join targets keyed by parent_dlt_id.

Run from repo root:
    uv run python pipelines/facebook_unify_ods.py
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dlt
import gcsfs
import polars as pl
import pyarrow as pa
from dotenv import load_dotenv

_log = logging.getLogger("facebook_unify_ods")

# Bucket layout — input is Pipeline 1's Hive-partitioned output:
#   gs://<bucket>/raw/source=facebook/client=*/entity=*/date=*/*.parquet
# Output mirrors the layout pattern (no client= partition by design):
#   gs://<bucket>/ods/source=facebook/entity=<entity>/<file>.parquet
_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "dlt_lakehouse_poc")
# Mirror the Pipeline 1 layout (dataset_name=raw + layout starting with
# source=facebook/...) for discovery; ODS output uses dataset_name=ods.
_RAW_HIVE_PREFIX = "raw/source=facebook"
_ODS_HIVE_PREFIX = "ods/source=facebook"  # informational only — see layout in main()

# Hash a stable epoch when extracted_at is null so the source_schema_hash
# is still deterministic — matches dbt's `COALESCE(extracted_at, TIMESTAMP('1970-01-01'))`.
_EXTRACTED_AT_FALLBACK_MICROS = 0


# --------------------------------------------------------------------------- #
# Column + key specs ported from
# data-platform/dbt/macros/staging_engines/facebook_column_specs.sql.
# Single source of truth in dbt today; this is the lakehouse-side mirror.
# Keep the field order aligned to the dbt spec — it determines the struct
# field order in the payload column, which affects parquet bytewise output.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ColSpec:
    source_col: str
    alias: str
    cast_to: str  # "STRING" | "INT64" | "FLOAT64" | "TIMESTAMP" | "DATE" | "JSON_STRING"


@dataclass(frozen=True)
class KeyConfig:
    """Natural-key columns (post-alias) used to build source_id and to dedupe.

    For single-column keys, source_id = str(value). For composite keys
    (ads_insights), source_id = '|'.join(str(v) for v in values).
    For child tables the natural key is `_dlt_id` and dedupe is a no-op
    (dlt re-derives the same _dlt_id deterministically per source row).
    """
    natural_key_aliases: tuple[str, ...]
    is_child: bool = False


PARENT_SPECS: dict[str, list[ColSpec]] = {
    "ads": [
        ColSpec("id", "ad_id", "STRING"),
        ColSpec("adset_id", "adset_id", "STRING"),
        ColSpec("campaign_id", "campaign_id", "STRING"),
        ColSpec("account_id", "account_id", "STRING"),
        ColSpec("name", "ad_name", "STRING"),
        ColSpec("status", "configured_status", "STRING"),
        ColSpec("effective_status", "effective_status", "STRING"),
        ColSpec("creative__id", "creative_id", "STRING"),
        ColSpec("created_time", "created_at", "TIMESTAMP"),
        ColSpec("updated_time", "updated_at", "TIMESTAMP"),
    ],
    "ad_sets": [
        ColSpec("id", "adset_id", "STRING"),
        ColSpec("campaign_id", "campaign_id", "STRING"),
        ColSpec("account_id", "account_id", "STRING"),
        ColSpec("name", "adset_name", "STRING"),
        ColSpec("effective_status", "effective_status", "STRING"),
        ColSpec("bid_strategy", "bid_strategy", "STRING"),
        ColSpec("daily_budget", "daily_budget", "FLOAT64"),
        ColSpec("lifetime_budget", "lifetime_budget", "FLOAT64"),
        ColSpec("targeting__age_min", "targeting_age_min", "INT64"),
        ColSpec("targeting__age_max", "targeting_age_max", "INT64"),
        ColSpec("start_time", "start_time", "TIMESTAMP"),
        ColSpec("end_time", "end_time", "TIMESTAMP"),
        ColSpec("created_time", "created_at", "TIMESTAMP"),
        ColSpec("updated_time", "updated_at", "TIMESTAMP"),
    ],
    "ad_creatives": [
        ColSpec("id", "creative_id", "STRING"),
        ColSpec("account_id", "account_id", "STRING"),
        ColSpec("name", "creative_name", "STRING"),
        ColSpec("title", "title", "STRING"),
        ColSpec("body", "body", "STRING"),
        ColSpec("link_url", "link_url", "STRING"),
        ColSpec("image_url", "image_url", "STRING"),
        ColSpec("thumbnail_url", "thumbnail_url", "STRING"),
        ColSpec("video_id", "video_id", "STRING"),
        ColSpec("call_to_action_type", "cta_type", "STRING"),
        ColSpec("object_type", "media_type", "STRING"),
        ColSpec("instagram_permalink_url", "instagram_permalink_url", "STRING"),
    ],
    "ad_creatives_asset_feed": [
        ColSpec("id", "creative_id", "STRING"),
        ColSpec("account_id", "account_id", "STRING"),
        ColSpec("asset_feed_spec", "asset_feed_spec_json", "JSON_STRING"),
    ],
    "campaigns": [
        ColSpec("id", "campaign_id", "STRING"),
        ColSpec("account_id", "account_id", "STRING"),
        ColSpec("name", "campaign_name", "STRING"),
        ColSpec("status", "configured_status", "STRING"),
        ColSpec("effective_status", "effective_status", "STRING"),
        ColSpec("objective", "objective", "STRING"),
        ColSpec("buying_type", "buying_type", "STRING"),
        ColSpec("bid_strategy", "bid_strategy", "STRING"),
        ColSpec("daily_budget", "daily_budget", "FLOAT64"),
        ColSpec("lifetime_budget", "lifetime_budget", "FLOAT64"),
        ColSpec("budget_remaining", "budget_remaining", "FLOAT64"),
        ColSpec("start_time", "start_time", "TIMESTAMP"),
        ColSpec("stop_time", "stop_time", "TIMESTAMP"),
        ColSpec("created_time", "created_at", "TIMESTAMP"),
        ColSpec("updated_time", "updated_at", "TIMESTAMP"),
    ],
    "ads_insights": [
        ColSpec("ad_id", "ad_id", "STRING"),
        ColSpec("ad_name", "ad_name", "STRING"),
        ColSpec("adset_id", "adset_id", "STRING"),
        ColSpec("adset_name", "adset_name", "STRING"),
        ColSpec("campaign_id", "campaign_id", "STRING"),
        ColSpec("campaign_name", "campaign_name", "STRING"),
        ColSpec("account_id", "account_id", "STRING"),
        ColSpec("objective", "objective", "STRING"),
        ColSpec("date_start", "date_start", "DATE"),
        ColSpec("date_stop", "date_stop", "DATE"),
        ColSpec("impressions", "impressions", "INT64"),
        ColSpec("reach", "reach", "INT64"),
        ColSpec("clicks", "clicks", "INT64"),
        ColSpec("unique_clicks", "unique_clicks", "INT64"),
        ColSpec("inline_link_clicks", "inline_link_clicks", "INT64"),
        ColSpec("spend", "spend", "FLOAT64"),
        ColSpec("cpc", "cpc", "FLOAT64"),
        ColSpec("cpm", "cpm", "FLOAT64"),
        ColSpec("ctr", "ctr", "FLOAT64"),
        ColSpec("frequency", "frequency", "FLOAT64"),
        ColSpec("quality_ranking", "quality_ranking", "STRING"),
        ColSpec("conversion_rate_ranking", "conversion_rate_ranking", "STRING"),
        ColSpec("engagement_rate_ranking", "engagement_rate_ranking", "STRING"),
        ColSpec("_dlt_id", "parent_dlt_id", "STRING"),
    ],
}

PARENT_KEYS: dict[str, KeyConfig] = {
    "ads": KeyConfig(("ad_id",)),
    "ad_sets": KeyConfig(("adset_id",)),
    "ad_creatives": KeyConfig(("creative_id",)),
    "ad_creatives_asset_feed": KeyConfig(("creative_id",)),
    "campaigns": KeyConfig(("campaign_id",)),
    "ads_insights": KeyConfig(("ad_id", "date_start")),
}

# Children share two specs by family — actions/action_values have full
# attribution-window columns, video_pNN_watched_actions don't.
_CHILD_SPEC_ACTIONS: list[ColSpec] = [
    ColSpec("_dlt_id", "child_dlt_id", "STRING"),
    ColSpec("_dlt_parent_id", "parent_dlt_id", "STRING"),
    ColSpec("_dlt_list_idx", "list_idx", "INT64"),
    ColSpec("action_type", "action_type", "STRING"),
    ColSpec("action_target_id", "action_target_id", "STRING"),
    ColSpec("action_destination", "action_destination", "STRING"),
    ColSpec("value", "value", "FLOAT64"),
    ColSpec("_1d_view", "value_1d_view", "FLOAT64"),
    ColSpec("_1d_click", "value_1d_click", "FLOAT64"),
    ColSpec("_7d_view", "value_7d_view", "FLOAT64"),
    ColSpec("_7d_click", "value_7d_click", "FLOAT64"),
]
_CHILD_SPEC_VIDEO: list[ColSpec] = [
    ColSpec("_dlt_id", "child_dlt_id", "STRING"),
    ColSpec("_dlt_parent_id", "parent_dlt_id", "STRING"),
    ColSpec("_dlt_list_idx", "list_idx", "INT64"),
    ColSpec("action_type", "action_type", "STRING"),
    ColSpec("action_target_id", "action_target_id", "STRING"),
    ColSpec("action_destination", "action_destination", "STRING"),
    ColSpec("value", "value", "FLOAT64"),
]
CHILD_SPECS: dict[str, list[ColSpec]] = {
    "ads_insights__actions": _CHILD_SPEC_ACTIONS,
    "ads_insights__action_values": _CHILD_SPEC_ACTIONS,
    "ads_insights__video_p25_watched_actions": _CHILD_SPEC_VIDEO,
    "ads_insights__video_p50_watched_actions": _CHILD_SPEC_VIDEO,
    "ads_insights__video_p75_watched_actions": _CHILD_SPEC_VIDEO,
    "ads_insights__video_p100_watched_actions": _CHILD_SPEC_VIDEO,
}


# --------------------------------------------------------------------------- #
# Discovery: walk the raw bucket, group parquet files by
# (org_slug, client_slug, object_type). Path-as-data — gives us tenant
# identity without re-tagging in Pipeline 1.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class TenantPartition:
    """One (org, client, object_type) grouping of raw parquet files."""

    org_slug: str
    client_slug: str
    object_type: str
    file_paths: tuple[str, ...]


# Hive segment regex: each path part is `key=value`, value is anything
# but `/`. We extract `client` and `entity`; `date` is captured but
# unused at this layer (the unification pre-aggregates across dates).
# org_slug isn't in the path today — clients may live under different
# orgs in the registry, but for the POC both live under `neurograph`,
# so we hardcode it via env override or a default.
_DEFAULT_ORG_SLUG = os.environ.get("DEFAULT_ORG_SLUG", "neurograph")
_HIVE_KV_RE = re.compile(r"^(?P<key>[^=]+)=(?P<value>.+)$")


def discover_partitions(fs: gcsfs.GCSFileSystem) -> list[TenantPartition]:
    """Walk Hive-partitioned raw paths, group by (client, entity).

    Path shape:
        <bucket>/raw/source=facebook/client=<X>/entity=<Y>/date=<Z>/*.parquet

    Returns one TenantPartition per (client, entity) — date partitions are
    rolled up because the unification's dedupe spans dates anyway. The
    entity may be a parent ("ads_insights") or child
    ("ads_insights__actions") since dlt writes children as separate tables
    at the same hierarchical level.
    """
    partitions: dict[tuple[str, str], list[str]] = {}
    raw_root = f"{_BUCKET_NAME}/{_RAW_HIVE_PREFIX}/"
    for path in fs.glob(f"{raw_root}**/*.parquet"):
        keys = _parse_hive_keys(path, root=raw_root)
        client_slug = keys.get("client")
        entity = keys.get("entity")
        if not client_slug or not entity:
            continue
        if entity not in PARENT_SPECS and entity not in CHILD_SPECS:
            # Non-business tables (`_dlt_pipeline_state` etc.) live at
            # entity=_dlt_* — skip them at the unification layer.
            continue
        partitions.setdefault((client_slug, entity), []).append(f"gs://{path}")

    return [
        TenantPartition(
            org_slug=_DEFAULT_ORG_SLUG,
            client_slug=client_slug,
            object_type=entity,
            file_paths=tuple(sorted(files)),
        )
        for (client_slug, entity), files in sorted(partitions.items())
    ]


def _parse_hive_keys(path: str, *, root: str) -> dict[str, str]:
    """Pull `key=value` segments out of a Hive-partitioned object path."""
    rel = path[len(root):] if path.startswith(root) else path
    out: dict[str, str] = {}
    for segment in rel.split("/"):
        m = _HIVE_KV_RE.match(segment)
        if m:
            out[m.group("key")] = m.group("value")
    return out


# --------------------------------------------------------------------------- #
# Per-partition transform: read raw parquet → cast columns → tag tenant.
# Polars is the workhorse here — its expression engine handles the
# missing-column-tolerant safe-select that the dbt macro builds at
# compile time, and it can serialize structs to JSON in one expression.
# --------------------------------------------------------------------------- #


def _read_partition(part: TenantPartition) -> pl.DataFrame:
    """Read a tenant's raw parquet files into a polars DataFrame.

    polars.read_parquet understands `gs://` URIs natively via the
    object_store backend (uses GOOGLE_APPLICATION_CREDENTIALS through
    the same env var the rest of the stack reads). Reading multiple
    files in one call gets us parallel reads + a single concatenated
    DataFrame for free.
    """
    return pl.read_parquet(list(part.file_paths))


def _cast_expr(spec: ColSpec, present_cols: set[str]) -> pl.Expr:
    """Polars expression that produces one output column per ColSpec.

    Mirrors the dbt `_present` branching: if the source column doesn't
    exist on this tenant's parquet, emit a typed null literal so the
    output schema is identical across tenants and the concat lines up.
    Otherwise apply the cast.
    """
    if spec.source_col not in present_cols:
        return pl.lit(None, dtype=_polars_dtype(spec.cast_to)).alias(spec.alias)

    src = pl.col(spec.source_col)
    if spec.cast_to == "STRING":
        # Match dbt's NULLIF(TRIM(CAST(... AS STRING)), '') — empty strings
        # collapse to null so downstream `IS NULL` checks work.
        cleaned = src.cast(pl.Utf8).str.strip_chars()
        return pl.when(cleaned == "").then(None).otherwise(cleaned).alias(spec.alias)
    if spec.cast_to == "INT64":
        return src.cast(pl.Int64, strict=False).alias(spec.alias)
    if spec.cast_to == "FLOAT64":
        return src.cast(pl.Float64, strict=False).alias(spec.alias)
    if spec.cast_to == "DATE":
        return src.cast(pl.Date, strict=False).alias(spec.alias)
    if spec.cast_to == "TIMESTAMP":
        # Raw parquet stores some columns as already-typed timestamps and
        # others as strings (FB API quirk). Cast strict=False so both shapes
        # converge.
        return src.cast(pl.Datetime("us", time_zone="UTC"), strict=False).alias(spec.alias)
    if spec.cast_to == "JSON_STRING":
        # Source column may be a struct (already-parsed by dlt) OR a JSON
        # string — re-encode either way so downstream gets a canonical
        # JSON string with no Python-repr leakage.
        return _to_json_string_expr(src).alias(spec.alias)
    raise ValueError(f"unknown cast_to: {spec.cast_to}")


def _to_json_string_expr(expr: pl.Expr) -> pl.Expr:
    """Best-effort JSON encode of a column whose source type may be struct or string."""
    return (
        pl.when(expr.is_null())
        .then(None)
        .otherwise(expr.cast(pl.Utf8, strict=False))
    )


def _polars_dtype(cast_to: str) -> pl.DataType:
    return {
        "STRING": pl.Utf8,
        "INT64": pl.Int64,
        "FLOAT64": pl.Float64,
        "DATE": pl.Date,
        "TIMESTAMP": pl.Datetime("us", time_zone="UTC"),
        "JSON_STRING": pl.Utf8,
    }[cast_to]


def _extracted_at_expr(present_cols: set[str]) -> pl.Expr:
    """Mirror generate_facebook_landing_zone's _loaded_at_expr.

    Parents have `_dlt_load_id` (a stringified unix-epoch float).
    Children don't — fall back to current run time. The dlt
    documentation already calls this out: child rows are atomic with
    their parent so a single-run dedup is a no-op.
    """
    if "_dlt_load_id" in present_cols:
        # _dlt_load_id is e.g. "1778269109.0876858" — float seconds since epoch.
        return (
            pl.col("_dlt_load_id").cast(pl.Float64, strict=False) * 1_000_000
        ).cast(pl.Int64, strict=False).cast(
            pl.Datetime("us", time_zone="UTC")
        ).alias("extracted_at")
    return pl.lit(datetime.now(timezone.utc).replace(microsecond=0)).alias("extracted_at")


# --------------------------------------------------------------------------- #
# Unification: parents (with payload + dedupe) and children (cast + tag only).
# --------------------------------------------------------------------------- #


def unify_parent(
    object_type: str,
    parts: list[TenantPartition],
    loaded_at: datetime,
) -> pa.Table | None:
    """Concat all tenants' parquet for one object_type → unified pyarrow Table."""
    if not parts:
        return None
    spec = PARENT_SPECS[object_type]
    key = PARENT_KEYS[object_type]
    spec_alias_order = [c.alias for c in spec]

    per_tenant: list[pl.DataFrame] = []
    for part in parts:
        raw = _read_partition(part)
        present = set(raw.columns)
        # extracted_at comes from raw's _dlt_load_id (string-encoded epoch
        # float), so derive it before the spec-based select drops it.
        with_extracted = raw.with_columns(_extracted_at_expr(present))
        casted = with_extracted.select([
            *[_cast_expr(c, present) for c in spec],
            pl.col("extracted_at"),
        ])
        with_tenant = casted.with_columns(
            pl.lit(part.org_slug).alias("org_uid"),
            pl.lit(part.client_slug).alias("client_uid"),
            pl.lit(f"{part.org_slug}__{part.client_slug}").alias("tenant_skey"),
        )
        per_tenant.append(with_tenant)

    df = pl.concat(per_tenant, how="diagonal_relaxed")

    # Dedupe per (tenant_skey, natural_key) keeping the latest extracted_at.
    # Mirrors generate_facebook_landing_zone's ROW_NUMBER() OVER (PARTITION BY
    # … ORDER BY _loaded_at_expr DESC) WHERE _rn = 1.
    natural_key_cols = [c for c in key.natural_key_aliases]
    df = (
        df.sort(["tenant_skey", *natural_key_cols, "extracted_at"], descending=[False] * (len(natural_key_cols) + 1) + [True])
        .unique(subset=["tenant_skey", *natural_key_cols], keep="first", maintain_order=True)
    )

    # Build source_id from the natural key (composite → '|'-joined).
    if len(natural_key_cols) == 1:
        source_id_expr = pl.col(natural_key_cols[0]).cast(pl.Utf8)
    else:
        parts_exprs = [pl.col(c).cast(pl.Utf8) for c in natural_key_cols]
        source_id_expr = pl.concat_str(parts_exprs, separator="|")
    df = df.with_columns(source_id_expr.alias("source_id"))

    # Build the JSON payload. Polars' struct.json_encode collapses all
    # cast columns into one canonical JSON string per row, in the spec
    # field order — same shape the dbt TO_JSON(STRUCT(...)) produces in BQ.
    df = df.with_columns(
        pl.struct(spec_alias_order).struct.json_encode().alias("payload")
    )

    # source_schema_hash = MD5 hex of source_id || '|' || extracted_at_micros.
    # Drives change detection in any downstream that compares hashes — even
    # if the lakehouse external table doesn't run the dbt MERGE anymore,
    # keeping the column makes the parquet a true superset of the dbt output.
    df = df.with_columns(_source_schema_hash_expr().alias("source_schema_hash"))

    # Final column order matches the dbt master MERGE target.
    df = df.select([
        "tenant_skey", "org_uid", "client_uid",
        "source_id", "source_schema_hash", "payload",
        pl.col("extracted_at").alias("_record_created_at"),
        pl.col("extracted_at").alias("_record_updated_at"),
        pl.lit(loaded_at).alias("_loaded_at"),
    ])
    return df.to_arrow()


def unify_child(
    object_type: str,
    parts: list[TenantPartition],
    loaded_at: datetime,
) -> pa.Table | None:
    """Concat all tenants' parquet for one child object_type.

    Children are row-per-list-item join targets. They keep their
    parent_dlt_id FK back to the unified ads_insights, plus
    list_idx and the casted item columns. No payload / source_id —
    they're not independently merge-keyed in the dbt master either
    (the master MERGE for children uses child_dlt_id directly).
    """
    if not parts:
        return None
    spec = CHILD_SPECS[object_type]

    per_tenant: list[pl.DataFrame] = []
    for part in parts:
        raw = _read_partition(part)
        present = set(raw.columns)
        casted = raw.select([_cast_expr(c, present) for c in spec])
        with_tenant = casted.with_columns(
            pl.lit(part.org_slug).alias("org_uid"),
            pl.lit(part.client_slug).alias("client_uid"),
            pl.lit(f"{part.org_slug}__{part.client_slug}").alias("tenant_skey"),
            pl.lit(loaded_at).alias("_loaded_at"),
        )
        per_tenant.append(with_tenant)

    df = pl.concat(per_tenant, how="diagonal_relaxed")
    return df.to_arrow()


def _source_schema_hash_expr() -> pl.Expr:
    """MD5 hex of `source_id|<extracted_at_micros>`.

    Polars doesn't ship MD5; map_elements is fine here — the entire
    insights table for a 7d POC is sub-100k rows, so per-row Python
    is microseconds-not-seconds. If this becomes a hot path we can
    swap to a vectorized MD5 (xxhash64 keeps the change-detector
    semantic and is built-in via `Expr.hash`, but the hex would
    differ from BigQuery's existing column).
    """
    micros = (
        pl.col("extracted_at")
        .dt.timestamp(time_unit="us")
        .fill_null(_EXTRACTED_AT_FALLBACK_MICROS)
    )
    material = pl.concat_str(
        [pl.col("source_id"), pl.lit("|"), micros.cast(pl.Utf8)],
        separator="",
    )
    return material.map_elements(
        lambda s: hashlib.md5(s.encode("utf-8")).hexdigest(),
        return_dtype=pl.Utf8,
    )


# --------------------------------------------------------------------------- #
# dlt resources — one per object_type, each yielding a single pyarrow Table.
# dlt's filesystem destination handles parquet writes natively for Arrow
# tables; we get parallel writes for free.
# --------------------------------------------------------------------------- #


def _build_resources(
    partitions: list[TenantPartition],
    loaded_at: datetime,
):
    by_type: dict[str, list[TenantPartition]] = {}
    for part in partitions:
        by_type.setdefault(part.object_type, []).append(part)

    resources = []

    for object_type in PARENT_SPECS:
        parts = by_type.get(object_type, [])
        if not parts:
            _log.info("skipping parent=%s — no raw partitions", object_type)
            continue
        resources.append(_make_parent_resource(object_type, parts, loaded_at))

    for object_type in CHILD_SPECS:
        parts = by_type.get(object_type, [])
        if not parts:
            _log.info("skipping child=%s — no raw partitions", object_type)
            continue
        resources.append(_make_child_resource(object_type, parts, loaded_at))

    return resources


def _make_parent_resource(object_type, parts, loaded_at):
    @dlt.resource(
        name=f"facebook_{object_type}",
        table_name=object_type,
        write_disposition="replace",
    )
    def _resource() -> Iterator[pa.Table]:
        t0 = time.monotonic()
        table = unify_parent(object_type, parts, loaded_at)
        _log.info(
            "unified parent=%s tenants=%d rows=%d in %.2fs",
            object_type, len(parts), 0 if table is None else table.num_rows,
            time.monotonic() - t0,
        )
        if table is not None:
            yield table

    return _resource


def _make_child_resource(object_type, parts, loaded_at):
    @dlt.resource(
        name=f"facebook_{object_type}",
        table_name=object_type,
        write_disposition="replace",
    )
    def _resource() -> Iterator[pa.Table]:
        t0 = time.monotonic()
        table = unify_child(object_type, parts, loaded_at)
        _log.info(
            "unified child=%s tenants=%d rows=%d in %.2fs",
            object_type, len(parts), 0 if table is None else table.num_rows,
            time.monotonic() - t0,
        )
        if table is not None:
            yield table

    return _resource


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    load_dotenv()

    fs = gcsfs.GCSFileSystem(
        token=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    partitions = discover_partitions(fs)
    if not partitions:
        print(
            f"ERROR: no raw partitions found under "
            f"gs://{_BUCKET_NAME}/{_RAW_HIVE_PREFIX}/. Run Pipeline 1 first.",
            file=sys.stderr,
        )
        return 1
    _log.info(
        "discovered %d partitions across %d clients × %d object_types",
        len(partitions),
        len({(p.org_slug, p.client_slug) for p in partitions}),
        len({p.object_type for p in partitions}),
    )

    loaded_at = datetime.now(timezone.utc)
    resources = _build_resources(partitions, loaded_at)

    # Output Hive layout: dataset_name="ods" provides the top-level
    # prefix; the layout adds source=facebook/entity={table_name}/.
    # No `client=` partition — see module docstring. `{curr_date}` is
    # omitted at this layer (write_disposition=replace replaces the
    # entity wholesale per run; a date partition would just orphan
    # yesterday's file).
    layout = (
        "source=facebook/"
        "entity={table_name}/"
        "{load_id}.{file_id}.{ext}"
    )
    pipeline = dlt.pipeline(
        pipeline_name="facebook_unify_ods",
        destination=dlt.destinations.filesystem(layout=layout),
        dataset_name="ods",
        progress="log",
    )
    info = pipeline.run(resources, loader_file_format="parquet")
    print(info)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
