"""Asset manifest read-side demo for the core app dev.

The handoff contract is one parquet dataset in GCS:

    gs://dlt_lakehouse_poc/manifest/source=facebook/date=<YYYY-MM-DD>/<run_ts>.parquet

This notebook documents:
  - WHERE the manifest lives (path layout above)
  - WHAT columns it carries (schema cell below)
  - WHEN it changes (Pipeline 3 rewrites it on every run with current
    gcs_uri / sha256 / downloaded_at — re-running keeps app pointers
    fresh, never stale)
  - HOW to query it from any DuckDB-attached service (no warehouse load,
    no copy)

The app team owns ingest from here on — read this manifest into your
Postgres pointer table on whatever cadence makes sense (sync job, event
trigger, etc.). Each (creative_id, asset_kind) is the natural upsert
key; gcs_uri is the pointer the rendering layer signs and serves.

Run:
    uv run marimo edit notebooks/lakehouse_demo.py
"""

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # FB asset manifest — interface contract for the app team

    - **Path:**
      `gs://dlt_lakehouse_poc/manifest/source=facebook/date=<YYYY-MM-DD>/<run_ts>.parquet`
    - **Updated:** each Pipeline 3 run rewrites the manifest with
      current `gcs_uri` / `sha256` / `downloaded_at`. The path is
      stable; only the `<run_ts>.parquet` filename rolls forward,
      so a glob over the prefix always lands on the latest.
    - **Natural key:** `(creative_id, asset_kind)`
    - **Pointer column:** `gcs_uri` — feeds signed-URL generation
      on the app side (or Cloud CDN if the bucket is public).
    """)
    return


@app.cell
def _():
    import os
    import duckdb
    import gcsfs
    from dotenv import load_dotenv

    load_dotenv()
    BUCKET = os.environ.get("GCS_BUCKET_NAME", "dlt_lakehouse_poc")

    # Same gcsfs the pipelines use → same SA auth path. DuckDB borrows
    # the registered filesystem to resolve gs:// URIs without a separate
    # CREATE SECRET / httpfs auth dance.
    fs = gcsfs.GCSFileSystem(token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    con = duckdb.connect()
    con.register_filesystem(fs)
    return BUCKET, con


@app.cell
def _(BUCKET, con):
    # Hive-partitioning hint surfaces `date` from the path as a column
    # even though the parquet body doesn't store it.
    MANIFEST_GLOB = f"gs://{BUCKET}/manifest/source=facebook/**/*.parquet"
    CREATIVES_GLOB = f"gs://{BUCKET}/ods/source=facebook/entity=ad_creatives/*.parquet"

    con.sql(f"""
        CREATE OR REPLACE VIEW manifest AS
        SELECT * FROM read_parquet('{MANIFEST_GLOB}', hive_partitioning=true);

        CREATE OR REPLACE VIEW ad_creatives AS
        SELECT * FROM read_parquet('{CREATIVES_GLOB}');
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Schema — the columns the app team consumes

    Same shape on every run. The app's PG pointer table should mirror
    these columns (or a subset). `gcs_uri` is the load-bearing column;
    everything else is metadata for rendering / cache invalidation.
    """)
    return


@app.cell
def _(con):
    schema = con.sql("DESCRIBE manifest").df()
    schema
    return


@app.cell
def _(mo):
    mo.md("""
    ## Counts — how much data is currently in the manifest
    """)
    return


@app.cell
def _(con):
    summary = con.sql("""
        SELECT
            COUNT(*)                                  AS n_asset_rows,
            COUNT(DISTINCT creative_id)               AS n_creatives,
            COUNT(DISTINCT client_uid)                AS n_clients,
            ROUND(SUM(byte_size) / 1024.0 / 1024.0,
                  1)                                  AS total_mb,
            MIN(downloaded_at)                        AS first_download,
            MAX(downloaded_at)                        AS last_download
        FROM manifest
    """).df()
    summary
    return


@app.cell
def _(mo):
    mo.md("""
    ## Breakdown by client × asset_kind
    """)
    return


@app.cell
def _(con):
    by_client_kind = con.sql("""
        SELECT
            client_uid,
            asset_kind,
            COUNT(*)                                  AS n_assets,
            ROUND(SUM(byte_size) / 1024.0 / 1024.0,
                  2)                                  AS mb,
            ROUND(AVG(byte_size) / 1024.0, 1)         AS avg_kb
        FROM manifest
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()
    by_client_kind
    return (by_client_kind,)


@app.cell
def _(by_client_kind, mo):
    import altair as alt

    chart = (
        alt.Chart(by_client_kind)
        .mark_bar()
        .encode(
            x=alt.X("client_uid:N", title="client"),
            y=alt.Y("n_assets:Q", title="asset count"),
            color=alt.Color("asset_kind:N", title="kind"),
            tooltip=["client_uid", "asset_kind", "n_assets", "mb"],
        )
        .properties(width=480, height=260, title="Assets per client by kind")
    )
    mo.ui.altair_chart(chart)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Sample pointer rows

    These are the rows the app's sync job consumes. `gcs_uri` is the
    canonical pointer; the rendering layer turns it into a signed
    URL (`storage.Blob.generate_signed_url(expires=1h)`) or serves
    it through Cloud CDN if the bucket is public.
    """)
    return


@app.cell
def _(con):
    sample = con.sql("""
        SELECT
            client_uid,
            asset_kind,
            creative_id,
            content_type,
            byte_size,
            sha256,
            downloaded_at,
            gcs_uri
        FROM manifest
        ORDER BY byte_size DESC
        LIMIT 10
    """).df()
    sample
    return


@app.cell
def _(mo):
    mo.md("""
    ## Joining manifest to `ad_creatives` ODS

    The same DuckDB session can join the manifest against the
    unified `ad_creatives` parquet to produce "creative + its media"
    payloads. Useful as a debugging view; the app's PG sync should
    normally just read the manifest and let the app join to its own
    creative tables.
    """)
    return


@app.cell
def _(con):
    creatives_with_assets = con.sql("""
        WITH parsed_creatives AS (
            SELECT
                client_uid,
                source_id                              AS creative_id,
                payload->>'creative_name'              AS creative_name,
                payload->>'media_type'                 AS media_type,
                payload->>'cta_type'                   AS cta_type
            FROM ad_creatives
        )
        SELECT
            c.client_uid,
            c.creative_id,
            -- truncate long FB-generated names so the table stays readable
            SUBSTR(c.creative_name, 1, 60)             AS creative_name,
            c.media_type,
            c.cta_type,
            COUNT(m.asset_kind)                        AS n_assets,
            STRING_AGG(m.asset_kind, ', '
                       ORDER BY m.asset_kind)          AS kinds_present,
            SUM(m.byte_size)                           AS total_bytes
        FROM parsed_creatives c
        LEFT JOIN manifest m
            ON  m.client_uid  = c.client_uid
            AND m.creative_id = c.creative_id
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY n_assets DESC, total_bytes DESC
        LIMIT 15
    """).df()
    creatives_with_assets
    return


@app.cell
def _(mo):
    mo.md("""
    ## Freshness

    Re-running Pipeline 3 (`uv run python pipelines/facebook_assets_to_gcs.py`)
    rewrites the manifest at a new `<run_ts>.parquet` filename
    under the same `date=<YYYY-MM-DD>/` prefix. The glob above
    always picks up the latest because it scans the whole prefix —
    if you want only the most recent run, sort by filename
    descending and pick the head, or read the partition with the
    max `date=`.

    The app's pointer table stays fresh as long as the sync job
    re-reads this manifest on whatever cadence works for the team
    (every 15min, on-demand, event-driven). The manifest never
    carries stale URIs because it's regenerated end-to-end on each
    Pipeline 3 run.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
