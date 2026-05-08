"""GCS write smoke test for the lakehouse POC.

Loads two synthetic rows to gs://${GCS_BUCKET_NAME}/_smoke/ via dlt's
filesystem destination + parquet loader. The point is to fail fast on
auth / project / bucket-permission problems before we wire up the real
Facebook pipeline.

Run from the repo root:
    uv run python pipelines/_smoke_gcs.py

Cleans up its own table at the end so the bucket stays tidy.
"""
from __future__ import annotations

import os
import sys

import dlt
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()

    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    bucket = os.environ.get("GCS_BUCKET_NAME", "dlt_lakehouse_poc")
    if not sa_path or not os.path.isfile(sa_path):
        print(
            "ERROR: GOOGLE_APPLICATION_CREDENTIALS is unset or points at a "
            "non-existent file. Check .env.",
            file=sys.stderr,
        )
        return 1

    pipeline = dlt.pipeline(
        pipeline_name="lakehouse_smoke",
        destination=dlt.destinations.filesystem(bucket_url=f"gs://{bucket}"),
        dataset_name="_smoke",
        progress="log",
    )

    info = pipeline.run(
        [{"id": 1, "msg": "hello lakehouse"}, {"id": 2, "msg": "from dlt"}],
        table_name="ping",
        loader_file_format="parquet",
        write_disposition="replace",
    )
    print(info)
    print(f"OK: wrote to gs://{bucket}/_smoke/ping/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
