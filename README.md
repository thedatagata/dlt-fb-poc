# dlt-sandbox

A working sandbox that scaffolds a custom **Facebook Marketing API** ingestion pipeline using the [dlt](https://dlthub.com) AI Workbench, then derives a semantic-layer-ready ontology + Kimball CDM from the raw normalized output.

The point of this repo is to demonstrate, end to end:

1. **Agentic ingestion** — give Claude Code a target API and let the dlt toolkits (`/find-source`, `/create-rest-api-pipeline`, `/debug-pipeline`, `/validate-data`) build, test, and harden a custom REST connector.
2. **Auto-normalization** — show how dlt unfolds deeply nested Marketing API JSON (`object_story_spec`, `asset_feed_spec`, `actions`, `action_values`, `purchase_roas`) into a relational schema **with no hand-written transforms**.
3. **Ontology + CDM** — show how the `transformations` toolkit (`/annotate-sources`, `/create-ontology`, `/generate-cdm`, `/create-transformation`) turns that raw schema into an entity-graph ontology and a dimensionally-modeled canonical data model that's suitable input for a semantic layer (Cube/Bonnard) or BI tool.

The Facebook source pulled here goes well beyond what the verified `facebook_ads` source or Airbyte's connector expose by default — full creative payload (`asset_feed_spec` DCO variants, `object_story_spec` narrative, Advantage+ feature flags) plus ads_insights with non-default breakdowns and per-action attribution windows. The narrative is **"custom connector + dlt normalization unlocks data the turnkey connectors don't surface."**

---

## Prerequisites

| Requirement | Notes |
|---|---|
| macOS / Linux | Tested on macOS 24.6, Python 3.12 |
| `uv` | Modern Python package manager. Install: `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Claude Code CLI | The dlt AI workbench installs Claude Code skills/commands — you launch them from inside Claude Code |
| Facebook Marketing API access | A long-lived access token with `ads_read` scope and access to the target ad account. See [Credentials](#credentials) |
| dltHub Pro / Workspace license (optional) | Not required for the core REST pipeline. Some toolkits and the `dlthub-runtime` deploy path are license-gated. Set `DLT_LICENSE_KEY` env var or `license = "..."` in `.dlt/secrets.toml` if you have one |

---

## Setup

### 1. Clone and bootstrap the project

```bash
git clone <this-repo> dlt-sandbox
cd dlt-sandbox

uv venv
source .venv/bin/activate
uv sync          # installs dlt[duckdb,workspace], polars, pyarrow from pyproject.toml + uv.lock
```

This pulls in `dlt 1.25.0` and the workspace extras (DuckDB destination, marimo dashboard, FastMCP, pyarrow).

### 2. Activate dlt workspace mode

Workspace features (profiles, dashboard, MCP server, AI toolkits) are gated behind a marker file. It's already in this repo at `.dlt/.workspace` — no action needed for fresh clones. If you start from scratch:

```bash
mkdir -p .dlt && touch .dlt/.workspace
```

Verify:

```bash
uv run dlt workspace info
# Should print:
#   Workspace dir: /path/to/dlt-sandbox
#   Settings dir:  /path/to/dlt-sandbox/.dlt
#   Profile: dev
#   ...
```

### 3. (Optional) Install your dltHub license

The license is read at runtime from the `DLT_LICENSE_KEY` env var or `.dlt/secrets.toml`. There's no separate "log in" step — just set it and licensed features start working when invoked.

```bash
# Option A: env var (recommended for sandbox)
export DLT_LICENSE_KEY="your-access-code"
echo 'export DLT_LICENSE_KEY="your-access-code"' >> ~/.zshrc

# Option B: .dlt/secrets.toml
# license = "your-access-code"
```

> **Note:** dlt 1.25.0 does **not** ship a `dlt license` CLI subcommand. There's no dedicated "verify the license" command — the license is consumed at runtime when a licensed feature is invoked. If you don't have a license, the core REST pipeline + duckdb path works without one; only Pro/runtime-deploy features will fail.

### 4. Install the AI workbench for Claude Code

```bash
uv run dlt ai init
```

This drops `.claude/{skills,rules,commands}/` into the project, registers the `dlt-workspace-mcp` MCP server (see `.mcp.json`), and configures Claude Code to surface the slash commands. Agent is auto-detected from your environment (Claude Code → `claude`).

Confirm:

```bash
uv run dlt ai status
# Should show:
#   dlt 1.25.0
#   Agent: claude
#   Toolkits: init, rest-api-pipeline, data-exploration, transformations
```

If you're starting from scratch (no toolkits installed yet), add the three this sandbox uses:

```bash
uv run dlt ai toolkit install rest-api-pipeline    # /find-source, /create-rest-api-pipeline, /debug-pipeline, /validate-data, /view-data
uv run dlt ai toolkit install data-exploration     # /explore-data, /build-notebook
uv run dlt ai toolkit install transformations      # /annotate-sources, /create-ontology, /generate-cdm, /create-transformation
```

### 5. Credentials

The pipeline reads two values:

| Setting | Where it lives | What it is |
|---|---|---|
| `ad_account_id` | `.dlt/config.toml` under `[sources.facebook_marketing]` | Numeric ad account ID **without** the `act_` prefix (the pipeline adds it) |
| `access_token` | `.dlt/secrets.toml` under `[sources.facebook_marketing]` | Long-lived Marketing API user/system token with `ads_read` scope |

The current `.dlt/config.toml` already has a placeholder `ad_account_id`. To configure your own, **either** edit the files directly (don't paste secrets into chat) **or** ask Claude Code to run the `setup-secrets` skill.

**Generating a token (fast path):**

1. Go to https://developers.facebook.com/tools/explorer/
2. Pick your App (top right). Click **Get Token → Get User Access Token**.
3. Check `ads_read` and generate.
4. Sanity check: hit `GET /me/adaccounts` in the Explorer and confirm your ad account ID shows up.
5. Drop the token into `.dlt/secrets.toml`:

   ```toml
   [sources.facebook_marketing]
   access_token = "EAA..."
   ```

If the user/system user behind the token isn't assigned to your ad account in **Business Settings → Users → Ad Accounts**, the API returns Meta error code `#200` (`ads_management or ads_read permission`). Fix that in Business Manager, not in the pipeline.

---

## Running the pipeline

```bash
uv run python facebook_marketing_pipeline.py
```

This will:

1. Open `https://graph.facebook.com/v25.0/act_<id>/adcreatives?fields=...` and follow `paging.next` cursors automatically (Meta echoes `access_token` into the next URL — no auth re-injection needed).
2. Open `https://graph.facebook.com/v25.0/act_<id>/insights?level=ad&breakdowns=device_platform&action_breakdowns=...&action_attribution_windows=1d_view,7d_view,1d_click,7d_click&date_preset=last_7d&fields=...`.
3. Normalize both responses into a DuckDB database under `_local/dev/...` (path follows the `dev` profile in `dlt workspace info`).

**Defaults baked in for safe iteration:**

- `dev_mode=True` — fresh DuckDB dataset suffix on every run (no schema collisions while iterating).
- `.add_limit(1)` on the source — pulls one page (max 100 rows) per resource. Remove this once you're past the demo and want a real backfill (see `/adjust-endpoint`).
- `write_disposition="replace"` — full refresh on each run. Switch to `merge` + `dlt.sources.incremental` for production (see `/adjust-endpoint` and the dlt docs on incremental loading).
- `insights_date_preset="last_7d"` — pass `insights_date_preset="last_30d"` (or any of `today`, `yesterday`, `last_3d`, `last_14d`, `last_28d`, `last_90d`, `this_month`, `last_month`, `maximum`) to the source factory to widen the window.

### Inspecting the run

The dlt **workspace dashboard** is a local marimo UI (there's no hosted "app.dlthub.com" — the dashboard ships locally with `dlt[workspace]`):

```bash
uv run dlt workspace show
# or, equivalently:
uv run dlt dashboard
```

This boots a browser-based UI showing pipeline runs, schemas, row counts, traces, and load packages for every pipeline in this workspace.

For one-off SQL exploration, use the `dlt-workspace-mcp` server (already wired into `.mcp.json`) — from inside Claude Code, ask:

> /view-data show the first 10 rows of `adcreatives`

Claude will use the MCP tools (`execute_sql_query`, `preview_table`, `get_row_counts`) to query the destination directly — no need to write SQL by hand.

To explore in Python:

```python
import dlt, polars as pl

# dlt.attach reads the pipeline state from the workspace var dir and gives you a typed dataset handle
pipeline = dlt.attach("facebook_marketing")
ds = pipeline.dataset()

# Row counts per table
print(ds.row_counts().df())

# Read into polars (zero-copy via Arrow)
items = ds.adcreatives.df()
print(items.head())
```

---

## Pipeline documentation

### File: `facebook_marketing_pipeline.py`

A single-file dlt source built on `dlt.sources.rest_api.RESTAPIConfig`. The shape is declarative — no manual HTTP, pagination, or schema work.

#### Source factory: `facebook_marketing_source(...)`

```python
@dlt.source(name="facebook_marketing")
def facebook_marketing_source(
    ad_account_id: str = dlt.config.value,        # from .dlt/config.toml
    access_token: str = dlt.secrets.value,        # from .dlt/secrets.toml
    api_version: str = "v25.0",
    insights_date_preset: str = "last_7d",
):
    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://graph.facebook.com/{api_version}/",
            "paginator": {"type": "json_link", "next_url_path": "paging.next"},
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "data_selector": "data",
                "params": {"access_token": access_token, "limit": 100},
            },
        },
        "resources": [...],
    }
    yield from rest_api_resources(config)
```

| Decision | Why |
|---|---|
| `paginator.type = "json_link"` on `paging.next` | Meta returns full next-page URLs in `paging.next` with the access token already embedded. JSONLink follows them with zero re-auth glue. |
| `data_selector = "data"` | Marketing API wraps payloads in `{"data": [...]}`. dlt unwraps it. |
| `write_disposition = "replace"` | Full refresh per run while iterating. Switch to `merge` for production. |
| API version `v25.0` | `v22.0` expired Feb 2026. Bump to whatever's current in the [Graph API changelog](https://developers.facebook.com/docs/graph-api/changelog). |
| `dlt.config.value` / `dlt.secrets.value` | Standard dlt resolution — dlt walks the config tree under `[sources.facebook_marketing]` and the env vars `SOURCES__FACEBOOK_MARKETING__*`. |

#### Resources

**`adcreatives`** — `act_<id>/adcreatives` with `fields=` set to a curated 18-field list including `object_story_spec`, `asset_feed_spec`, and `degrees_of_freedom_spec`. Primary key: `id`.

The full field list (`CREATIVE_FIELDS`):
```
id, account_id, name, status, object_story_spec, asset_feed_spec,
degrees_of_freedom_spec, image_hash, image_url, video_id, thumbnail_url,
template_url, product_set_id, instagram_user_id, effective_object_story_id,
object_type, call_to_action_type, url_tags
```

**`ads_insights`** — `act_<id>/insights` at `level=ad` with composite primary key `(ad_id, date_start, device_platform)`.

```
breakdowns                    = device_platform
action_breakdowns             = action_type, action_target_id, action_destination
action_attribution_windows    = 1d_view, 7d_view, 1d_click, 7d_click
date_preset                   = last_7d
```

The full field list (`INSIGHTS_FIELDS`):
```
ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
account_id, account_name, impressions, spend, clicks, reach, frequency,
cpc, cpm, ctr, actions, action_values, cost_per_action_type,
inline_link_clicks, inline_link_click_ctr, unique_clicks, video_play_actions,
purchase_roas, website_purchase_roas, date_start, date_stop
```

#### Sharp edges (worth knowing before you tweak)

| Issue | What's happening | Fix |
|---|---|---|
| `28d_click` returns empty | Deprecated since Apr 2021 | Stick to `1d_view, 7d_view, 1d_click, 7d_click` (this pipeline already does) |
| `product_id` × `device_platform` breakdowns are mutually exclusive | Meta API constraint | Run a second `ads_insights_by_product` resource with `breakdowns=product_id` if you need both |
| `instagram_actor_id` is a deprecated alias | Returns nothing | Use `instagram_user_id` (this pipeline already does) |
| Numeric metrics arrive as `text` | Marketing API serializes everything as strings | Cast in the CDM transformation layer (see [Ontology & CDM](#ontology--semantic-layer)) |
| Token ↔ ad account permission mismatch | Most common failure | Token needs `ads_read` AND the token's user/system user must be assigned to the ad account in Business Manager |

#### Auto-normalization — the demo punchline

After a successful run, dlt emits **30+ tables** from the two declared resources. The deeply nested fields fan out into named child tables connected by `_dlt_parent_id`:

```
adcreatives                                                 -- 1 row per creative
adcreatives__asset_feed_spec__bodies                        -- N body-text variants per creative
adcreatives__asset_feed_spec__titles                        -- N title variants
adcreatives__asset_feed_spec__descriptions                  -- N description variants
adcreatives__asset_feed_spec__images                        -- N image variants (hashes)
adcreatives__asset_feed_spec__videos                        -- N video variants (id + thumbnail)
adcreatives__asset_feed_spec__call_to_action_types          -- allowed CTAs in the variant pool
adcreatives__asset_feed_spec__ad_formats                    -- allowed formats
adcreatives__asset_feed_spec__link_urls                     -- destination URL variants
adcreatives__asset_feed_spec__onsite_destinations           -- Shopify product/collection refs
adcreatives__object_story_spec__template_data__retailer_item_ids  -- retailer item ids on carousel/template ads

ads_insights                                                -- 1 row per (ad_id, date, device_platform)
ads_insights__actions                                       -- per-action_type counts (purchase, view_content, link_click, ...)
ads_insights__action_values                                 -- per-action_type monetary value
ads_insights__purchase_roas                                 -- omni-channel ROAS per ad/day/action
ads_insights__website_purchase_roas                         -- pixel-only ROAS
ads_insights__video_play_actions                            -- per-action_type video play counts
ads_insights__cost_per_action_type                          -- CPA per action_type
```

Plus the `_dlt_loads` and `_dlt_pipeline_state` bookkeeping tables, plus the Advantage+ feature flag columns (~33 of them) wide on the `adcreatives` table.

**This is the part that's hard to do without dlt** — Airbyte/Fivetran/the verified `facebook_ads` source either drop the nested arrays, dump them as opaque JSON columns, or require hand-written normalization in dbt downstream. dlt does it with no code.

---

## Ontology & semantic layer

After the pipeline runs and you have raw normalized tables, the `transformations` toolkit walks four steps to a Kimball-style canonical data model and (eventually) `@dlt.hub.transformation` Python functions that materialize it.

The artifacts already live in **`.schema/creative_performance/`** — they were produced by running the toolkit against this pipeline.

### The four-step workflow

| Step | Skill | Output |
|---|---|---|
| 1. Annotate sources | `/annotate-sources` | `taxonomy.json` — maps source tables to canonical concepts |
| 2. Build entity graph | `/create-ontology` | `ontology.md` (+ `ontology.json`) — entities, attributes, relationships |
| 3. Generate CDM | `/generate-cdm` | `<pipeline>.dbml` — Kimball fact/dimension model with grains, surrogate keys, SCD strategy |
| 4. Materialize | `/create-transformation` | `transformation.py` — `@dlt.hub.transformation` functions that map source → CDM |

### Step 1 — `taxonomy.json`

A machine-readable map of **canonical concept → source tables**. For each concept it captures:

```json
"Creative": {
  "description": "An ad creative — one record per Meta creative_id, holding image/video assets, narrative copy, ...",
  "use_cases": ["creative analysis", "ad performance attribution"],
  "tables": [{"table": "adcreatives", "source_pipeline": "facebook_marketing", "role": "primary"}],
  "natural_key": "id",
  "assumptions": [
    "object_story_spec.link_data.* and object_story_spec.video_data.* are mutually exclusive ...",
    "effective_object_story_id resolves to the published page post and is the stable cross-platform identifier"
  ]
}
```

This is the layer that makes the rest declarative — every downstream transformation can reach back to "which source table backs this concept" without hand-tracing JSON paths. The same file also tracks `_excluded` tables (with a reason — e.g. dlt internals, image crop coordinates, Meta-internal experiment labels) and `_outstanding_work` (gaps that need new endpoints or transformations to close).

### Step 2 — `ontology.md`

The entity-graph view, grouped by **concept** rather than by table. Eleven entities for this pipeline:

| Entity | Grain | Source tables |
|---|---|---|
| **Creative** | one row per Meta `creative_id` | `adcreatives` |
| **CreativeVariant** | `(creative_id, variant_kind, position)` | 8 `adcreatives__asset_feed_spec__*` children |
| **CreativeProductRef** | `(creative_id, ref_kind, product_id)` | `__onsite_destinations`, `__retailer_item_ids` |
| **CreativeFeatureFlag** | `(creative_id, feature_name)` | `adcreatives` (wide → long unpivot of ~33 `degrees_of_freedom_spec` columns) |
| **AdPerformance** | `(ad_id, date_start, device_platform)` | `ads_insights` |
| **ConversionEvent** | `(ad_id, date_start, device_platform, action_type)` | `__actions` ⨝ `__action_values` ⨝ `__video_play_actions` |
| **AttributedROAS** | `(ad_id, date_start, device_platform, action_type, roas_kind)` | `__purchase_roas`, `__website_purchase_roas` |
| **Ad / AdSet / Campaign / AdAccount** | thin dimensions | `ads_insights` (denorm) |

Each entity lists its attributes with **type, source column, and notes** (e.g. "*ratio — do NOT sum*", "*UNIONED across `link_data.*`, `template_data.*`, `video_data.*`*"). That note column is what survives into the CDM and the eventual semantic layer — it's the difference between a metric that aggregates correctly and one that produces nonsense at the dashboard layer.

The ontology also tracks **semantic gaps** — things the schema can't answer yet:

- `Ad ↔ Creative` bridge missing (the central gap — `ads_insights` doesn't carry `creative_id`). Resolution: add the `/{ad_account}/ads` endpoint via `/new-endpoint`.
- Campaign objective/budget — would need `/campaigns` endpoint.
- AdSet targeting/budget/schedule — would need `/adsets` endpoint.

### Step 3 — `facebook_marketing.dbml`

DBML (Database Markup Language) describing the source schema with **concept annotations** in the `note` field of each table:

```dbml
Table "adcreatives" [note: 'concept: Creative | role: primary | natural_key: id | also_source_for: AdAccount (denorm), CreativeFeatureFlag (wide unpivot)'] {
    "id"   text [pk, not null, note: 'natural_key']
    "name" text
    ...
}
```

This is the input to `/generate-cdm`, which applies Kimball dimensional modeling on top — classifying each entity as fact or dimension, defining grain, designing surrogate keys, choosing SCD strategy, and identifying conformed dimensions for cross-source joins. Render the DBML at https://dbdiagram.io/d to view it visually.

### Step 4 — transformations (not yet written)

`/create-transformation` is the next step. It would emit `@dlt.hub.transformation` functions that:

- **Cast text-typed numeric metrics** on `ads_insights` (impressions, spend, clicks, reach, frequency, cpc, cpm, ctr) to proper numeric types.
- **Unpivot** the ~33 wide `degrees_of_freedom_spec__*__enroll_status` columns into long-form `CreativeFeatureFlag(creative_id, feature_name, enroll_status)`.
- **Union** Creative narrative columns from `link_data.*`, `template_data.*`, `video_data.*` into single canonical fields (`Creative.headline`, `.body`, `.description`, `.destination_url`).
- **Join** `ads_insights__actions` and `ads_insights__action_values` on `(_dlt_parent_id, action_type)` to materialize `ConversionEvent` rows that carry both count and value.

These get written, validated, and run by Claude Code via `/create-transformation` — no dbt repository, no jinja, no manual SQL.

### Why this matters for a semantic layer

The ontology format maps almost 1:1 to a Cube/Bonnard-style cube definition:

| Ontology output | Cube YAML field |
|---|---|
| Entity name | cube name |
| Attribute with `semantic_type=measure_additive` | measure with `type: sum` |
| Attribute marked `RATIO — do NOT sum` | measure with `type: number` and a SQL expression |
| Relationship | `joins` block |
| Description fields | `description:` |

The first ~70% of cube YAML is mechanical translation from this annotation layer — meaning every connector you migrate to dlt becomes both an ingestion win **and** a semantic-layer-population win. The same `/annotate-sources` → `/create-ontology` → `/generate-cdm` chain that produces the CDM here is reusable across Shopify, GA, Google Ads, etc.

---

## Repo layout

```
dlt-sandbox/
├── README.md                                  ← this file
├── pyproject.toml                             ← uv project, dlt[duckdb,workspace] + polars + pyarrow
├── uv.lock                                    ← pinned versions
├── facebook_marketing_pipeline.py             ← the pipeline (single file, declarative RESTAPIConfig)
├── main.py                                    ← unused stub from `uv init`
│
├── .dlt/                                      ← dlt workspace settings
│   ├── .workspace                             ← marker file enabling workspace mode
│   ├── .toolkits                              ← installed toolkit manifest
│   ├── config.toml                            ← non-secret config, incl. ad_account_id
│   ├── secrets.toml                           ← access_token (gitignored — see .gitignore)
│   └── .var/dev/                              ← per-profile working dirs (pipelines, traces, load packages)
│
├── _local/dev/                                ← DuckDB destination files live here
│
├── .schema/creative_performance/              ← ontology + CDM artifacts
│   ├── taxonomy.json                          ← /annotate-sources output
│   ├── ontology.md                            ← /create-ontology output (entity graph)
│   ├── ontology.json                          ← machine-readable ontology
│   └── facebook_marketing.dbml                ← /generate-cdm input/output (annotated DBML)
│
├── .claude/                                   ← installed by `dlt ai init`
│   ├── skills/                                ← slash command implementations
│   ├── rules/                                 ← workflow rules per toolkit
│   └── settings.local.json
│
└── .mcp.json                                  ← registers dlt-workspace-mcp for Claude Code
```

`*.secrets.toml` and `_local/` are gitignored. Schema artifacts in `.schema/` are committed because they're documentation, not data.

---

## Things to play with

Once you have a green run, the natural next experiments:

1. **Remove `.add_limit(1)` and re-run.** See what a real backfill looks like (still capped by `last_7d`).
2. **Widen the date window.** Pass `insights_date_preset="last_30d"` to `facebook_marketing_source(...)` in `load_facebook_marketing()`.
3. **Add the `/ads` endpoint** to close the Ad ↔ Creative gap. From inside Claude Code:
   > /new-endpoint Add `/{ad_account_id}/ads` with fields `id, creative{id}, adset_id, campaign_id, status, effective_status` to bridge AdPerformance.ad_id → Creative.id.
4. **Add incremental loading.** From inside Claude Code:
   > /adjust-endpoint Switch `ads_insights` to `merge` write disposition with `dlt.sources.incremental` on `date_start`, 3-day lag window.
5. **Run the dashboard.** `uv run dlt workspace show` — browse the schema, traces, load packages, row counts.
6. **Build a marimo notebook from the data.** From inside Claude Code:
   > /explore-data Profile `ads_insights`. Show spend × CTR by `device_platform` and top 10 ads by ROAS.

   Then `/build-notebook` to assemble the marimo app.
7. **Materialize the CDM.** From inside Claude Code:
   > /create-transformation Implement the four transformations listed in `.schema/creative_performance/taxonomy.json._outstanding_work`.
8. **Switch destination.** `uv add "dlt[filesystem]"` then re-run with a filesystem destination — same pipeline code, lakehouse output. Tests the destination-agnostic story.

---

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `dlt: error: argument command: invalid choice: 'license'` | dlt 1.25.0 has no `dlt license` subcommand | License is just an env var or a key in `secrets.toml`. No verify command. |
| Meta error code `#200`: "Ad account owner has NOT granted ads_management or ads_read permission" | Token is valid but the user/system user behind it isn't assigned to the ad account, or the token wasn't generated with `ads_read` | Business Settings → Ad Accounts → Add Person/System User → grant "View performance" or higher. Then regenerate token. |
| `dlt ai status` shows "Workspace not yet initialized" | Missing `.dlt/.workspace` marker file | `mkdir -p .dlt && touch .dlt/.workspace` |
| Slash commands not appearing in Claude Code | New skills installed mid-session | Restart Claude Code in the project dir |
| `28d_click` columns empty | Deprecated since Apr 2021 | Use `1d_view, 7d_view, 1d_click, 7d_click` |
| `breakdowns=product_id,device_platform` returns empty | Mutually exclusive in Marketing API | Run two separate `ads_insights` resources, one per breakdown axis |

---

## References

- [dlt docs index](https://dlthub.com/docs/llms.txt) — LLM-friendly docs map
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic) — full `RESTAPIConfig` reference
- [dlt CLI](https://dlthub.com/docs/reference/command-line-interface) — `dlt workspace`, `dlt ai`, `dlt pipeline`
- [Meta Marketing API insights](https://developers.facebook.com/docs/marketing-api/insights) — fields, breakdowns, attribution windows
- [Meta Graph API changelog](https://developers.facebook.com/docs/graph-api/changelog) — current API version
- [DBML reference](https://dbml.dbdiagram.io/docs/) — for reading `facebook_marketing.dbml`
