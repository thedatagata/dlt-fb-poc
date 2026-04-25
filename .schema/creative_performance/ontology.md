# Creative Performance Ontology

**CDM:** `creative_performance`
**Source pipeline:** `facebook_marketing` (single source — no cross-source stitching)
**Generated from:** `taxonomy.json` + `facebook_marketing.dbml`

This ontology is the entity-graph view of the Fishewear Facebook Marketing data — the input to Kimball CDM design and (eventually) a Bonnard-style semantic layer. Every entity, attribute, and relationship below is grounded in columns that actually exist in the source schema.

---

## Entity overview

| Entity | Grain | Source tables | Notes |
|---|---|---|---|
| **Creative** | one row per Meta creative_id | `adcreatives` | Creative DNA: copy, assets, Advantage+ flags, catalog refs |
| **CreativeVariant** | (creative_id, variant_kind, position) | 8 `adcreatives__asset_feed_spec__*` children | DCO multi-asset variants — needs union+discriminator |
| **CreativeProductRef** | (creative_id, ref_kind, product_id) | `adcreatives__asset_feed_spec__onsite_destinations`, `adcreatives__object_story_spec__template_data__retailer_item_ids` | Bridge to Shopify catalog |
| **CreativeFeatureFlag** | (creative_id, feature_name) | `adcreatives` (wide → long unpivot) | Advantage+ / DCO enrollment per feature |
| **AdPerformance** | (ad_id, date_start, device_platform) | `ads_insights` | Daily delivery + cost fact |
| **ConversionEvent** | (ad_id, date_start, device_platform, action_type) | `ads_insights__actions` ⨝ `ads_insights__action_values` ⨝ `ads_insights__video_play_actions` | Per-action-type events with attribution windows |
| **AttributedROAS** | (ad_id, date_start, device_platform, action_type, roas_kind) | `ads_insights__purchase_roas`, `ads_insights__website_purchase_roas` | Omni vs. pixel ROAS — values are RATIOS |
| **Ad** | one row per ad_id | `ads_insights` (denorm) | **Missing creative_id** — gap |
| **AdSet** | one row per adset_id | `ads_insights` (denorm) | Thin dim — no targeting/budget |
| **Campaign** | one row per campaign_id | `ads_insights` (denorm) | Thin dim — no objective/budget |
| **AdAccount** | one row per account_id | `ads_insights` + `adcreatives` (denorm) | |

---

## Creative

The ad creative as authored in Ads Manager. One row per Meta `creative_id`. The richest entity in the ontology — it carries text copy, asset hashes, format flags, Advantage+ enrollment, and Shopify product references.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| creative_id | text | `adcreatives.id` | natural key |
| account_id | text | `adcreatives.account_id` | FK → AdAccount |
| name | text | `adcreatives.name` | |
| status | text | `adcreatives.status` | ACTIVE / PAUSED / DELETED |
| object_type | text | `adcreatives.object_type` | SHARE / VIDEO / PHOTO |
| call_to_action_type | text | `adcreatives.call_to_action_type` | canonical CTA (SHOP_NOW etc.) |
| effective_object_story_id | text | `adcreatives.effective_object_story_id` | stable cross-platform post id |
| instagram_user_id | text | `adcreatives.instagram_user_id` | also in `object_story_spec__instagram_user_id` |
| page_id | text | `adcreatives.object_story_spec__page_id` | |
| image_hash | text | `adcreatives.image_hash` | |
| image_url | text | `adcreatives.image_url` | |
| thumbnail_url | text | `adcreatives.thumbnail_url` | |
| video_id | text | `adcreatives.video_id` | also in `object_story_spec__video_data__video_id` |
| **headline** | text | `object_story_spec__link_data__name` | UNIONED across `template_data__name`, `video_data__title` |
| **body** | text | `object_story_spec__link_data__message` | UNIONED across `template_data__message`, `video_data__message` |
| **description** | text | `object_story_spec__link_data__description` | UNIONED across `video_data__link_description` |
| caption | text | `object_story_spec__link_data__caption` | |
| **destination_url** | text | `object_story_spec__link_data__link` | UNIONED across `template_data__link`, `*__call_to_action__value__link` |
| asset_feed_optimization_type | text | `adcreatives.asset_feed_spec__optimization_type` | DEGREES_OF_FREEDOM / FORMAT_AUTOMATION |
| use_flexible_image_aspect_ratio | bool | `object_story_spec__link_data__use_flexible_image_aspect_ratio` | |
| is_click_to_message | bool | `asset_feed_spec__additional_data__is_click_to_message` | |
| is_multi_share_end_card | bool | `asset_feed_spec__additional_data__multi_share_end_card` | |
| show_multiple_images | bool | `object_story_spec__template_data__show_multiple_images` | |
| force_single_link | bool | `object_story_spec__template_data__force_single_link` | |
| is_shops_bundle | bool | `asset_feed_spec__shops_bundle` | |
| is_reasons_to_shop | bool | `asset_feed_spec__reasons_to_shop` | |
| is_auto_update_allowed | bool | `asset_feed_spec__promotional_metadata__is_auto_update_allowed` | |
| template_format_option | text | `object_story_spec__template_data__format_option` | |
| template_post_click_headline | text | `object_story_spec__template_data__post_click_configuration__post_click_item_headline` | |
| template_post_click_description | text | `object_story_spec__template_data__post_click_configuration__post_click_item_description` | |
| call_destination_type | text | `asset_feed_spec__call_ads_configuration__call_destination_type` | |
| call_phone_number | text | `asset_feed_spec__call_ads_configuration__phone_number` | |
| primary_product_set_id | text | `adcreatives.product_set_id` | FK → Shopify product_set (if catalog ad) |

**Relationships:**

| Edge | To | Cardinality |
|---|---|---|
| HAS_VARIANT | CreativeVariant | 1:N |
| REFERENCES_PRODUCT | CreativeProductRef | 1:N |
| HAS_FEATURE_FLAG | CreativeFeatureFlag | 1:N |
| BELONGS_TO_ACCOUNT | AdAccount | N:1 |
| RENDERS_AS (inverse, **inferred — gap**) | Ad | 1:N |

---

## CreativeVariant

DCO multi-asset variants. One Creative spawns N variants across 8 child tables. Materialized by union with a `variant_kind` discriminator.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| creative_id | text | walked via `_dlt_parent_id` | FK → Creative |
| variant_kind | text | discriminator | body / title / description / image / video / cta / ad_format / link_url |
| position | bigint | `_dlt_list_idx` | positional ordering — Meta does not expose stable variant IDs |
| text | text | `bodies.text` / `titles.text` / `descriptions.text` | populated when variant_kind ∈ {body, title, description} |
| enum_value | text | `call_to_action_types.value` / `ad_formats.value` | populated when variant_kind ∈ {cta, ad_format} |
| image_hash | text | `images.hash` | |
| video_id | text | `videos.video_id` | |
| thumbnail_url | text | `videos.thumbnail_url` | |
| website_url | text | `link_urls.website_url` | |
| display_url | text | `link_urls.display_url` | |

---

## CreativeProductRef

Shopify catalog references attached to a creative. Bridges Meta creative → Shopify product/collection.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| creative_id | text | walked via `_dlt_parent_id` | FK → Creative |
| ref_kind | text | discriminator | single_product / collection / retailer_item |
| product_id | text | `onsite_destinations.details_page_product_id` ∪ `retailer_item_ids.value` | Shopify product id |
| product_set_id | text | `onsite_destinations.shop_collection_product_set_id` | |
| storefront_shop_id | text | `onsite_destinations.storefront_shop_id` | |
| auto_optimization | text | `onsite_destinations.auto_optimization` | |

---

## CreativeFeatureFlag

Long-form view of the ~33 `degrees_of_freedom_spec__creative_features_spec__*__enroll_status` columns. Requires a wide→long unpivot in the transformation step.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| creative_id | text | `adcreatives.id` | FK → Creative |
| feature_name | text | derived from column name | e.g. `advantage_plus_creative`, `image_enhancement`, `text_optimizations`, `image_uncrop`, `enhance_cta` |
| enroll_status | text | `adcreatives.degrees_of_freedom_spec__creative_features_spec__*__enroll_status` | OPT_IN / OPT_OUT / OPT_OUT_BY_USER |

---

## AdPerformance

Daily ad-level fact at `(ad_id, date_start, device_platform)` grain. The cost-and-delivery side of the warehouse.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| ad_id | text | `ads_insights.ad_id` | natural key part 1; FK → Ad |
| date_start | date | `ads_insights.date_start` | natural key part 2 (cast text→date) |
| device_platform | text | `ads_insights.device_platform` | natural key part 3 — breakdown |
| date_stop | date | `ads_insights.date_stop` | |
| ad_name / adset_id / adset_name / campaign_id / campaign_name / account_id / account_name | text | denorm | FKs to Ad / AdSet / Campaign / AdAccount |
| impressions | int64 | `ads_insights.impressions` | cast text→int64; sum-additive |
| reach | int64 | `ads_insights.reach` | cast; **NOT sum-additive across dates** |
| clicks | int64 | `ads_insights.clicks` | cast; sum-additive |
| inline_link_clicks | int64 | `ads_insights.inline_link_clicks` | cast; sum-additive |
| unique_clicks | int64 | `ads_insights.unique_clicks` | cast; **NOT sum-additive across dates** |
| spend | decimal | `ads_insights.spend` | cast; sum-additive; USD assumed |
| frequency | decimal | `ads_insights.frequency` | cast; **ratio — do NOT sum** |
| cpc | decimal | `ads_insights.cpc` | cast; **ratio** |
| cpm | decimal | `ads_insights.cpm` | cast; **ratio** |
| ctr | decimal | `ads_insights.ctr` | cast; **ratio (percent)** |
| inline_link_click_ctr | decimal | `ads_insights.inline_link_click_ctr` | cast; **ratio** |

**Relationships:**

| Edge | To | Cardinality |
|---|---|---|
| MEASURES | Ad | N:1 |
| BELONGS_TO_ADSET | AdSet | N:1 |
| BELONGS_TO_CAMPAIGN | Campaign | N:1 |
| BELONGS_TO_ACCOUNT | AdAccount | N:1 |
| HAS_CONVERSION_EVENT | ConversionEvent | 1:N |
| HAS_ATTRIBUTED_ROAS | AttributedROAS | 1:N |

---

## ConversionEvent

Per-action-type events with attribution windows. Materialized by joining `ads_insights__actions` and `ads_insights__action_values` on `(_dlt_parent_id, action_type)` so each row carries both count and value. Video views fold in from `ads_insights__video_play_actions`.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| ad_id / date_start / device_platform | text/date/text | walked via `_dlt_parent_id` | natural key parts 1-3; FK → AdPerformance |
| action_type | text | `actions.action_type` | natural key part 4 — e.g. `purchase`, `add_to_cart`, `view_content`, `link_click`, `video_view` |
| action_destination | text | `actions.action_destination` | |
| action_target_id | text | `actions.action_target_id` | useful for product_id breakdowns |
| count | decimal | `actions.value` | sum-additive |
| count_1d_view | decimal | `actions._1d_view` | sum-additive within window |
| count_1d_click | decimal | `actions._1d_click` | sum-additive within window |
| count_7d_click | decimal | `actions._7d_click` | sum-additive within window |
| value | decimal | `action_values.value` | monetary; sum-additive |
| value_1d_view / value_1d_click / value_7d_click | decimal | `action_values._*` | sum-additive within window |

---

## AttributedROAS

Per-action-type ROAS at attribution-window grain. **Critical:** all `roas_*` columns are RATIOS (revenue/spend). Never sum-aggregate; recompute as `SUM(revenue) / NULLIF(SUM(spend), 0)` at query time.

| Attribute | Type | Source | Notes |
|---|---|---|---|
| ad_id / date_start / device_platform | text/date/text | walked via `_dlt_parent_id` | natural key parts 1-3 |
| action_type | text | `purchase_roas.action_type` | natural key part 4 |
| roas_kind | text | discriminator | `omni` (purchase_roas) / `website` (website_purchase_roas) |
| roas | decimal | `*_purchase_roas.value` | RATIO |
| roas_1d_view / roas_1d_click / roas_7d_click | decimal | `*_purchase_roas._*` | RATIO |

---

## Ad / AdSet / Campaign / AdAccount

Thin dimensions derived from the denormalized id/name pairs on `ads_insights`. Listed for completeness — they are FK targets for AdPerformance but carry no descriptive attributes beyond name + parent FKs.

| Entity | Attributes | Notes |
|---|---|---|
| **Ad** | ad_id (PK), ad_name, adset_id, campaign_id, account_id, **creative_id (MISSING)** | The bridge to Creative is the central gap |
| **AdSet** | adset_id (PK), adset_name, campaign_id, account_id | No targeting/budget — would need /adsets endpoint |
| **Campaign** | campaign_id (PK), campaign_name, account_id | No objective/budget — would need /campaigns endpoint |
| **AdAccount** | account_id (PK), account_name | |

---

## Semantic gaps

| Gap | Use case affected | Resolution |
|---|---|---|
| **Ad ↔ Creative bridge missing** | Creative-level performance attribution (the central POC story) | Add `/{ad_account}/ads` endpoint via `/new-endpoint` with fields `id, creative{id}, adset_id, campaign_id, status, effective_status` |
| Campaign objective/budget | Performance analysis | `/campaigns` endpoint |
| AdSet targeting/budget/schedule | Performance analysis | `/adsets` endpoint |
| Shopify product metadata | Product-level creative attribution | Separate Shopify ingestion pipeline |
| CreativeVariant-level performance | Variant attribution | Not derivable — Meta does not expose per-variant impressions/clicks |

---

## Assumptions & exclusions

- **Single-source ontology** — only `facebook_marketing` pipeline contributes. No cross-source stitching applies; `master_source` is `facebook_marketing` for every attribute.
- **Cross-format unification** — Creative.headline / .body / .description / .destination_url are unioned across `link_data.*` (image/link ads), `template_data.*` (carousel/template), and `video_data.*` (video) source columns. CDM transformation should COALESCE in that priority order.
- **Numeric metrics typed as text** — every metric on `ads_insights` and its children is `text` in the source. CDM transformation must cast (impressions/clicks/reach → int64; spend/cpc/cpm/ctr/frequency/ROAS → decimal).
- **CreativeFeatureFlag is virtual** — materialized only after a wide→long unpivot of the ~33 `degrees_of_freedom_spec__*__enroll_status` columns.
- **Excluded source tables** (carried from `taxonomy.json._excluded`): dlt internals, image crop coordinate sub-tables, `*__adlabels` experiment labels, `asset_customization_rules` placement targeting, `promotional_metadata.allowed_coupon_code_sources`.

---

Please review this file. Once confirmed, the next step is `generate-cdm` — applying Kimball dimensional modeling (fact vs. dimension classification, surrogate keys, SCD strategy, conformed dimensions) to produce the implementation-ready DBML.
