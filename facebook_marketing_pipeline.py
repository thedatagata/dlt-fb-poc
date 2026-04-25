"""Custom dlt REST API pipeline for the Facebook Marketing API.

POC scope: pull `adcreatives` (full creative payload) and ad-level `insights`
with breakdowns and action attribution that the verified `facebook_ads`
source and Airbyte's connector do not surface by default. Demonstrates
dlt's automatic normalization of deeply nested JSON (object_story_spec,
asset_feed_spec, actions, action_values) into transform-ready relational
child tables.
"""

from typing import Any, List

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

CREATIVE_FIELDS: List[str] = [
    "id",
    "account_id",
    "name",
    "status",
    "object_story_spec",
    "asset_feed_spec",
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

INSIGHTS_FIELDS: List[str] = [
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "campaign_id",
    "campaign_name",
    "account_id",
    "account_name",
    "impressions",
    "spend",
    "clicks",
    "reach",
    "frequency",
    "cpc",
    "cpm",
    "ctr",
    "actions",
    "action_values",
    "cost_per_action_type",
    "inline_link_clicks",
    "inline_link_click_ctr",
    "unique_clicks",
    "video_play_actions",
    "purchase_roas",
    "website_purchase_roas",
    "date_start",
    "date_stop",
]


@dlt.source(name="facebook_marketing")
def facebook_marketing_source(
    ad_account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    api_version: str = "v25.0",
    insights_date_preset: str = "last_7d",
) -> Any:
    """Custom Facebook Marketing API source.

    Args:
        ad_account_id: Numeric ad account ID (without the `act_` prefix).
            Auto-loaded from .dlt/config.toml under [sources.facebook_marketing].
        access_token: Long-lived Marketing API user/system token with `ads_read`.
            Auto-loaded from .dlt/secrets.toml under [sources.facebook_marketing].
        api_version: Graph API version. Defaults to v25.0 (current stable).
        insights_date_preset: One of `today`, `yesterday`, `last_3d`, `last_7d`,
            `last_14d`, `last_28d`, `last_30d`, `last_90d`, `this_month`,
            `last_month`, `maximum`. Defaults to `last_7d`.

    Example:
        pipeline.run(facebook_marketing_source())
        pipeline.run(facebook_marketing_source(insights_date_preset="last_30d"))
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://graph.facebook.com/{api_version}/",
            "paginator": {
                "type": "json_link",
                "next_url_path": "paging.next",
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "data_selector": "data",
                "params": {
                    "access_token": access_token,
                    "limit": 100,
                },
            },
        },
        "resources": [
            {
                "name": "adcreatives",
                "primary_key": "id",
                "endpoint": {
                    "path": f"act_{ad_account_id}/adcreatives",
                    "params": {
                        "fields": ",".join(CREATIVE_FIELDS),
                    },
                },
            },
            {
                "name": "ads_insights",
                "primary_key": ["ad_id", "date_start", "device_platform"],
                "endpoint": {
                    "path": f"act_{ad_account_id}/insights",
                    "params": {
                        "level": "ad",
                        "breakdowns": "device_platform",
                        "action_breakdowns": "action_type,action_target_id,action_destination",
                        "action_attribution_windows": "1d_view,7d_view,1d_click,7d_click",
                        "date_preset": insights_date_preset,
                        "fields": ",".join(INSIGHTS_FIELDS),
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_facebook_marketing() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="facebook_marketing",
        destination="duckdb",
        dataset_name="facebook_marketing_data",
        dev_mode=True,
    )

    load_info = pipeline.run(facebook_marketing_source().add_limit(1))
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_facebook_marketing()
