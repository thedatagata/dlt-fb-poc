"""Submit, poll, and fetch helpers for Facebook async insights."""

# Standard library
import asyncio
import json
from dataclasses import dataclass
from typing import Any, Iterator

# Third-party
import httpx
import requests
from dlt.sources.helpers.rest_client.redaction import sanitize_url

# Local application
from _rate_limit_telemetry import (
    RateLimitObserver,
)


_FB_ERROR_KEYS = (
    "message", "type", "code", "error_subcode",
    "error_user_title", "error_user_msg", "fbtrace_id",
)


def _redacted_error_summary(resp_text: str) -> str:
    """Extract FB error fields from a response body without echoing it whole.

    FB returns errors as `{"error": {...}}`. We extract only known-safe
    fields (no token echoes have been observed in these, but limiting
    to the documented field set keeps any future surprise out of logs).
    Falls back to "<unparseable>" when the body isn't JSON.
    """
    try:
        body = json.loads(resp_text)
    except (ValueError, TypeError):
        return "<unparseable>"
    err = body.get("error") if isinstance(body, dict) else None
    if not isinstance(err, dict):
        return "<no error block>"
    return " | ".join(
        f"{k}={err[k]}" for k in _FB_ERROR_KEYS if k in err
    )


def _raise_for_status_redacted(resp: requests.Response) -> None:
    """requests.Response.raise_for_status with the access_token stripped.

    Mirrors the redaction in `_fetch_graph_objects_by_id` in
    `facebook_marketing_pipeline.py` — the default `raise_for_status`
    embeds the full request URL (including the `access_token` query
    param) into the exception message, which propagates into logs and
    callers' contexts. We rebuild the same HTTPError shape but pass
    the URL through `sanitize_url` and append a parsed FB error
    summary so 400s are diagnosable without re-running.
    """
    if resp.status_code < 400:
        return
    raise requests.HTTPError(
        f"{resp.status_code} {resp.reason} for url: {sanitize_url(resp.url)} "
        f"| fb_error: {_redacted_error_summary(resp.text)}",
        response=resp,
    )


def _raise_for_status_redacted_async(resp: httpx.Response) -> None:
    """httpx variant of `_raise_for_status_redacted`."""
    if resp.status_code < 400:
        return
    raise httpx.HTTPStatusError(
        f"{resp.status_code} {resp.reason_phrase} for url: "
        f"{sanitize_url(str(resp.request.url))} "
        f"| fb_error: {_redacted_error_summary(resp.text)}",
        request=resp.request,
        response=resp,
    )


_ACTION_BREAKDOWNS = "action_type,action_target_id,action_destination"
_ATTRIBUTION_WINDOWS = "1d_view,7d_view,1d_click,7d_click"
_TIME_INCREMENT = "1"
_POLL_FIELDS = (
    "async_status,async_percent_completion,id,"
    "error_code,error_subcode,error_user_title,error_user_msg"
)
_TERMINAL_SUCCESS = {"Job Completed"}
_TERMINAL_FAILURE = {"Job Failed", "Job Skipped"}
_TERMINAL = _TERMINAL_SUCCESS | _TERMINAL_FAILURE
_ASYNC_HTTP_MAX_ATTEMPTS = 5
_ASYNC_HTTP_RETRY_STATUSES = {429, 500, 502, 503, 504}
_ASYNC_HTTP_RETRY_METHODS = {"GET", "HEAD"}


@dataclass(frozen=True)
class AsyncJobStatus:
    """One FB async report-run polling response."""

    async_status: str
    async_percent_completion: int
    error_code: int | None = None
    error_subcode: int | None = None
    error_user_title: str | None = None
    error_user_msg: str | None = None


def submit_report_run(
    session: requests.Session,
    *,
    api_version: str,
    ad_account_id: str,
    access_token: str,
    fields: list[str],
    since: str,
    until: str,
) -> str:
    """POST /act_<id>/insights and return report_run_id."""
    resp = session.post(
        _graph_url(api_version, f"act_{ad_account_id}/insights"),
        params=_submit_params(
            access_token=access_token,
            fields=fields,
            since=since,
            until=until,
        ),
    )
    _raise_for_status_redacted(resp)
    return _parse_report_run_id(resp.json())


async def submit_report_run_async(
    client: httpx.AsyncClient,
    *,
    api_version: str,
    ad_account_id: str,
    access_token: str,
    fields: list[str],
    since: str,
    until: str,
    observer: RateLimitObserver | None = None,
) -> str:
    """Async POST /act_<id>/insights and return report_run_id."""
    resp = await _request_async(
        client,
        "POST",
        _graph_url(api_version, f"act_{ad_account_id}/insights"),
        params=_submit_params(
            access_token=access_token,
            fields=fields,
            since=since,
            until=until,
        ),
        observer=observer,
    )
    _raise_for_status_redacted_async(resp)
    return _parse_report_run_id(resp.json())


def poll_report_run(
    session: requests.Session,
    *,
    api_version: str,
    report_run_id: str,
    access_token: str,
) -> AsyncJobStatus:
    """GET /<report_run_id>; caller owns scheduling future polls."""
    resp = session.get(
        _graph_url(api_version, report_run_id),
        params={"fields": _POLL_FIELDS, "access_token": access_token},
    )
    _raise_for_status_redacted(resp)
    return _parse_status(resp.json())


async def poll_report_run_async(
    client: httpx.AsyncClient,
    *,
    api_version: str,
    report_run_id: str,
    access_token: str,
    observer: RateLimitObserver | None = None,
) -> AsyncJobStatus:
    """Async GET /<report_run_id>; caller owns scheduling future polls."""
    resp = await _request_async(
        client,
        "GET",
        _graph_url(api_version, report_run_id),
        params={"fields": _POLL_FIELDS, "access_token": access_token},
        observer=observer,
    )
    _raise_for_status_redacted_async(resp)
    return _parse_status(resp.json())


def fetch_report_run_results(
    session: requests.Session,
    *,
    api_version: str,
    report_run_id: str,
    access_token: str,
    fields: list[str],
) -> Iterator[list[dict]]:
    """GET /<report_run_id>/insights and yield one data page at a time."""
    url = _graph_url(api_version, f"{report_run_id}/insights")
    params: dict[str, Any] | None = {
        "fields": ",".join(fields),
        "limit": 500,
        "access_token": access_token,
    }
    while url:
        resp = session.get(url, params=params)
        _raise_for_status_redacted(resp)
        payload = resp.json()
        page = payload.get("data") or []
        if page:
            yield page
        url = (payload.get("paging") or {}).get("next")
        params = None


async def fetch_report_run_results_async(
    client: httpx.AsyncClient,
    *,
    api_version: str,
    report_run_id: str,
    access_token: str,
    fields: list[str],
    observer: RateLimitObserver | None = None,
) -> list[list[dict]]:
    """Async fetch of all report-run pages, returned as page lists."""
    pages: list[list[dict]] = []
    url = _graph_url(api_version, f"{report_run_id}/insights")
    params: dict[str, Any] | None = {
        "fields": ",".join(fields),
        "limit": 500,
        "access_token": access_token,
    }
    while url:
        resp = await _request_async(
            client,
            "GET",
            url,
            params=params,
            observer=observer,
        )
        _raise_for_status_redacted_async(resp)
        payload = resp.json()
        page = payload.get("data") or []
        if page:
            pages.append(page)
        url = (payload.get("paging") or {}).get("next")
        params = None
    return pages


def is_terminal(status: str) -> bool:
    return status in _TERMINAL


def is_terminal_success(status: str) -> bool:
    return status in _TERMINAL_SUCCESS


def is_terminal_failure(status: str) -> bool:
    return status in _TERMINAL_FAILURE


def build_async_client() -> httpx.AsyncClient:
    """Return an async client for Graph API calls.

    Retries are implemented in `_request_async` so tests can use a plain
    MockTransport without extra transport wrappers.
    """
    return httpx.AsyncClient(timeout=60.0)


def fields_hash_material(fields: list[str], *, api_version: str = "v25.0") -> str:
    """Stable material whose hash identifies async insight query shape."""
    return json.dumps(
        {
            "api_version": api_version,
            "fields": fields,
            "action_breakdowns": _ACTION_BREAKDOWNS,
            "action_attribution_windows": _ATTRIBUTION_WINDOWS,
            "time_increment": _TIME_INCREMENT,
        },
        sort_keys=True,
    )


async def _request_async(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None,
    observer: RateLimitObserver | None,
) -> httpx.Response:
    last_response: httpx.Response | None = None
    for attempt in range(_ASYNC_HTTP_MAX_ATTEMPTS):
        resp = await client.request(method, url, params=params)
        if observer is not None:
            observer.observe_headers(resp.headers)
        last_response = resp
        if (
            method.upper() not in _ASYNC_HTTP_RETRY_METHODS
            or resp.status_code not in _ASYNC_HTTP_RETRY_STATUSES
        ):
            return resp
        if attempt < _ASYNC_HTTP_MAX_ATTEMPTS - 1:
            await asyncio.sleep(min(2**attempt, 30))
    if last_response is None:
        raise RuntimeError("async request did not return a response")
    return last_response


def _submit_params(
    *,
    access_token: str,
    fields: list[str],
    since: str,
    until: str,
) -> dict[str, str]:
    return {
        "level": "ad",
        "action_breakdowns": _ACTION_BREAKDOWNS,
        "action_attribution_windows": _ATTRIBUTION_WINDOWS,
        "time_increment": _TIME_INCREMENT,
        "time_range": json.dumps({"since": since, "until": until}),
        "fields": ",".join(fields),
        "access_token": access_token,
    }


def _parse_report_run_id(payload: dict[str, Any]) -> str:
    rid = payload.get("report_run_id")
    if not rid:
        raise RuntimeError(f"FB did not return report_run_id: {payload}")
    return str(rid)


def _parse_status(payload: dict[str, Any]) -> AsyncJobStatus:
    return AsyncJobStatus(
        async_status=str(payload.get("async_status") or ""),
        async_percent_completion=_int_or_zero(payload.get("async_percent_completion")),
        error_code=_int_or_none(payload.get("error_code")),
        error_subcode=_int_or_none(payload.get("error_subcode")),
        error_user_title=_str_or_none(payload.get("error_user_title")),
        error_user_msg=_str_or_none(payload.get("error_user_msg")),
    )


def _int_or_zero(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else 0


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return None
    return None


def _str_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _graph_url(api_version: str, path: str) -> str:
    return f"https://graph.facebook.com/{api_version}/{path}"