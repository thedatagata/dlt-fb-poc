"""Run-scoped Facebook Marketing API rate-limit telemetry.

The Marketing API returns pressure hints in response headers. This
module parses those headers defensively, accumulates the highest
observed pressure for one ingest run, and exposes a requests response
hook that is inert unless a run has bound an observer.
"""

# Standard library
import contextlib
import contextvars
import json
import logging
from dataclasses import dataclass
from typing import Any, Iterator

# Third-party
import requests


_OBSERVER: contextvars.ContextVar["RateLimitObserver | None"] = contextvars.ContextVar(
    "facebook_rate_limit_observer",
    default=None,
)


@dataclass(frozen=True)
class AppUsage:
    call_count: float | None = None
    total_cputime: float | None = None
    total_time: float | None = None


@dataclass(frozen=True)
class AdAccountUsage:
    acc_id_util_pct: float | None = None
    reset_time_duration: int | None = None
    ads_api_access_tier: str | None = None


@dataclass(frozen=True)
class BusinessUseCaseUsage:
    business_id: str
    type: str | None = None
    call_count: float | None = None
    total_cputime: float | None = None
    total_time: float | None = None
    estimated_time_to_regain_access: int | None = None


class RateLimitObserver:
    """Accumulate worst observed FB rate-limit pressure for one run."""

    def __init__(self, *, log: logging.Logger | Any) -> None:
        self._log = log

        self.max_app_call_count_pct: float | None = None
        self.max_app_cputime_pct: float | None = None
        self.max_app_total_time_pct: float | None = None

        self.max_acc_util_pct: float | None = None
        self.min_acc_reset_seconds: int | None = None
        self.ads_api_access_tier: str | None = None

        self.max_buc_call_count_pct: float | None = None
        self.max_buc_cputime_pct: float | None = None
        self.max_buc_total_time_pct: float | None = None
        self.max_buc_eta_minutes: int | None = None
        self.worst_buc_type: str | None = None
        self.worst_buc_business_id: str | None = None
        self._worst_buc_key: tuple[int, int, float] | None = None

        self.throttled_during_run = False
        self.responses_seen = 0
        self.responses_with_rate_limit_headers = 0

    def observe_response(self, response: requests.Response) -> None:
        """Parse and accumulate pressure headers from a requests response."""
        self.responses_seen += 1
        self._observe_headers(response.headers)

    def observe_headers(self, headers: Any) -> None:
        """Parse and accumulate pressure headers from a mapping-like object."""
        self.responses_seen += 1
        self._observe_headers(headers)

    def _observe_headers(self, headers: Any) -> None:
        saw_header = False

        app = parse_app_usage(headers.get("X-App-Usage"), log=self._log)
        if app is not None:
            saw_header = True
            self._observe_app(app)

        account = parse_ad_account_usage(
            headers.get("X-Ad-Account-Usage"),
            log=self._log,
        )
        if account is not None:
            saw_header = True
            self._observe_account(account)

        buc = parse_business_use_case_usage(
            headers.get("X-Business-Use-Case-Usage"),
            log=self._log,
        )
        if buc:
            saw_header = True
            self._observe_business_use_cases(buc)

        if saw_header:
            self.responses_with_rate_limit_headers += 1

    def to_row_fields(self) -> dict[str, Any]:
        """Return fields matching IngestRunRow rate-limit columns."""
        return {
            "max_app_call_count_pct": self.max_app_call_count_pct,
            "max_app_cputime_pct": self.max_app_cputime_pct,
            "max_app_total_time_pct": self.max_app_total_time_pct,
            "max_acc_util_pct": self.max_acc_util_pct,
            "min_acc_reset_seconds": self.min_acc_reset_seconds,
            "ads_api_access_tier": self.ads_api_access_tier,
            "max_buc_call_count_pct": self.max_buc_call_count_pct,
            "max_buc_cputime_pct": self.max_buc_cputime_pct,
            "max_buc_total_time_pct": self.max_buc_total_time_pct,
            "max_buc_eta_minutes": self.max_buc_eta_minutes,
            "worst_buc_type": self.worst_buc_type,
            "worst_buc_business_id": self.worst_buc_business_id,
            "throttled_during_run": self.throttled_during_run,
        }

    def observation_metadata(
        self,
        *,
        org_uid: str,
        client_uid: str,
        client_slug: str,
        ad_account_id: str,
    ) -> dict[str, Any]:
        """Return JSON-safe Dagster observation metadata."""
        return {
            "org_uid": org_uid,
            "client_uid": client_uid,
            "client_slug": client_slug,
            "ad_account_id": ad_account_id,
            "responses_seen": self.responses_seen,
            "responses_with_rate_limit_headers": self.responses_with_rate_limit_headers,
            **self.to_row_fields(),
        }

    def _observe_app(self, app: AppUsage) -> None:
        self.max_app_call_count_pct = _max_optional(
            self.max_app_call_count_pct,
            app.call_count,
        )
        self.max_app_cputime_pct = _max_optional(
            self.max_app_cputime_pct,
            app.total_cputime,
        )
        self.max_app_total_time_pct = _max_optional(
            self.max_app_total_time_pct,
            app.total_time,
        )
        self._log_threshold("app.call_count", app.call_count)
        self._log_threshold("app.total_cputime", app.total_cputime)
        self._log_threshold("app.total_time", app.total_time)

    def _observe_account(self, account: AdAccountUsage) -> None:
        self.max_acc_util_pct = _max_optional(
            self.max_acc_util_pct,
            account.acc_id_util_pct,
        )
        if account.reset_time_duration is not None:
            if self.min_acc_reset_seconds is None:
                self.min_acc_reset_seconds = account.reset_time_duration
            else:
                self.min_acc_reset_seconds = min(
                    self.min_acc_reset_seconds,
                    account.reset_time_duration,
                )
        if account.ads_api_access_tier:
            self.ads_api_access_tier = account.ads_api_access_tier
            if account.ads_api_access_tier == "development_access":
                self._log.warning(
                    "Facebook ads API access tier is development_access; "
                    "Marketing API rate limits may be materially lower"
                )
        self._log_threshold("ad_account.acc_id_util_pct", account.acc_id_util_pct)

    def _observe_business_use_cases(self, usages: list[BusinessUseCaseUsage]) -> None:
        worst = max(usages, key=_business_usage_sort_key)
        for usage in usages:
            self.max_buc_call_count_pct = _max_optional(
                self.max_buc_call_count_pct,
                usage.call_count,
            )
            self.max_buc_cputime_pct = _max_optional(
                self.max_buc_cputime_pct,
                usage.total_cputime,
            )
            self.max_buc_total_time_pct = _max_optional(
                self.max_buc_total_time_pct,
                usage.total_time,
            )
            self.max_buc_eta_minutes = _max_int_optional(
                self.max_buc_eta_minutes,
                usage.estimated_time_to_regain_access,
            )
        worst_key = _business_usage_sort_key(worst)
        if self._worst_buc_key is None or worst_key > self._worst_buc_key:
            self._worst_buc_key = worst_key
            self.worst_buc_type = worst.type
            self.worst_buc_business_id = worst.business_id

        self._log_threshold("buc.call_count", self.max_buc_call_count_pct)
        self._log_threshold("buc.total_cputime", self.max_buc_cputime_pct)
        self._log_threshold("buc.total_time", self.max_buc_total_time_pct)
        if (
            worst.estimated_time_to_regain_access is not None
            and worst.estimated_time_to_regain_access > 0
        ):
            self.throttled_during_run = True
            self._log.error(
                "Facebook business-use-case throttle active business_id=%s "
                "type=%s eta_minutes=%s",
                worst.business_id,
                worst.type,
                worst.estimated_time_to_regain_access,
            )

    def _log_threshold(self, label: str, value: float | None) -> None:
        if value is None:
            return
        if value >= 100:
            self.throttled_during_run = True
            self._log.error("Facebook rate-limit pressure %s=%.2f%%", label, value)
        elif value >= 80:
            self._log.warning("Facebook rate-limit pressure %s=%.2f%%", label, value)
        elif value >= 50:
            self._log.info("Facebook rate-limit pressure %s=%.2f%%", label, value)


@contextlib.contextmanager
def bind_observer(observer: RateLimitObserver) -> Iterator[RateLimitObserver]:
    """Bind an observer to the current context for request hooks."""
    token = _OBSERVER.set(observer)
    try:
        yield observer
    finally:
        _OBSERVER.reset(token)


def current_observer() -> RateLimitObserver | None:
    """Return the observer bound to this context, if any."""
    return _OBSERVER.get()


def install_rate_limit_hook(
    session: requests.Session,
    log: logging.Logger | Any,
    *,
    observer: RateLimitObserver | None = None,
) -> None:
    """Install a requests response hook.

    If an observer is provided, the hook records to that observer directly.
    Otherwise it falls back to the context-bound observer for callers that
    issue requests in the same execution context.
    """

    def _hook(response: requests.Response, *args: Any, **kwargs: Any) -> requests.Response:
        active_observer = observer or current_observer()
        if active_observer is None:
            return response
        try:
            active_observer.observe_response(response)
        except Exception as exc:
            log.debug("ignored Facebook rate-limit telemetry parse failure: %s", exc)
        return response

    session.hooks.setdefault("response", [])
    session.hooks["response"].append(_hook)


def parse_app_usage(raw: str | None, *, log: logging.Logger | Any) -> AppUsage | None:
    data = _loads_header(raw, "X-App-Usage", log=log)
    if not isinstance(data, dict):
        return None
    return AppUsage(
        call_count=_to_float(data.get("call_count")),
        total_cputime=_to_float(data.get("total_cputime")),
        total_time=_to_float(data.get("total_time")),
    )


def parse_ad_account_usage(
    raw: str | None,
    *,
    log: logging.Logger | Any,
) -> AdAccountUsage | None:
    data = _loads_header(raw, "X-Ad-Account-Usage", log=log)
    if not isinstance(data, dict):
        return None
    return AdAccountUsage(
        acc_id_util_pct=_to_float(data.get("acc_id_util_pct")),
        reset_time_duration=_to_int(data.get("reset_time_duration")),
        ads_api_access_tier=_to_str(data.get("ads_api_access_tier")),
    )


def parse_business_use_case_usage(
    raw: str | None,
    *,
    log: logging.Logger | Any,
) -> list[BusinessUseCaseUsage]:
    data = _loads_header(raw, "X-Business-Use-Case-Usage", log=log)
    if not isinstance(data, dict):
        return []

    usages: list[BusinessUseCaseUsage] = []
    for business_id, buckets in data.items():
        if not isinstance(business_id, str) or not isinstance(buckets, list):
            continue
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            usages.append(
                BusinessUseCaseUsage(
                    business_id=business_id,
                    type=_to_str(bucket.get("type")),
                    call_count=_to_float(bucket.get("call_count")),
                    total_cputime=_to_float(bucket.get("total_cputime")),
                    total_time=_to_float(bucket.get("total_time")),
                    estimated_time_to_regain_access=_to_int(
                        bucket.get("estimated_time_to_regain_access")
                    ),
                )
            )
    return usages


def _loads_header(raw: str | None, name: str, *, log: logging.Logger | Any) -> Any:
    if raw is None or raw == "":
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.debug("ignored malformed Facebook %s header: %s", name, exc)
        return None


def _business_usage_sort_key(usage: BusinessUseCaseUsage) -> tuple[int, int, float]:
    eta = usage.estimated_time_to_regain_access or 0
    pressure = max(
        usage.call_count or 0,
        usage.total_cputime or 0,
        usage.total_time or 0,
    )
    return (1 if eta > 0 else 0, eta, pressure)


def _max_optional(left: float | None, right: float | None) -> float | None:
    if right is None:
        return left
    if left is None:
        return right
    return max(left, right)


def _max_int_optional(left: int | None, right: int | None) -> int | None:
    if right is None:
        return left
    if left is None:
        return right
    return max(left, right)


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> int | None:
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


def _to_str(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None