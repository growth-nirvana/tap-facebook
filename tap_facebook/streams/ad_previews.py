"""Stream class for AdPreviews.

Fetches ad-format-specific preview iframe URLs from Meta's
GET /{ad-id}/previews endpoint. Meta returns each preview as an HTML
``<iframe src="...">`` snippet; we parse out the ``src`` URL so it can be
loaded in a downstream report.

Reference: https://developers.facebook.com/docs/marketing-api/reference/ad-preview/
"""

from __future__ import annotations

import re
import time
import typing as t
from html import unescape

import requests
from singer_sdk import typing as th
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, Stream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


DEFAULT_AD_PREVIEW_FORMATS = (
    "INSTAGRAM_STANDARD",
    "INSTAGRAM_STORY",
    "FACEBOOK_STORY",
    "MOBILE_FEED_STANDARD",
    "DESKTOP_FEED_STANDARD",
)

# Captures the iframe src URL from Meta's HTML preview body.
IFRAME_SRC_RE = re.compile(r"""src=["']([^"']+)["']""")

RETRYABLE_FACEBOOK_CODES = {1, 2, 4, 17, 32, 341, 368, 613}
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2


class AdPreviewsStream(Stream):
    """Per-ad preview iframe URLs, one row per (ad_id, ad_format)."""

    name = "ad_previews"
    primary_keys = ("account_id", "ad_id", "ad_format")  # noqa: RUF012
    replication_method = REPLICATION_FULL_TABLE

    schema = th.PropertiesList(
        th.Property("account_id", th.StringType),
        th.Property("ad_id", th.StringType),
        th.Property("ad_name", th.StringType),
        th.Property("effective_status", th.StringType),
        th.Property("ad_format", th.StringType),
        th.Property("preview_url", th.StringType),
        th.Property("preview_iframe_html", th.StringType),
        th.Property("run_id", th.IntegerType),
    ).to_dict()

    @property
    def url_base(self) -> str:
        return f"https://graph.facebook.com/{self.config['api_version']}"

    @property
    def _ad_formats(self) -> list[str]:
        configured = self.config.get("ad_preview_formats")
        return list(configured) if configured else list(DEFAULT_AD_PREVIEW_FORMATS)

    @property
    def _accounts(self) -> list[str]:
        account_ids_str = self.config.get("account_ids", "")
        if account_ids_str:
            accounts = [aid.strip() for aid in account_ids_str.split(",") if aid.strip()]
        else:
            accounts = [self.config["account_id"]]
        return list(dict.fromkeys(accounts))

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.config['access_token']}"}

    def _request_with_retry(
        self,
        url: str,
        params: dict[str, t.Any],
    ) -> requests.Response | None:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    url,
                    headers=self._headers,
                    params=params,
                    timeout=60,
                )
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    raise
                sleep_for = min(BASE_BACKOFF_SECONDS ** attempt, 300)
                self.logger.warning(
                    "Network error on %s (attempt %d/%d): %s. Sleeping %ss.",
                    url, attempt, MAX_RETRIES, exc, sleep_for,
                )
                time.sleep(sleep_for)
                continue

            if resp.ok:
                return resp

            error = {}
            try:
                error = resp.json().get("error", {}) or {}
            except ValueError:
                pass
            code = error.get("code")
            is_transient = error.get("is_transient", False)
            retryable = (
                resp.status_code >= 500
                or resp.status_code == 429
                or is_transient
                or code in RETRYABLE_FACEBOOK_CODES
            )
            if retryable and attempt < MAX_RETRIES:
                sleep_for = min(BASE_BACKOFF_SECONDS ** attempt, 300)
                self.logger.warning(
                    "Retryable Facebook error %s (code=%s) on %s. "
                    "Sleeping %ss before retry %d/%d.",
                    resp.status_code, code, url, sleep_for, attempt, MAX_RETRIES,
                )
                time.sleep(sleep_for)
                continue
            return resp
        return None

    def _iter_ads(self, account_id: str) -> t.Iterator[dict]:
        url = f"{self.url_base}/act_{account_id}/ads"
        params: dict[str, t.Any] = {
            "fields": "id,name,effective_status",
            "limit": 200,
        }
        while True:
            resp = self._request_with_retry(url, params)
            if resp is None or not resp.ok:
                status = resp.status_code if resp is not None else "no response"
                self.logger.error(
                    "Failed to list ads for account %s (status=%s). Skipping account.",
                    account_id, status,
                )
                return
            payload = resp.json()
            data = payload.get("data", [])
            yield from data
            after = (
                payload.get("paging", {}).get("cursors", {}).get("after")
            )
            if not after or not data:
                return
            params = {
                "fields": "id,name,effective_status",
                "limit": 200,
                "after": after,
            }

    def _get_preview(self, ad_id: str, ad_format: str) -> dict | None:
        url = f"{self.url_base}/{ad_id}/previews"
        params = {"ad_format": ad_format}
        resp = self._request_with_retry(url, params)
        if resp is None or not resp.ok:
            status = resp.status_code if resp is not None else "no response"
            self.logger.warning(
                "Skipping preview for ad %s format %s (status=%s).",
                ad_id, ad_format, status,
            )
            return None
        data = resp.json().get("data", [])
        if not data:
            return None
        iframe_html = data[0].get("body", "") or ""
        match = IFRAME_SRC_RE.search(iframe_html)
        preview_url = unescape(match.group(1)) if match else None
        return {"preview_url": preview_url, "preview_iframe_html": iframe_html}

    def get_records(
        self,
        context: Context | None,  # noqa: ARG002
    ) -> t.Iterable[dict]:
        run_id = int(time.time() * 1000)
        ad_formats = self._ad_formats
        for account_id in self._accounts:
            self.logger.info(
                "Fetching ad previews for account %s (formats=%s)",
                account_id, ad_formats,
            )
            ad_count = 0
            for ad in self._iter_ads(account_id):
                ad_count += 1
                for ad_format in ad_formats:
                    preview = self._get_preview(ad["id"], ad_format)
                    if preview is None:
                        continue
                    yield {
                        "account_id": account_id,
                        "ad_id": ad["id"],
                        "ad_name": ad.get("name"),
                        "effective_status": ad.get("effective_status"),
                        "ad_format": ad_format,
                        "preview_url": preview["preview_url"],
                        "preview_iframe_html": preview["preview_iframe_html"],
                        "run_id": run_id,
                    }
            self.logger.info(
                "Processed %d ads for account %s", ad_count, account_id,
            )
