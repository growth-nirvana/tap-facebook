"""Stream class for ad creative image URLs.

Resolves displayable image URLs for each ad without using the Preview API.
One row is emitted per image (carousel/dynamic ads may produce multiple rows).
"""

from __future__ import annotations

import json
import time
import typing as t

import requests
from singer_sdk import typing as th
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, Stream

from tap_facebook.creative_image_resolution import (
    collect_image_hashes,
    resolve_creative_images,
)

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

CREATIVE_FIELDS = (
    "id,name,object_type,image_url,image_hash,thumbnail_url,video_id,"
    "object_story_spec,asset_feed_spec,effective_instagram_media_id,"
    "source_instagram_media_id"
)

AD_LIST_FIELDS = (
    "id,name,account_id,adset_id,campaign_id,effective_status,"
    "adset{id,name},campaign{id,name},creative{id}"
)

RETRYABLE_FACEBOOK_CODES = {1, 2, 4, 17, 32, 341, 368, 613}
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2


class AdCreativeImagesStream(Stream):
    """Resolved image URLs per ad, with campaign/adset/ad hierarchy."""

    name = "ad_creative_images"
    primary_keys = ("account_id", "ad_id", "image_index")  # noqa: RUF012
    replication_method = REPLICATION_FULL_TABLE

    schema = th.PropertiesList(
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("adset_id", th.StringType),
        th.Property("adset_name", th.StringType),
        th.Property("ad_id", th.StringType),
        th.Property("ad_name", th.StringType),
        th.Property("effective_status", th.StringType),
        th.Property("creative_id", th.StringType),
        th.Property("ad_type", th.StringType),
        th.Property("image_index", th.IntegerType),
        th.Property("image_hash", th.StringType),
        th.Property("image_url", th.StringType),
        th.Property("image_source", th.StringType),
        th.Property("image_width", th.IntegerType),
        th.Property("image_height", th.IntegerType),
        th.Property("is_primary", th.BooleanType),
        th.Property("run_id", th.IntegerType),
    ).to_dict()

    @property
    def url_base(self) -> str:
        return f"https://graph.facebook.com/{self.config['api_version']}"

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
                sleep_for = min(BASE_BACKOFF_SECONDS**attempt, 300)
                self.logger.warning(
                    "Network error on %s (attempt %d/%d): %s. Sleeping %ss.",
                    url,
                    attempt,
                    MAX_RETRIES,
                    exc,
                    sleep_for,
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
                sleep_for = min(BASE_BACKOFF_SECONDS**attempt, 300)
                self.logger.warning(
                    "Retryable Facebook error %s (code=%s) on %s. "
                    "Sleeping %ss before retry %d/%d.",
                    resp.status_code,
                    code,
                    url,
                    sleep_for,
                    attempt,
                    MAX_RETRIES,
                )
                time.sleep(sleep_for)
                continue
            return resp
        return None

    def _graph_get(self, path: str, params: dict[str, t.Any] | None = None) -> dict | None:
        resp = self._request_with_retry(f"{self.url_base}/{path.lstrip('/')}", params or {})
        if resp is None or not resp.ok:
            status = resp.status_code if resp is not None else "no response"
            self.logger.warning("Graph GET failed for %s (status=%s).", path, status)
            return None
        return resp.json()

    def _get_account_name(self, account_id: str) -> str | None:
        payload = self._graph_get(
            f"act_{account_id}",
            {"fields": "name,account_id"},
        )
        return payload.get("name") if payload else None

    def _iter_ads(self, account_id: str) -> t.Iterator[dict]:
        url = f"{self.url_base}/act_{account_id}/ads"
        params: dict[str, t.Any] = {
            "fields": AD_LIST_FIELDS,
            "limit": 200,
        }
        while True:
            resp = self._request_with_retry(url, params)
            if resp is None or not resp.ok:
                status = resp.status_code if resp is not None else "no response"
                self.logger.error(
                    "Failed to list ads for account %s (status=%s). Skipping account.",
                    account_id,
                    status,
                )
                return
            payload = resp.json()
            data = payload.get("data", [])
            yield from data
            after = payload.get("paging", {}).get("cursors", {}).get("after")
            if not after or not data:
                return
            params = {
                "fields": AD_LIST_FIELDS,
                "limit": 200,
                "after": after,
            }

    def _fetch_adimages_by_hash(
        self,
        account_id: str,
        hashes: list[str],
    ) -> dict[str, dict[str, t.Any]]:
        if not hashes:
            return {}
        payload = self._graph_get(
            f"act_{account_id}/adimages",
            {
                "hashes": json.dumps(hashes),
                "fields": "hash,url,width,height",
            },
        )
        if not payload:
            return {}
        return {row["hash"]: row for row in payload.get("data") or [] if row.get("hash")}

    def _fetch_ig_media(self, media_id: str) -> dict[str, t.Any] | None:
        return self._graph_get(
            media_id,
            {"fields": "id,media_type,media_url,thumbnail_url"},
        )

    def _fetch_creative(self, creative_id: str) -> dict[str, t.Any] | None:
        return self._graph_get(creative_id, {"fields": CREATIVE_FIELDS})

    def get_records(
        self,
        context: Context | None,  # noqa: ARG002
    ) -> t.Iterable[dict]:
        run_id = int(time.time() * 1000)
        for account_id in self._accounts:
            account_name = self._get_account_name(account_id)
            self.logger.info("Fetching ad creative images for account %s", account_id)
            ad_count = 0
            image_count = 0
            for ad in self._iter_ads(account_id):
                ad_count += 1
                creative_ref = ad.get("creative") or {}
                creative_id = creative_ref.get("id")
                if not creative_id:
                    continue

                creative = self._fetch_creative(creative_id)
                if not creative:
                    continue

                hashes = collect_image_hashes(creative)
                adimages_by_hash = self._fetch_adimages_by_hash(account_id, hashes)

                ig_media = None
                media_id = creative.get("effective_instagram_media_id") or creative.get(
                    "source_instagram_media_id"
                )
                if media_id:
                    ig_media = self._fetch_ig_media(media_id)

                adset = ad.get("adset") or {}
                campaign = ad.get("campaign") or {}
                for image in resolve_creative_images(
                    creative,
                    adimages_by_hash,
                    ig_media=ig_media,
                ):
                    image_count += 1
                    yield {
                        "account_id": account_id,
                        "account_name": account_name,
                        "campaign_id": ad.get("campaign_id") or campaign.get("id"),
                        "campaign_name": campaign.get("name"),
                        "adset_id": ad.get("adset_id") or adset.get("id"),
                        "adset_name": adset.get("name"),
                        "ad_id": ad.get("id"),
                        "ad_name": ad.get("name"),
                        "effective_status": ad.get("effective_status"),
                        "creative_id": creative_id,
                        "ad_type": image["ad_type"],
                        "image_index": image["image_index"],
                        "image_hash": image.get("image_hash"),
                        "image_url": image["image_url"],
                        "image_source": image["image_source"],
                        "image_width": image.get("image_width"),
                        "image_height": image.get("image_height"),
                        "is_primary": image["is_primary"],
                        "run_id": run_id,
                    }

            self.logger.info(
                "Processed %d ads and emitted %d images for account %s",
                ad_count,
                image_count,
                account_id,
            )
