"""Helpers for resolving ad creative image URLs."""

from __future__ import annotations

import typing as t


def classify_creative(creative: dict[str, t.Any]) -> str:
    spec = creative.get("object_story_spec") or {}
    if creative.get("video_id") or spec.get("video_data"):
        if spec.get("instagram_actor_id") or creative.get("effective_instagram_media_id"):
            return "VIDEO_INSTAGRAM"
        return "VIDEO"
    if spec.get("link_data", {}).get("child_attachments"):
        return "CAROUSEL"
    if creative.get("asset_feed_spec"):
        return "DYNAMIC_ASSET_FEED"
    if creative.get("effective_instagram_media_id") or creative.get(
        "source_instagram_media_id"
    ):
        return "INSTAGRAM_MEDIA"
    return (creative.get("object_type") or "UNKNOWN").upper()


def collect_image_hashes(creative: dict[str, t.Any]) -> list[str]:
    spec = creative.get("object_story_spec") or {}
    link_data = spec.get("link_data") or {}
    hashes: list[str] = []

    def add(value: str | None) -> None:
        if value and value not in hashes:
            hashes.append(value)

    add(link_data.get("image_hash"))
    for attachment in link_data.get("child_attachments") or []:
        add(attachment.get("image_hash"))
    for image in (creative.get("asset_feed_spec") or {}).get("images") or []:
        add(image.get("hash"))
    add(creative.get("image_hash"))
    return hashes


def _score_image_source(source: str, url: str, width: int | None = None) -> int:
    score = width or 0
    if "p64x64" in url:
        score -= 1_000_000
    if "url_128" in source:
        score -= 1_000_000
    if source == "creative.thumbnail_url":
        score -= 500_000
    if source.startswith("adimages.url["):
        score += 100_000
    if source == "creative.image_url":
        score += 90_000
    if source == "object_story_spec.video_data.image_url":
        score += 80_000
    if source == "ig_media.thumbnail_url":
        score += 70_000
    return score


def resolve_creative_images(
    creative: dict[str, t.Any],
    adimages_by_hash: dict[str, dict[str, t.Any]],
    *,
    ig_media: dict[str, t.Any] | None = None,
) -> list[dict[str, t.Any]]:
    """Return one record payload per resolved image for a creative."""
    ad_type = classify_creative(creative)
    spec = creative.get("object_story_spec") or {}
    link_data = spec.get("link_data") or {}
    video_data = spec.get("video_data") or {}

    resolved: list[dict[str, t.Any]] = []
    seen_urls: set[str] = set()

    def append_image(
        *,
        image_index: int,
        image_hash: str | None,
        image_url: str | None,
        image_source: str,
        image_width: int | None = None,
        image_height: int | None = None,
        is_primary: bool = False,
    ) -> None:
        if not image_url or image_url in seen_urls:
            return
        if image_url.endswith(".mp4") or "/m366/" in image_url:
            return
        seen_urls.add(image_url)
        resolved.append(
            {
                "image_index": image_index,
                "image_hash": image_hash,
                "image_url": image_url,
                "image_source": image_source,
                "image_width": image_width,
                "image_height": image_height,
                "ad_type": ad_type,
                "is_primary": is_primary,
            }
        )

    hashes = collect_image_hashes(creative)
    for index, image_hash in enumerate(hashes):
        adimage = adimages_by_hash.get(image_hash) or {}
        append_image(
            image_index=index,
            image_hash=image_hash,
            image_url=adimage.get("url"),
            image_source=f"adimages.url[{image_hash}]",
            image_width=adimage.get("width"),
            image_height=adimage.get("height"),
            is_primary=index == 0,
        )

    if not resolved:
        if creative.get("image_url"):
            append_image(
                image_index=0,
                image_hash=creative.get("image_hash"),
                image_url=creative.get("image_url"),
                image_source="creative.image_url",
                is_primary=True,
            )

        video_poster = video_data.get("image_url")
        if video_poster:
            append_image(
                image_index=0,
                image_hash=None,
                image_url=video_poster,
                image_source="object_story_spec.video_data.image_url",
                is_primary=True,
            )

        if ig_media:
            append_image(
                image_index=0,
                image_hash=None,
                image_url=ig_media.get("thumbnail_url"),
                image_source="ig_media.thumbnail_url",
                is_primary=True,
            )
            media_url = ig_media.get("media_url")
            if media_url and ig_media.get("media_type") != "VIDEO":
                append_image(
                    image_index=0,
                    image_hash=None,
                    image_url=media_url,
                    image_source="ig_media.media_url",
                    is_primary=True,
                )

        if not resolved and creative.get("thumbnail_url"):
            append_image(
                image_index=0,
                image_hash=creative.get("thumbnail_id"),
                image_url=creative.get("thumbnail_url"),
                image_source="creative.thumbnail_url",
                is_primary=True,
            )

    if len(resolved) > 1:
        best = max(
            resolved,
            key=lambda row: _score_image_source(
                row["image_source"],
                row["image_url"],
                row.get("image_width"),
            ),
        )
        for row in resolved:
            row["is_primary"] = row is best

    return resolved
