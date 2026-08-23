"""Tests for ad creative image resolution helpers."""

from tap_facebook.creative_image_resolution import (
    classify_creative,
    resolve_creative_images,
)


def test_classify_carousel_creative():
    creative = {
        "object_type": "SHARE",
        "object_story_spec": {
            "link_data": {
                "child_attachments": [{"image_hash": "abc"}],
            },
        },
    }
    assert classify_creative(creative) == "CAROUSEL"


def test_resolve_carousel_images_from_adimages():
    creative = {
        "object_type": "SHARE",
        "object_story_spec": {
            "link_data": {
                "image_hash": "hash1",
                "child_attachments": [{"image_hash": "hash2"}],
            },
        },
    }
    adimages_by_hash = {
        "hash1": {"hash": "hash1", "url": "https://cdn.example/1.jpg", "width": 1080, "height": 1080},
        "hash2": {"hash": "hash2", "url": "https://cdn.example/2.jpg", "width": 1080, "height": 1080},
    }

    rows = resolve_creative_images(creative, adimages_by_hash)

    assert len(rows) == 2
    assert rows[0]["image_hash"] == "hash1"
    assert rows[0]["is_primary"] is True
    assert rows[1]["image_hash"] == "hash2"


def test_resolve_video_uses_ig_thumbnail_when_no_hashes():
    creative = {
        "object_type": "VIDEO",
        "video_id": "123",
        "effective_instagram_media_id": "999",
        "object_story_spec": {"video_data": {"video_id": "123"}},
    }

    rows = resolve_creative_images(
        creative,
        {},
        ig_media={
            "media_type": "VIDEO",
            "media_url": "https://cdn.example/video.mp4",
            "thumbnail_url": "https://cdn.example/poster.jpg",
        },
    )

    assert len(rows) == 1
    assert rows[0]["image_source"] == "ig_media.thumbnail_url"
    assert rows[0]["image_url"] == "https://cdn.example/poster.jpg"
