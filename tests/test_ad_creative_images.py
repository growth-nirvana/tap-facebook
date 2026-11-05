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


# --- The stream reads in bulk and emits the same rows as reading one ad at a time ---

import json  # noqa: E402
import urllib.parse  # noqa: E402

import pytest  # noqa: E402

from tap_facebook.creative_image_resolution import collect_image_hashes  # noqa: E402
from tap_facebook.streams import ad_creative_images as aci  # noqa: E402

CREATIVES = {
    "c_img": {"id": "c_img", "object_type": "SHARE", "object_story_spec": {"link_data": {"image_hash": "h1"}}},
    "c_car": {
        "id": "c_car", "object_type": "SHARE",
        "object_story_spec": {"link_data": {"image_hash": "h2", "child_attachments": [
            {"image_hash": "h3"}, {"image_hash": "h4"}, {"image_hash": "h2"}]}},
    },
    "c_vid": {
        "id": "c_vid", "object_type": "VIDEO", "video_id": "v1", "effective_instagram_media_id": "ig1",
        "thumbnail_url": "https://cdn.example/p64x64/v1.jpg",
        "object_story_spec": {"video_data": {"video_id": "v1", "image_url": "https://cdn.example/poster1.jpg"}},
    },
    "c_vid_thumb": {
        "id": "c_vid_thumb", "object_type": "VIDEO", "video_id": "v2", "thumbnail_url": "https://cdn.example/t2.jpg",
        "object_story_spec": {"video_data": {"video_id": "v2"}},
    },
    "c_ig_img": {"id": "c_ig_img", "object_type": "PHOTO", "source_instagram_media_id": "ig2"},
    "c_ig_gone": {"id": "c_ig_gone", "object_type": "PHOTO", "source_instagram_media_id": "ig_gone",
                  "thumbnail_url": "https://cdn.example/t_gone.jpg"},
    "c_hash_missing": {"id": "c_hash_missing", "object_type": "SHARE", "image_hash": "h9",
                       "image_url": "https://cdn.example/fallback.jpg"},
    "c_dyn": {"id": "c_dyn", "object_type": "SHARE", "effective_instagram_media_id": "ig3",
              "asset_feed_spec": {"images": [{"hash": "h5"}, {"hash": "h6"}]}},
    "c_mp4": {"id": "c_mp4", "object_type": "SHARE", "object_story_spec": {"link_data": {"image_hash": "h7"}},
              "thumbnail_url": "https://cdn.example/t7.jpg"},
}
ADIMAGES = {
    "h1": {"hash": "h1", "url": "https://cdn.example/1.jpg", "width": 1080, "height": 1080},
    "h2": {"hash": "h2", "url": "https://cdn.example/2.jpg", "width": 600, "height": 600},
    "h3": {"hash": "h3", "url": "https://cdn.example/3.jpg", "width": 1200, "height": 1200},
    "h4": {"hash": "h4", "url": "https://cdn.example/4.jpg", "width": 800, "height": 800},
    "h5": {"hash": "h5", "url": "https://cdn.example/5.jpg", "width": 500, "height": 500},
    "h6": {"hash": "h6", "url": "https://cdn.example/6.jpg", "width": 900, "height": 900},
    "h7": {"hash": "h7", "url": "https://cdn.example/7.mp4", "width": 100, "height": 100},
}
IG_MEDIA = {
    "ig1": {"id": "ig1", "media_type": "VIDEO", "media_url": "https://cdn.example/ig1.mp4",
            "thumbnail_url": "https://cdn.example/ig1_thumb.jpg"},
    "ig2": {"id": "ig2", "media_type": "IMAGE", "media_url": "https://cdn.example/ig2.jpg"},
    "ig3": {"id": "ig3", "media_type": "IMAGE", "media_url": "https://cdn.example/ig3.jpg"},
}
UNREADABLE = {"c_gone", "ig_gone"}


def ad(n, creative_id):
    row = {"id": f"ad{n}", "name": f"Ad {n}", "account_id": "1", "adset_id": f"as{n % 2}",
           "campaign_id": "cp1", "effective_status": "ACTIVE",
           "adset": {"id": f"as{n % 2}", "name": f"Adset {n % 2}"}, "campaign": {"id": "cp1", "name": "Campaign"}}
    if creative_id:
        row["creative"] = {"id": creative_id}
    return row


ADS = [ad(i, c) for i, c in enumerate([
    "c_img", "c_car", "c_vid", "c_car", "c_vid_thumb", "c_ig_img", "c_ig_gone", "c_hash_missing",
    "c_dyn", "c_mp4", "c_gone", None, "c_car", "c_img",
])]


class FakeResponse:
    def __init__(self, status, payload):
        self.status_code, self._payload = status, payload
        self.ok = status < 400

    def json(self):
        return self._payload


ERROR = FakeResponse(400, {"error": {"code": 100, "message": "Unsupported get request."}})


class FakeGraph:
    """Answers the Graph reads the stream makes, from the fixtures above."""

    def __init__(self):
        self.calls = []

    def __call__(self, url, params):
        path = urllib.parse.urlparse(url).path.split("/", 2)[2] if url.count("/") > 3 else ""
        self.calls.append((path, dict(params)))
        nodes = {**CREATIVES, **IG_MEDIA}
        if path == "act_1":
            return FakeResponse(200, {"name": "Account One", "account_id": "1", "id": "act_1"})
        if path == "act_1/ads":
            return self._page(ADS, params, default_limit=25)
        if path == "act_1/adimages":
            wanted = json.loads(params["hashes"])
            return self._page([ADIMAGES[h] for h in wanted if h in ADIMAGES], params, default_limit=25)
        if path == "":
            ids = params["ids"].split(",")
            if any(i in UNREADABLE or i not in nodes for i in ids):
                return ERROR
            return FakeResponse(200, {i: nodes[i] for i in ids})
        return FakeResponse(200, nodes[path]) if path in nodes else ERROR

    @staticmethod
    def _page(rows, params, default_limit):
        limit, start = int(params.get("limit", default_limit)), int(params.get("after", 0))
        page = rows[start : start + limit]
        payload = {"data": page}
        if start + limit < len(rows):
            payload["paging"] = {"cursors": {"after": str(start + limit)}, "next": "https://next"}
        return FakeResponse(200, payload)


def make_stream(graph):
    from tap_facebook.tap import TapFacebook

    tap = TapFacebook(config={"access_token": "x", "account_ids": "1", "start_date": "2026-10-01T00:00:00Z"},
                      parse_env_config=False)
    stream = aci.AdCreativeImagesStream(tap=tap)
    stream._request_with_retry = lambda url, params: graph(url, params)
    return stream


def legacy_rows(stream):
    """The stream before bulk reads (v1.5.0): one creative, adimages and IG read per ad."""
    account_name = stream._get_account_name("1")
    for ad_row in stream._iter_ads("1"):
        creative_id = (ad_row.get("creative") or {}).get("id")
        if not creative_id:
            continue
        creative = stream._graph_get(creative_id, {"fields": aci.CREATIVE_FIELDS})
        if not creative:
            continue
        hashes = collect_image_hashes(creative)
        adimages_by_hash = {}
        if hashes:
            payload = stream._graph_get("act_1/adimages", {"hashes": json.dumps(hashes), "fields": aci.ADIMAGE_FIELDS})
            adimages_by_hash = {r["hash"]: r for r in (payload or {}).get("data") or [] if r.get("hash")}
        ig_media = None
        media_id = creative.get("effective_instagram_media_id") or creative.get("source_instagram_media_id")
        if media_id:
            ig_media = stream._graph_get(media_id, {"fields": aci.IG_MEDIA_FIELDS})
        adset, campaign = ad_row.get("adset") or {}, ad_row.get("campaign") or {}
        for image in resolve_creative_images(creative, adimages_by_hash, ig_media=ig_media):
            yield {
                "account_id": "1", "account_name": account_name,
                "campaign_id": ad_row.get("campaign_id") or campaign.get("id"), "campaign_name": campaign.get("name"),
                "adset_id": ad_row.get("adset_id") or adset.get("id"), "adset_name": adset.get("name"),
                "ad_id": ad_row.get("id"), "ad_name": ad_row.get("name"),
                "effective_status": ad_row.get("effective_status"), "creative_id": creative_id,
                "ad_type": image["ad_type"], "image_index": image["image_index"],
                "image_hash": image.get("image_hash"), "image_url": image["image_url"],
                "image_source": image["image_source"], "image_width": image.get("image_width"),
                "image_height": image.get("image_height"), "is_primary": image["is_primary"],
            }


@pytest.fixture(params=[(500, 50), (3, 2)], ids=["default-sizes", "tiny-pages-and-chunks"])
def sizes(request, monkeypatch):
    ads_page, per_request = request.param
    monkeypatch.setattr(aci, "ADS_PAGE_SIZE", ads_page)
    monkeypatch.setattr(aci, "NODES_PER_REQUEST", per_request)
    return request.param


def new_rows(stream):
    rows = list(stream.get_records(None))
    assert len({r["run_id"] for r in rows}) == 1
    return [{k: v for k, v in r.items() if k != "run_id"} for r in rows]


def test_bulk_rows_equal_one_ad_at_a_time(sizes):
    stream = make_stream(FakeGraph())
    assert new_rows(stream) == list(legacy_rows(stream))


def test_bulk_rows_cover_carousel_video_and_hash_cases(sizes):
    rows = new_rows(make_stream(FakeGraph()))
    by_ad = {}
    for r in rows:
        by_ad.setdefault(r["ad_id"], []).append((r["image_index"], r["image_hash"], r["image_source"], r["is_primary"]))
    assert set(rows[0]) == set(aci.AdCreativeImagesStream.schema["properties"]) - {"run_id"}
    assert by_ad["ad0"] == by_ad["ad13"] == [(0, "h1", "adimages.url[h1]", True)]
    carousel = [(0, "h2", "adimages.url[h2]", False), (1, "h3", "adimages.url[h3]", True),
                (2, "h4", "adimages.url[h4]", False)]
    assert by_ad["ad1"] == by_ad["ad3"] == by_ad["ad12"] == carousel  # shared creative, every ad gets its rows
    assert by_ad["ad2"] == [(0, None, "object_story_spec.video_data.image_url", True),
                            (0, None, "ig_media.thumbnail_url", False)]
    assert by_ad["ad4"] == [(0, None, "creative.thumbnail_url", True)]
    assert by_ad["ad5"] == [(0, None, "ig_media.media_url", True)]
    assert by_ad["ad6"] == [(0, None, "creative.thumbnail_url", True)]  # its IG media cannot be read
    assert by_ad["ad7"] == [(0, "h9", "creative.image_url", True)]  # hash unknown to adimages
    assert by_ad["ad8"] == [(0, "h5", "adimages.url[h5]", False), (1, "h6", "adimages.url[h6]", True)]
    assert by_ad["ad9"] == [(0, None, "creative.thumbnail_url", True)]  # adimage is an .mp4
    assert "ad10" not in by_ad and "ad11" not in by_ad  # unreadable creative, no creative


def test_each_creative_hash_and_media_is_read_once():
    graph = FakeGraph()
    new_rows(make_stream(graph))
    paths = [p for p, _ in graph.calls]
    assert paths.count("act_1/ads") == 1
    assert paths.count("act_1/adimages") == 1
    bulk = [params["ids"].split(",") for p, params in graph.calls if p == ""]
    creative_ids = list(dict.fromkeys(a["creative"]["id"] for a in ADS if a.get("creative")))
    assert bulk[0] == creative_ids  # one read for every distinct creative (it fails: c_gone)
    # ...so they are read one at a time; then IG media only where no hash resolved (not ig3).
    singles = [p for p in paths if p not in ("", "act_1", "act_1/ads", "act_1/adimages")]
    assert singles == [*creative_ids, "ig1", "ig2", "ig_gone"]  # ig_gone fails the IG chunk too
    assert bulk[1] == ["ig1", "ig2", "ig_gone"]
    assert len(graph.calls) < 2 * len(ADS)


def test_a_readable_chunk_is_one_call(monkeypatch):
    graph = FakeGraph()
    stream = make_stream(graph)
    nodes = stream._fetch_nodes(["c_img", "c_car", "c_img", None], aci.CREATIVE_FIELDS)
    assert list(nodes) == ["c_img", "c_car"]
    assert graph.calls == [("", {"ids": "c_img,c_car", "fields": aci.CREATIVE_FIELDS})]


def test_adimages_reads_every_page():
    graph = FakeGraph()
    stream = make_stream(graph)
    got = stream._fetch_adimages_by_hash("1", list(ADIMAGES) * 2)
    assert got == ADIMAGES
