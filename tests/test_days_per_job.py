"""Insights jobs covering several days (days_per_job) and splitting a job Meta finds too large."""

from __future__ import annotations

import logging

import pendulum
import pytest

from tap_facebook.streams.ad_insights import AdsInsightStream, InsightsJobTooLarge, job_windows

D = pendulum.date


def windows(*args):
    return [(a.to_date_string(), b.to_date_string()) for a, b in job_windows(*args)]


def test_one_day_per_job_is_the_default_behaviour():
    assert windows(D(2026, 10, 6), D(2026, 10, 8), 1, 1, False) == [
        ("2026-10-06", "2026-10-06"), ("2026-10-07", "2026-10-07"), ("2026-10-08", "2026-10-08"),
    ]


def test_days_per_job_covers_every_day_once():
    got = windows(D(2026, 10, 6), D(2026, 11, 5), 10, 1, False)
    assert got == [("2026-10-06", "2026-10-15"), ("2026-10-16", "2026-10-25"), ("2026-10-26", "2026-11-04"), ("2026-11-05", "2026-11-05")]


def test_days_per_job_rounds_down_to_whole_increments():
    assert windows(D(2026, 10, 5), D(2026, 10, 25), 10, 7, False) == [
        ("2026-10-05", "2026-10-11"), ("2026-10-12", "2026-10-18"), ("2026-10-19", "2026-10-25"),
    ]


def test_monthly_windows_are_calendar_months():
    assert windows(D(2026, 9, 1), D(2026, 11, 5), 1, 1, True) == [
        ("2026-09-01", "2026-09-30"), ("2026-10-01", "2026-10-31"), ("2026-11-01", "2026-11-05"),
    ]


class FakeJob:
    def __init__(self, since, until):
        self.since, self.until = since, until


def make_stream(too_large_over_days: int):
    stream = object.__new__(AdsInsightStream)
    stream._report_definition = {
        "level": "ad", "action_breakdowns": [], "action_report_time": "mixed", "breakdowns": ["age"],
        "action_attribution_windows_view": "1d_view", "action_attribution_windows_click": "7d_click",
    }
    stream.__dict__["logger"] = logging.getLogger("test.days_per_job")
    stream.jobs = []

    def run(params):
        since, until = params["time_range"]["since"], params["time_range"]["until"]
        stream.jobs.append((since, until, params["time_increment"]))
        if (pendulum.parse(until) - pendulum.parse(since)).in_days() + 1 > too_large_over_days:
            raise InsightsJobTooLarge("Please reduce the amount of data")
        return FakeJob(since, until)

    def results(job, since, until):
        day = pendulum.parse(since).date()
        while day <= pendulum.parse(until).date():
            yield {"date_start": day.to_date_string()}
            day = day.add(days=1)

    stream._run_job_to_completion = run
    stream._iter_job_results_with_retry = results
    return stream


def test_a_too_large_job_is_split_until_it_fits():
    stream = make_stream(too_large_over_days=3)
    rows = list(stream._fetch_window(D(2026, 10, 1), D(2026, 10, 10), ["spend"], time_increment=1, min_days=1))
    assert [r["date_start"] for r in rows] == [f"2026-10-{d:02d}" for d in range(1, 11)]  # every day once, in order
    assert all(inc == 1 for _, _, inc in stream.jobs)  # still daily rows
    assert stream.jobs[0] == ("2026-10-01", "2026-10-10", 1)


def test_a_single_day_that_is_too_large_fails_clearly():
    stream = make_stream(too_large_over_days=0)
    with pytest.raises(RuntimeError, match="2026-10-01 - 2026-10-01 failed"):
        list(stream._fetch_window(D(2026, 10, 1), D(2026, 10, 1), ["spend"], time_increment=1, min_days=1))
