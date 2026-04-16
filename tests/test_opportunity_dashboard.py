"""OpportunityDashboard tick-driven state (no time-based inactive)."""

import pytest

import pcp_arbitrage.opportunity_dashboard as opportunity_dashboard_module
from pcp_arbitrage.models import Triplet
from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard
from pcp_arbitrage.pcp_calculator import ArbitrageSignal


def _t() -> Triplet:
    return Triplet(
        exchange="okx",
        symbol="BTC",
        expiry="261225",
        strike=56000.0,
        call_id="c",
        put_id="p",
        future_id="f",
    )


def _sig(triplet: Triplet, *, ann: float = 0.10) -> ArbitrageSignal:
    return ArbitrageSignal(
        direction="forward",
        triplet=triplet,
        call_price=1.0,
        put_price=1.0,
        future_price=1.0,
        call_price_coin=0.01,
        put_price_coin=0.01,
        spot_price=60000.0,
        gross_profit=100.0,
        total_fee=1.49,
        net_profit=246.19,
        annualized_return=ann,
        days_to_expiry=264.0,
    )


def test_record_qualified_then_none_is_inactive():
    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.10), 0.01)
    lines = d.render_lines_for_test()
    assert any("active" in line for line in lines)
    d.record_evaluation("okx", t, "forward", None, 0.01)
    lines = d.render_lines_for_test()
    assert any("inactive" in line for line in lines)


def test_record_qualified_then_below_threshold_is_inactive():
    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "reverse", _sig(t, ann=0.10), 0.01)
    d.record_evaluation("okx", t, "reverse", _sig(t, ann=0.001), 0.01)
    lines = d.render_lines_for_test()
    assert any("inactive" in line for line in lines)


def test_no_row_until_first_qualified():
    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "forward", None, 0.01)
    lines = d.render_lines_for_test()
    assert len(lines) == 6  # title + 2 subtitles + sep + header + sep, no data rows
    assert not any("okx" in line for line in lines)


def test_reactivate_resets_duration_tracking():
    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.10), 0.01)
    d.record_evaluation("okx", t, "forward", None, 0.01)
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.10), 0.01)
    lines = d.render_lines_for_test()
    assert any("active" in line for line in lines)


def test_inactive_preserves_last_active_duration(monkeypatch):
    """Duration column shows frozen active span after inactive, not '—'."""
    # __init__ + 两次 record_evaluation + _format_lines 各消耗一次 time.time
    seq = iter([1000.0, 1000.0, 1123.45, 1123.45])

    def _fake_time() -> float:
        try:
            return next(seq)
        except StopIteration:
            return 1123.45

    monkeypatch.setattr(opportunity_dashboard_module.time, "time", _fake_time)

    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "forward", _sig(t), 0.01)
    d.record_evaluation("okx", t, "forward", None, 0.01)
    row = next(iter(d._rows.values()))
    assert row.frozen_active_duration_sec == pytest.approx(123.45)
    lines = d.render_lines_for_test()
    data = [ln for ln in lines if "okx" in ln][0]
    assert "123450ms" in data  # 123.45s frozen duration, formatted as ms in ann/duration cells


def test_max_ann_pct_tracks_peak_across_ticks():
    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.10), 0.01)
    r = next(iter(d._rows.values()))
    assert r.max_ann_pct == pytest.approx(10.0)
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.20), 0.01)
    r = next(iter(d._rows.values()))
    assert r.max_ann_pct == pytest.approx(20.0)
    # 低于阈值仍有信号：历史峰值仍可被更高但未达阈值的年化刷新
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.25), 0.30)
    r = next(iter(d._rows.values()))
    assert r.active is False
    assert r.max_ann_pct == pytest.approx(20.0)


def test_inactive_freezes_values_until_reactivated():
    d = OpportunityDashboard(max_rows=10)
    t = _t()
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.10), 0.01)
    r = next(iter(d._rows.values()))
    gross_active = r.gross
    fee_active = r.fee
    ann_active = r.ann_pct
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.001), 0.01)  # turn inactive
    r = next(iter(d._rows.values()))
    assert r.active is False
    assert r.gross == pytest.approx(gross_active)
    assert r.fee == pytest.approx(fee_active)
    assert r.ann_pct == pytest.approx(ann_active)

    # still inactive: below-threshold ticks should no longer mutate values
    sig2 = _sig(t, ann=0.002)
    sig2.gross_profit = 777.0
    sig2.total_fee = 9.99
    sig2.net_profit = 666.0
    d.record_evaluation("okx", t, "forward", sig2, 0.01)
    r = next(iter(d._rows.values()))
    assert r.active is False
    assert r.gross == pytest.approx(gross_active)
    assert r.fee == pytest.approx(fee_active)
    assert r.ann_pct == pytest.approx(ann_active)

    # qualified again -> reactivated and values refresh
    sig3 = _sig(t, ann=0.30)
    sig3.gross_profit = 888.0
    sig3.total_fee = 8.88
    sig3.net_profit = 700.0
    d.record_evaluation("okx", t, "forward", sig3, 0.01)
    r = next(iter(d._rows.values()))
    assert r.active is True
    assert r.gross == pytest.approx(888.0)
    assert r.fee == pytest.approx(8.88)
    assert r.ann_pct == pytest.approx(30.0)


def test_near_duplicate_strikes_merge_to_one_row():
    """行权价浮点差异但展示合约相同（如 74500 vs 74500+ε）时只保留一行，避免重复日志/会话。"""
    d = OpportunityDashboard(max_rows=10)
    t1 = Triplet(
        exchange="okx",
        symbol="BTC",
        expiry="260417",
        strike=74500.0,
        call_id="c1",
        put_id="p1",
        future_id="f1",
    )
    t2 = Triplet(
        exchange="okx",
        symbol="BTC",
        expiry="260417",
        strike=74500.00000000093,
        call_id="c1",
        put_id="p1",
        future_id="f1",
    )
    d.record_evaluation("okx", t1, "reverse", _sig(t1, ann=0.10), 0.01)
    d.record_evaluation("okx", t2, "reverse", _sig(t2, ann=0.10), 0.01)
    assert len(d._rows) == 1


def test_inactive_edge_immediate_upsert_current(monkeypatch):
    calls = []

    def _fake_upsert(path, rows):
        calls.append((path, len(rows), rows[0].active))

    monkeypatch.setattr(
        "pcp_arbitrage.db.upsert_opportunity_current",
        _fake_upsert,
    )
    d = OpportunityDashboard(max_rows=10, sqlite_path="/tmp/fake.db")
    t = _t()
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.10), 0.01)
    d.record_evaluation("okx", t, "forward", _sig(t, ann=0.001), 0.01)
    assert calls, "expected immediate upsert on active->inactive edge"
    path, nrows, active = calls[-1]
    assert path == "/tmp/fake.db"
    assert nrows == 1
    assert active is False
