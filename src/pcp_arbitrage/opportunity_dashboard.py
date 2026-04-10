"""Terminal table of arbitrage opportunities (tick-driven active/inactive state)."""

from __future__ import annotations

import datetime
import logging
import math
import time
from dataclasses import dataclass

from pcp_arbitrage.exchange_symbols import format_strike_display
from pcp_arbitrage.models import FeeRates, Triplet
from pcp_arbitrage.pcp_calculator import (
    ArbitrageSignal,
    inverse_future_usd_face_for_exchange,
    per_leg_fees_from_stored_leg_px,
)
from rich import box
from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)


@dataclass
class _RunnerMeta:
    option_taker_rate: float
    option_maker_rate: float
    future_taker_rate: float
    future_maker_rate: float
    n_triplets: int
    settle_type: str = ""   # e.g. "币本位(USD)" / "U本位(USDT)" / "U本位(USDC)"
    symbols: list[str] = None  # type: ignore[assignment]
    triplets: list = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.symbols is None:
            self.symbols = []
        if self.triplets is None:
            self.triplets = []


@dataclass
class _Row:
    exchange: str
    label: str
    direction_cn: str
    active: bool
    first_active: float
    last_eval: float
    gross: float  # gross_profit（扣费前，USDT）
    days_to_expiry: float  # 距到期天数（来自信号）
    ann_pct: float
    max_ann_pct: float  # 本组合历史上出现过的最高年化%（有信号即参与比较）
    net: float
    fee: float
    tradeable: float  # 三腿对手盘挂量最小（与交易所深度口径一致）
    last_active_eval: float | None = None
    strike: float | None = None
    call_px_usdt: float | None = None
    put_px_usdt: float | None = None
    fut_px_usdt: float | None = None
    call_fee: float | None = None   # 单边 call 手续费 (USDT)
    put_fee: float | None = None    # 单边 put 手续费 (USDT)
    fut_fee: float | None = None    # 单边 future 手续费 (USDT)
    # 最近一次从 active 变 inactive 时冻结的「持续活跃时长」（秒）；active 时为 None
    frozen_active_duration_sec: float | None = None
    # opportunity_sessions.id while active; None after close or before first persist
    active_session_id: int | None = None


@dataclass
class _TripletSnap:
    """Per-triplet book snapshot + ann for web dashboard triplets table."""
    exchange: str
    symbol: str
    expiry: str
    strike: float
    call_id: str
    put_id: str
    future_id: str
    days_to_expiry: float
    # raw coin-denominated prices (None = no data yet)
    call_bid: float | None = None
    call_ask: float | None = None
    put_bid: float | None = None
    put_ask: float | None = None
    fut_bid: float | None = None
    fut_ask: float | None = None
    fwd_ann_pct: float | None = None   # None = no profitable signal
    rev_ann_pct: float | None = None
    updated_at: float = 0.0


def _fmt_uptime_segment(seconds: float) -> str:
    """进程已运行时长用分段（<1h）：秒/分，与机会列表的毫秒时长区分。"""
    if seconds < 1:
        return "—"
    if seconds < 60:
        return f"{int(seconds)}s"
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m}m{s:02d}s"


def _fmt_duration_ms(seconds: float) -> str:
    """机会持续时长，总毫秒数。"""
    if not math.isfinite(seconds) or seconds < 0:
        return "—"
    return f"{int(round(seconds * 1000))}ms"


def _fmt_uptime(seconds: float) -> str:
    """进程已运行时长（仪表盘创建时刻起算）。"""
    if seconds < 0:
        seconds = 0
    if seconds < 3600:
        return _fmt_uptime_segment(seconds)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h{m:02d}m{s:02d}s"


def _direction_cn(direction: str) -> str:
    return "正向" if direction == "forward" else "反向"


def _fmt_days_to_expiry(days: float) -> str:
    return f"{days:.1f}天"


def _duration_cell(r: _Row, now: float) -> str:
    """Duration: live while active; frozen after last transition to inactive."""
    if r.active:
        return _fmt_duration_ms(now - r.first_active)
    if r.frozen_active_duration_sec is not None:
        return _fmt_duration_ms(r.frozen_active_duration_sec)
    return "—"


def duration_sec_for_storage(r: _Row, now: float) -> float | None:
    """Seconds aligned with _duration_cell; None if unknown (matches '—')."""
    if r.active:
        d = now - r.first_active
        return d if d >= 1 else None
    if r.frozen_active_duration_sec is not None:
        return r.frozen_active_duration_sec if r.frozen_active_duration_sec >= 1 else None
    return None


def _ann_dur_cell(ann_pct: float, dur_str: str) -> str:
    return f"{ann_pct:.2f}% / {dur_str}"


class OpportunityDashboard:
    """Keeps one row per (exchange, triplet, direction); inactive only on re-eval that fails threshold."""

    def __init__(
        self,
        max_rows: int = 30,
        *,
        min_annualized_rate: float = 0.0,
        atm_range: float = 0.0,
        min_days_to_expiry: float = 0.0,
        symbols: list[str] | None = None,
        sqlite_path: str | None = None,
    ) -> None:
        self._max_rows = max(1, max_rows)
        self._min_annualized_rate = min_annualized_rate
        self._atm_range = atm_range
        self._min_days_to_expiry = min_days_to_expiry
        self._symbols = list(symbols) if symbols else []
        self._sqlite_path = sqlite_path
        self._runner_meta: dict[str, _RunnerMeta] = {}
        self._rows: dict[tuple[str, str, str, float, str], _Row] = {}
        # per-triplet live snapshot for web dashboard: key=(exchange,symbol,expiry,strike)
        self._triplet_snaps: dict[tuple[str, str, str, float], _TripletSnap] = {}
        self._started_at = time.time()
        # {symbol: price} — updated by each exchange runner via update_index_price()
        self._index_prices: dict[str, float] = {}
        # optional asyncio.Queue for event-driven price push to web dashboard
        self._price_event_queue: object = None  # set to asyncio.Queue by web_dashboard
        # Restored-from-SQLite rows that were active: re-checked once after WS books populate.
        self._startup_revalidate_keys: set[tuple[str, str, str, float, str]] = set()

    def note_startup_revalidate_key(self, key: tuple[str, str, str, float, str]) -> None:
        self._startup_revalidate_keys.add(key)

    def clear_startup_revalidate_key(
        self, exchange: str, triplet: Triplet, direction: str
    ) -> None:
        self._startup_revalidate_keys.discard(
            (exchange, triplet.symbol, triplet.expiry, triplet.strike, direction)
        )

    def finalize_startup_revalidate_stale(self) -> None:
        """Mark still-active restored rows inactive if no full book re-evaluation ran for them."""
        now = time.time()
        for key in list(self._startup_revalidate_keys):
            row = self._rows.get(key)
            self._startup_revalidate_keys.discard(key)
            if row is None or not row.active:
                continue
            logger.info(
                "[startup] restored active row had no book re-check — marking inactive: %s",
                key,
            )
            row.last_eval = now
            row.frozen_active_duration_sec = now - row.first_active
            self._end_opportunity_session(row, now)
            row.active = False

    def recalculate_stored_leg_fees(self, lot_size_by_symbol: dict[str, float]) -> int:
        """
        启动后：用 opportunity_current 里已存的 call/put/fut_px_usdt + 各 runner 费率 + 指数价，
        按与实时信号相同的公式重算三腿单边手续费，并更新 fee、net；可选写回 SQLite。
        """
        n = 0
        for row in self._rows.values():
            meta = self._runner_meta.get(row.exchange)
            if meta is None:
                continue
            label = row.label or ""
            sym = label.split("-")[0].strip() if label else ""
            if not sym:
                continue
            try:
                lot = float(lot_size_by_symbol.get(sym, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            if lot <= 0:
                continue
            cpx, ppx, fpx = row.call_px_usdt, row.put_px_usdt, row.fut_px_usdt
            if cpx is None or ppx is None or fpx is None:
                continue
            if not all(math.isfinite(float(x)) for x in (cpx, ppx, fpx)):
                continue
            idx = self._index_prices.get(sym)
            if idx is None or not math.isfinite(idx) or idx <= 0:
                sk = row.strike
                if sk is not None and math.isfinite(float(sk)) and float(sk) > 0:
                    idx = float(sk)
                else:
                    continue
            else:
                idx = float(idx)
            dir_key = "forward" if row.direction_cn == "正向" else "reverse"
            fr = FeeRates(
                option_taker_rate=meta.option_taker_rate,
                option_maker_rate=meta.option_maker_rate,
                future_taker_rate=meta.future_taker_rate,
                future_maker_rate=meta.future_maker_rate,
            )
            face = inverse_future_usd_face_for_exchange(
                row.exchange, sym, meta.settle_type
            )
            triple = per_leg_fees_from_stored_leg_px(
                dir_key,
                float(cpx),
                float(ppx),
                float(fpx),
                lot_size=lot,
                fee_rates=fr,
                index_for_fee=idx,
                future_inverse_usd_face=face,
            )
            if triple is None:
                continue
            cf, pf, ff = triple
            row.call_fee = cf
            row.put_fee = pf
            row.fut_fee = ff
            row.fee = 2.0 * (cf + pf + ff)
            row.net = row.gross - row.fee
            n += 1
        if n and self._sqlite_path:
            try:
                from pcp_arbitrage.db import upsert_opportunity_current

                upsert_opportunity_current(self._sqlite_path, list(self._rows.values()))
            except Exception as exc:
                logger.warning("[startup] upsert after fee recalc failed: %s", exc)
        if n:
            logger.info(
                "[startup] Recalculated per-leg fees from stored leg prices for %d rows",
                n,
            )
        return n

    def update_index_price(self, symbol: str, price: float) -> None:
        self._index_prices[symbol] = price
        q = self._price_event_queue
        if q is not None:
            try:
                q.put_nowait({"prices": {symbol: price}})
            except Exception:
                pass

    def set_runner_meta(
        self,
        exchange: str,
        *,
        option_taker_rate: float,
        option_maker_rate: float,
        future_taker_rate: float,
        future_maker_rate: float,
        n_triplets: int,
        settle_type: str = "",
        symbols: list[str] | None = None,
        triplets: list | None = None,
    ) -> None:
        self._runner_meta[exchange] = _RunnerMeta(
            option_taker_rate=option_taker_rate,
            option_maker_rate=option_maker_rate,
            future_taker_rate=future_taker_rate,
            future_maker_rate=future_maker_rate,
            n_triplets=n_triplets,
            settle_type=settle_type,
            symbols=list(symbols) if symbols else [],
            triplets=list(triplets) if triplets else [],
        )

    def get_all_triplets(self) -> dict[str, tuple[list, str]]:
        """Return {exchange: (triplets, settle_type)} for all known runners."""
        return {
            ex: (list(m.triplets), m.settle_type)
            for ex, m in self._runner_meta.items()
        }

    def get_triplet_snaps(self) -> list[_TripletSnap]:
        """Return live per-triplet snapshots for web dashboard."""
        return list(self._triplet_snaps.values())

    def update_leg_price(
        self,
        exchange: str,
        inst_id: str,
        bid: float | None,
        ask: float | None,
    ) -> None:
        """Update raw bid/ask for a single leg across all snaps that reference it."""
        for snap in self._triplet_snaps.values():
            if snap.exchange != exchange:
                continue
            if inst_id == snap.call_id:
                snap.call_bid = bid
                snap.call_ask = ask
            elif inst_id == snap.put_id:
                snap.put_bid = bid
                snap.put_ask = ask
            elif inst_id == snap.future_id:
                snap.fut_bid = bid
                snap.fut_ask = ask

    def _begin_opportunity_session(self, row: _Row) -> None:
        if row.active_session_id is not None:
            return
        if not self._sqlite_path:
            return
        try:
            from pcp_arbitrage.db import (
                find_open_opportunity_session_id,
                insert_opportunity_session,
            )

            existing = find_open_opportunity_session_id(
                self._sqlite_path,
                exchange=row.exchange,
                contract=row.label,
                direction=row.direction_cn,
            )
            if existing is not None:
                row.active_session_id = existing
                return

            started = datetime.datetime.fromtimestamp(
                row.first_active, datetime.UTC
            ).isoformat(timespec="milliseconds") + "Z"
            row.active_session_id = insert_opportunity_session(
                self._sqlite_path,
                exchange=row.exchange,
                contract=row.label,
                direction=row.direction_cn,
                started_utc=started,
            )
        except Exception as exc:
            logger.warning("[db] insert opportunity_session failed: %s", exc)

    def _end_opportunity_session(self, row: _Row, now: float) -> None:
        if not self._sqlite_path or row.active_session_id is None:
            return
        sid = row.active_session_id
        row.active_session_id = None
        dur = row.frozen_active_duration_sec
        try:
            from pcp_arbitrage.db import close_opportunity_session

            ended = datetime.datetime.utcfromtimestamp(now).isoformat(
                timespec="milliseconds"
            ) + "Z"
            close_opportunity_session(
                self._sqlite_path,
                sid,
                ended_utc=ended,
                duration_sec=dur,
                gross_usdt=row.gross,
                fee_usdt=row.fee,
                net_usdt=row.net,
                tradeable=row.tradeable,
                ann_pct=row.ann_pct,
                ann_pct_max=row.max_ann_pct,
                days_to_exp=row.days_to_expiry,
            )
        except Exception as exc:
            logger.warning("[db] close opportunity_session failed: %s", exc)

    def _sync_current_row_now(self, row: _Row) -> None:
        """Best-effort immediate upsert for a single row on state edge."""
        if not self._sqlite_path:
            return
        try:
            from pcp_arbitrage.db import upsert_opportunity_current

            upsert_opportunity_current(self._sqlite_path, [row])
        except Exception as exc:
            logger.warning("[db] immediate upsert_opportunity_current failed: %s", exc)

    def record_evaluation(
        self,
        exchange: str,
        triplet: Triplet,
        direction: str,
        sig: ArbitrageSignal | None,
        min_annualized_rate: float,
    ) -> None:
        key = (exchange, triplet.symbol, triplet.expiry, triplet.strike, direction)
        tkey = (exchange, triplet.symbol, triplet.expiry, triplet.strike)
        now = time.time()
        qualified = sig is not None and sig.annualized_return >= min_annualized_rate

        def _ann_pct(s: ArbitrageSignal) -> float:
            return s.annualized_return * 100

        # ── maintain _TripletSnap ──────────────────────────────────────────
        snap = self._triplet_snaps.get(tkey)
        if snap is None:
            snap = _TripletSnap(
                exchange=exchange,
                symbol=triplet.symbol,
                expiry=triplet.expiry,
                strike=triplet.strike,
                call_id=triplet.call_id,
                put_id=triplet.put_id,
                future_id=triplet.future_id,
                days_to_expiry=sig.days_to_expiry if sig is not None else 0.0,
            )
            self._triplet_snaps[tkey] = snap
        if sig is not None:
            snap.days_to_expiry = sig.days_to_expiry
            snap.updated_at = now
            # both directions share the same book, so always update all legs
            if direction == "forward":
                # forward uses call_ask, put_bid, future_bid
                snap.call_ask = sig.call_price_coin
                snap.put_bid = sig.put_price_coin
                snap.fut_bid = sig.future_price
                snap.fwd_ann_pct = _ann_pct(sig) if qualified else None
            else:
                # reverse uses call_bid, put_ask, future_ask
                snap.call_bid = sig.call_price_coin
                snap.put_ask = sig.put_price_coin
                snap.fut_ask = sig.future_price
                snap.rev_ann_pct = _ann_pct(sig) if qualified else None
        else:
            if direction == "forward":
                snap.fwd_ann_pct = None
            else:
                snap.rev_ann_pct = None

        # ── maintain _Row (existing logic) ─────────────────────────────────
        if qualified:
            assert sig is not None
            row = self._rows.get(key)
            ap = _ann_pct(sig)
            if row is None:
                self._rows[key] = _Row(
                    exchange=exchange,
                    label=f"{triplet.symbol}-{triplet.expiry}-{format_strike_display(triplet.symbol, triplet.strike)}",
                    direction_cn=_direction_cn(direction),
                    active=True,
                    first_active=now,
                    last_eval=now,
                    last_active_eval=now,
                    gross=sig.gross_profit,
                    days_to_expiry=sig.days_to_expiry,
                    ann_pct=ap,
                    max_ann_pct=ap,
                    net=sig.net_profit,
                    fee=sig.total_fee,
                    tradeable=sig.tradeable_qty,
                    strike=triplet.strike,
                    call_px_usdt=sig.call_price,
                    put_px_usdt=sig.put_price,
                    fut_px_usdt=sig.future_price,
                    call_fee=sig.call_fee,
                    put_fee=sig.put_fee,
                    fut_fee=sig.fut_fee,
                    frozen_active_duration_sec=None,
                )
                self._begin_opportunity_session(self._rows[key])
            else:
                reactivated = not row.active
                if not row.active:
                    row.first_active = now
                    row.frozen_active_duration_sec = None
                row.active = True
                row.last_eval = now
                row.last_active_eval = now
                row.gross = sig.gross_profit
                row.days_to_expiry = sig.days_to_expiry
                row.ann_pct = ap
                row.max_ann_pct = max(row.max_ann_pct, ap)
                row.net = sig.net_profit
                row.fee = sig.total_fee
                row.tradeable = sig.tradeable_qty
                row.strike = triplet.strike
                row.call_px_usdt = sig.call_price
                row.put_px_usdt = sig.put_price
                row.fut_px_usdt = sig.future_price
                row.call_fee = sig.call_fee
                row.put_fee = sig.put_fee
                row.fut_fee = sig.fut_fee
                if reactivated:
                    self._begin_opportunity_session(row)
            return

        row = self._rows.get(key)
        if row is None:
            return
        was_active = row.active
        if not was_active:
            # Freeze row values while inactive; wait for next qualified tick.
            return
        row.last_eval = now
        row.frozen_active_duration_sec = now - row.first_active
        self._end_opportunity_session(row, now)
        row.active = False
        self._sync_current_row_now(row)

    def _build_static_header(self, clock: str, up: str) -> RenderableType:
        lines: list[Text] = []
        ann = self._min_annualized_rate * 100

        # 第一行：时间和运行时长
        lines.append(
            Text.from_markup(
                f"[dim]当前时间 {clock}    已运行 {up}[/]"
            )
        )
        # 第二行：空行
        lines.append(Text(""))
        # 第三行：配置参数
        lines.append(
            Text.from_markup(
                f"[bold]最小年化阈值[/] ≥ [yellow]{ann:.2f}%[/]   "
                f"[bold]ATM范围[/] [yellow]±{self._atm_range * 100:.0f}%[/]   "
                f"[bold]最短到期[/] [yellow]{self._min_days_to_expiry:.1f}天[/]"
            )
        )
        if not self._runner_meta:
            lines.append(Text("交易所：各 Runner 初始化中…", style="dim italic"))
        else:
            total_t = sum(m.n_triplets for m in self._runner_meta.values())
            names = sorted(self._runner_meta.keys())
            # 空行隔开配置参数和交易所表格
            lines.append(Text(""))
            # 交易所信息表格
            ex_table = Table(
                box=box.SIMPLE_HEAD,
                show_header=True,
                show_footer=True,
                header_style="bold",
                footer_style="bold green",
                padding=(0, 1),
                show_edge=False,
            )
            ex_table.add_column("交易所", style="bold cyan", footer="合计")
            ex_table.add_column("结算类型", style="magenta")
            ex_table.add_column("期权 taker", justify="right", style="dim")
            ex_table.add_column("期权 maker", justify="right", style="dim")
            ex_table.add_column("交割合约 taker", justify="right", style="dim")
            ex_table.add_column("交割合约 maker", justify="right", style="dim")
            ex_table.add_column("监控标的", style="dim")
            ex_table.add_column(
                "组合数",
                justify="right",
                footer=f"[green]{total_t}[/]",
            )
            for name in names:
                m = self._runner_meta[name]
                sym_str = ", ".join(m.symbols) if m.symbols else ""
                ex_table.add_row(
                    name,
                    m.settle_type,
                    f"{m.option_taker_rate * 100:.4f}%",
                    f"{m.option_maker_rate * 100:.4f}%",
                    f"{m.future_taker_rate * 100:.4f}%",
                    f"{m.future_maker_rate * 100:.4f}%",
                    sym_str,
                    str(m.n_triplets),
                )
            lines.append(ex_table)
        return Panel(
            Group(*lines),
            title="[bold]运行信息[/]",
            border_style="blue",
            padding=(0, 1),
        )

    def _build_index_price_panel(self) -> RenderableType | None:
        """Top panel showing real-time index prices for monitored symbols."""
        if not self._index_prices:
            return None
        syms = self._symbols if self._symbols else sorted(self._index_prices.keys())
        # Symbols with small unit prices need more decimal places
        _HIGH_PRECISION = {"DOGE", "TRX", "SHIB", "PEPE"}
        parts: list[str] = []
        for sym in syms:
            px = self._index_prices.get(sym)
            if px is not None:
                fmt = ".4f" if sym.upper() in _HIGH_PRECISION else ",.2f"
                parts.append(f"[bold white]{sym}[/]  [yellow]{px:{fmt}}[/]")
        if not parts:
            return None
        content = Text.from_markup("   ".join(parts))
        return Panel(content, title="[bold]指数价格[/]", border_style="green", padding=(0, 1))

    def build_rich_table(self) -> RenderableType:
        """Full layout: index prices + static header + opportunity panel."""
        now = time.time()
        clock = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        up = _fmt_uptime(now - self._started_at)
        table = self._build_opportunities_table()
        opp_panel = Panel(
            table,
            title="[bold]机会列表[/]",
            border_style="yellow",
            padding=(0, 0),
        )
        index_panel = self._build_index_price_panel()
        sections: list[RenderableType] = []
        if index_panel is not None:
            sections += [index_panel, Text("")]
        sections += [self._build_static_header(clock, up), Text(""), opp_panel]
        return Group(*sections)

    def build_rich_table_no_index(self) -> RenderableType:
        """Full layout without index price panel — for web ANSI stream."""
        now = time.time()
        clock = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        up = _fmt_uptime(now - self._started_at)
        table = self._build_opportunities_table()
        opp_panel = Panel(table, title="[bold]机会列表[/]", border_style="yellow", padding=(0, 0))
        return Group(self._build_static_header(clock, up), Text(""), opp_panel)

    def _build_opportunities_table(self) -> Table:
        now = time.time()
        rows = list(self._rows.values())
        rows.sort(key=lambda r: (not r.active, -r.ann_pct))
        rows = rows[: self._max_rows]

        table = Table(
            caption=(
                "[dim]表中价差、手续费、净利等金额均已换算为 USDT\n"
                "深度：三腿对手盘最优档挂量取最小（单位与交易所一致：OKX/Binance 为张，Deribit 为标的数量或 USD 名义额）。\n"
                "最大年化：该组合历次评估中出现过的年化最高值（%）；\n"
                "实时/最大 列中年化后的时长为本次（或上次冻结的）机会持续毫秒数。\n"
                "状态：套利机会是否还在。\n"
                "正套（正向）：买 Call + 卖 Put + 卖交割合约 → "
                "价差 = put_bid + future_bid − call_ask − K\n"
                "反套（反向）：卖 Call + 买 Put + 买交割合约 → "
                "价差 = call_bid + K − put_ask − future_ask[/]"
            ),
            box=box.ROUNDED,
            header_style="bold cyan",
            show_lines=False,
            padding=(0, 1),
            caption_justify="left",
        )
        table.add_column("交易所", style="cyan", no_wrap=True)
        table.add_column("合约", min_width=16)
        table.add_column("距离到期", justify="right")
        table.add_column("方向", justify="center")
        table.add_column("价差", justify="right")
        table.add_column("手续费", justify="right")
        table.add_column("净利", justify="right")
        table.add_column("深度", justify="right")
        table.add_column("实时年化 / 时长", justify="right")
        table.add_column("最大年化 / 时长", justify="right")
        table.add_column("状态", justify="center")

        for r in rows:
            dur = _duration_cell(r, now)
            if r.active:
                status = Text("active", style="bold green")
                row_style = None
            else:
                status = Text("inactive", style="dim")
                row_style = "dim"

            table.add_row(
                r.exchange,
                r.label,
                _fmt_days_to_expiry(r.days_to_expiry),
                r.direction_cn,
                f"{r.gross:.2f}",
                f"{r.fee:.2f}",
                f"{r.net:.2f}",
                f"{r.tradeable:.4g}",
                _ann_dur_cell(r.ann_pct, dur),
                _ann_dur_cell(r.max_ann_pct, dur),
                status,
                style=row_style,
            )
        return table

    def _format_lines(self) -> list[str]:
        """Plain-text snapshot for unit tests (matches table content loosely)."""
        now = time.time()
        rows = list(self._rows.values())
        rows.sort(key=lambda r: (not r.active, -r.ann_pct))
        rows = rows[: self._max_rows]

        header = (
            f"{'交易所':<10} {'合约':<22} {'距离到期':>8} {'方向':^4} {'价差':>10} "
            f"{'手续费':>8} {'净利':>10} {'可交易量':>10} "
            f"{'实时年化/时长':>18} {'最大年化/时长':>18} {'状态':<8}"
        )
        sep = "─" * len(header)
        out = [
            "PCP opportunities (tick-driven; inactive = last eval below threshold)",
            "Fees: round-trip × 3 legs — see README; amounts in USDT",
            "Forward: gross = put_bid + future_bid - call_ask - K | "
            "Reverse: gross = call_bid + K - put_ask - future_ask",
            sep,
            header,
            sep,
        ]
        for r in rows:
            st = "active" if r.active else "inactive"
            dur = _duration_cell(r, now)
            dte = _fmt_days_to_expiry(r.days_to_expiry)
            ann_live = _ann_dur_cell(r.ann_pct, dur)
            ann_max = _ann_dur_cell(r.max_ann_pct, dur)
            out.append(
                f"{r.exchange:<10} {r.label:<22} {dte:>8} {r.direction_cn:^4} "
                f"{r.gross:>10.2f} {r.fee:>8.2f} {r.net:>10.2f} {r.tradeable:>10.4g} "
                f"{ann_live:>18} {ann_max:>18} {st:<8}"
            )
        return out

    def render_lines_for_test(self) -> list[str]:
        """Plain lines without Rich (for unit tests)."""
        return self._format_lines()
