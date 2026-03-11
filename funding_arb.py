"""
╔══════════════════════════════════════════════════════════════════╗
║     FUNDING RATE ARB BOT — DeepTrade (Sui) vs Binance Futures  ║
║     Strategy: Delta-neutral, thu chênh lệch funding rate        ║
║     Stack: Python asyncio + uvloop + WebSocket                  ║
╚══════════════════════════════════════════════════════════════════╝

Chiến lược:
  - Theo dõi funding rate trên DeepTrade và Binance liên tục
  - Khi chênh lệch > ngưỡng:
      SHORT bên có rate cao (nhận funding)
      LONG  bên có rate thấp (trả ít funding)
  - Delta-neutral → không bị rủi ro giá, chỉ ăn chênh lệch rate
  - Đóng position khi rate hội tụ hoặc margin xuống thấp

Cài đặt:
    pip install -r requirements.txt
    cp .env.example .env  # điền API keys

Chạy:
    python funding_arb.py
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import aiohttp
import websockets
from dotenv import load_dotenv

# ── Cài uvloop nếu có (Linux/Mac) — tăng tốc 2-4x ──────────
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("✅ uvloop enabled")
except ImportError:
    print("⚠️  uvloop không có — dùng asyncio mặc định (Windows OK)")

load_dotenv()

# ════════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════════
class Config:
    # API Keys
    BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
    SUI_PRIVATE_KEY    = os.getenv("SUI_PRIVATE_KEY", "")
    SUI_RPC_URL        = os.getenv("SUI_RPC_URL", "https://fullnode.mainnet.sui.io")
    DEEPTRADE_PKG      = os.getenv("DEEPTRADE_PACKAGE_ID", "")

    # Bot params
    SYMBOL             = os.getenv("SYMBOL", "SUI")
    MIN_FUNDING_DIFF   = float(os.getenv("MIN_FUNDING_DIFF", "0.0001"))   # 0.01%/h
    POSITION_USDC      = float(os.getenv("POSITION_SIZE_USDC", "100"))
    MAX_POSITION_USDC  = float(os.getenv("MAX_POSITION_USDC", "500"))
    MARGIN_SAFETY      = float(os.getenv("MARGIN_SAFETY", "1.5"))         # 150%
    LEG_TIMEOUT_MS     = int(os.getenv("LEG_TIMEOUT_MS", "500"))

    # Endpoints
    BINANCE_WS         = "wss://fstream.binance.com/ws"
    BINANCE_REST       = "https://fapi.binance.com"
    PYTH_WS            = "wss://hermes.pyth.network/ws"

    # Pyth price feed ID cho SUI/USD
    PYTH_SUI_USD       = "0x23d7315113f5b1d3ba7a83604c44b94d79f4fd69af77f804fc7f920a6dc65744"

# ════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════
try:
    import colorlog
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s]%(reset)s %(message)s",
        log_colors={"DEBUG":"cyan","INFO":"green","WARNING":"yellow","ERROR":"red","CRITICAL":"bold_red"}
    ))
    logging.basicConfig(level=logging.INFO, handlers=[
        handler,
        logging.FileHandler("funding_arb.log", encoding="utf-8")
    ])
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("funding_arb.log", encoding="utf-8")])

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ════════════════════════════════════════════════════════════
class Side(Enum):
    LONG  = "LONG"
    SHORT = "SHORT"

class Venue(Enum):
    BINANCE    = "BINANCE"
    DEEPTRADE  = "DEEPTRADE"

@dataclass
class FundingRate:
    venue:        Venue
    symbol:       str
    rate_per_hour: float        # % per hour
    next_funding: Optional[int] = None  # unix ms
    ts:           int = field(default_factory=lambda: int(time.time() * 1000))

@dataclass
class Position:
    venue:       Venue
    side:        Side
    size_usdc:   float
    entry_price: float
    open_ts:     int = field(default_factory=lambda: int(time.time() * 1000))
    pnl:         float = 0.0

@dataclass
class ArbState:
    # Funding rates
    binance_rate:    Optional[FundingRate] = None
    deeptrade_rate:  Optional[FundingRate] = None

    # Oracle prices
    pyth_price:      float = 0.0
    binance_price:   float = 0.0

    # Open positions
    positions:       list = field(default_factory=list)

    # Stats
    total_funding_collected: float = 0.0
    trade_count:     int = 0
    start_ts:        int = field(default_factory=lambda: int(time.time()))

    @property
    def funding_diff(self) -> float:
        """Chênh lệch funding rate giữa 2 sàn (% per hour)"""
        if not self.binance_rate or not self.deeptrade_rate:
            return 0.0
        return self.deeptrade_rate.rate_per_hour - self.binance_rate.rate_per_hour

    @property
    def has_open_position(self) -> bool:
        return len(self.positions) > 0

    @property
    def uptime_hours(self) -> float:
        return (int(time.time()) - self.start_ts) / 3600


# ════════════════════════════════════════════════════════════
#  1. BINANCE FUNDING RATE FEED
# ════════════════════════════════════════════════════════════
class BinanceFeed:
    """
    Lấy funding rate + mark price từ Binance Futures qua WebSocket
    Stream: <symbol>@markPrice → update mỗi 3 giây
    """
    def __init__(self, symbol: str, state: ArbState):
        self.symbol = symbol.upper() + "USDT"
        self.state  = state

    async def connect(self):
        url = f"{Config.BINANCE_WS}/{self.symbol.lower()}@markPrice"
        log.info(f"🔌 Binance WS connecting: {url}")

        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log.info("✅ Binance WS connected")
                    async for msg in ws:
                        await self._handle(json.loads(msg))
            except Exception as e:
                log.warning(f"⚠️  Binance WS error: {e} — reconnecting in 3s")
                await asyncio.sleep(3)

    async def _handle(self, data: dict):
        """
        Binance markPrice stream payload:
        {
          "e": "markPriceUpdate",
          "r": "0.00010000",    ← funding rate (per 8h)
          "p": "0.94680000",    ← mark price
          "T": 1234567890000    ← next funding time
        }
        """
        if data.get("e") != "markPriceUpdate":
            return

        # Binance rate là per 8h → convert sang per hour
        rate_8h    = float(data.get("r", 0))
        rate_1h    = rate_8h / 8
        mark_price = float(data.get("p", 0))

        self.state.binance_rate = FundingRate(
            venue         = Venue.BINANCE,
            symbol        = self.symbol,
            rate_per_hour = rate_1h,
            next_funding  = data.get("T"),
        )
        self.state.binance_price = mark_price

    async def fetch_initial_rate(self):
        """Lấy funding rate lần đầu qua REST trước khi WS kết nối"""
        url = f"{Config.BINANCE_REST}/fapi/v1/premiumIndex?symbol={self.symbol}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    data = await r.json()
                    rate_8h = float(data.get("lastFundingRate", 0))
                    self.state.binance_rate = FundingRate(
                        venue=Venue.BINANCE,
                        symbol=self.symbol,
                        rate_per_hour=rate_8h / 8,
                    )
                    self.state.binance_price = float(data.get("markPrice", 0))
                    log.info(f"📊 Binance initial rate: {rate_8h/8*100:.4f}%/h | Price: {self.state.binance_price}")
        except Exception as e:
            log.error(f"❌ Binance REST error: {e}")


# ════════════════════════════════════════════════════════════
#  2. PYTH ORACLE FEED (giá tham chiếu trên Sui)
# ════════════════════════════════════════════════════════════
class PythFeed:
    """
    Lấy giá SUI/USD từ Pyth Network — đây là oracle DeepTrade dùng
    Latency: ~50ms từ nguồn
    """
    def __init__(self, state: ArbState):
        self.state    = state
        self.price_id = Config.PYTH_SUI_USD

    async def connect(self):
        log.info("🔌 Pyth WS connecting...")
        while True:
            try:
                async with websockets.connect(Config.PYTH_WS, ping_interval=15) as ws:
                    # Subscribe vào SUI/USD price feed
                    await ws.send(json.dumps({
                        "ids":  [self.price_id],
                        "type": "subscribe",
                        "parsed": True,
                    }))
                    log.info("✅ Pyth WS connected — subscribed SUI/USD")
                    async for msg in ws:
                        await self._handle(json.loads(msg))
            except Exception as e:
                log.warning(f"⚠️  Pyth WS error: {e} — reconnecting in 3s")
                await asyncio.sleep(3)

    async def _handle(self, data: dict):
        try:
            parsed = data.get("parsed", [])
            if not parsed: return
            feed   = parsed[0]
            price  = float(feed["price"]["price"]) * (10 ** feed["price"]["expo"])
            self.state.pyth_price = price
        except Exception:
            pass


# ════════════════════════════════════════════════════════════
#  3. DEEPTRADE FUNDING RATE (qua Sui RPC)
# ════════════════════════════════════════════════════════════
class DeepTradeFeed:
    """
    Lấy hourly interest rate từ DeepTrade smart contract trên Sui
    DeepTrade dùng kiểu margin lending → interest rate thay vì funding rate

    NOTE: DeepTrade chưa có public WebSocket API cho funding rate
    → Poll qua Sui RPC mỗi 30 giây (đủ vì rate thay đổi chậm)
    → Khi có official SDK/API → upgrade lên WebSocket
    """
    def __init__(self, state: ArbState):
        self.state = state

    async def poll(self):
        """Poll interest rate từ DeepTrade mỗi 30 giây"""
        while True:
            await self._fetch_rate()
            await asyncio.sleep(30)

    async def _fetch_rate(self):
        """
        Query Sui RPC để đọc state của DeepTrade lending pool
        Đây là template — cần điền đúng object ID sau khi có contract address
        """
        try:
            payload = {
                "jsonrpc": "2.0",
                "id":      1,
                "method":  "sui_getObject",
                "params":  [
                    Config.DEEPTRADE_PKG,  # Object ID của lending pool
                    {"showContent": True}
                ]
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    Config.SUI_RPC_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    data = await r.json()
                    # Parse interest rate từ contract state
                    # Structure tùy thuộc vào DeepTrade contract
                    rate = self._parse_rate(data)
                    self.state.deeptrade_rate = FundingRate(
                        venue         = Venue.DEEPTRADE,
                        symbol        = Config.SYMBOL,
                        rate_per_hour = rate,
                    )
                    log.debug(f"📊 DeepTrade rate: {rate*100:.4f}%/h")

        except Exception as e:
            log.warning(f"⚠️  DeepTrade rate fetch error: {e}")
            # Fallback: dùng rate từ UI nếu có (0.0003%/h từ ảnh bạn chụp)
            if not self.state.deeptrade_rate:
                self.state.deeptrade_rate = FundingRate(
                    venue         = Venue.DEEPTRADE,
                    symbol        = Config.SYMBOL,
                    rate_per_hour = 0.000003,  # 0.0003% từ UI
                )

    def _parse_rate(self, rpc_response: dict) -> float:
        """
        Parse interest rate từ Sui RPC response
        TODO: Implement sau khi có đúng contract ABI từ DeepTrade
        Hiện tại return mock rate để test logic
        """
        try:
            content = rpc_response["result"]["data"]["content"]["fields"]
            # Tùy cấu trúc contract của DeepTrade
            # Ví dụ: content["hourly_interest_rate"] / 1e9
            rate = float(content.get("hourly_interest_rate", 3)) / 1e9
            return rate
        except Exception:
            return 0.000003  # Fallback: 0.0003%/h (từ UI)


# ════════════════════════════════════════════════════════════
#  4. EXECUTION ENGINE
# ════════════════════════════════════════════════════════════
class ExecutionEngine:
    """
    Thực thi lệnh khi có signal arb
    IMPORTANT: Hiện tại là DRY RUN (simulation) — không đặt lệnh thật
    Set DRY_RUN = False khi đã test kỹ
    """
    DRY_RUN = True  # ← ĐỔI THÀNH False KHI SẴN SÀNG TRADE THẬT

    def __init__(self, state: ArbState):
        self.state = state

    async def open_arb(self, long_venue: Venue, short_venue: Venue):
        """
        Mở 2 legs đồng thời:
        - LONG trên venue có rate thấp (trả ít funding)
        - SHORT trên venue có rate cao (nhận funding)
        """
        diff = self.state.funding_diff
        log.info(f"🎯 ARB SIGNAL | Diff: {diff*100:.4f}%/h | "
                 f"SHORT {short_venue.value} | LONG {long_venue.value}")

        if self.DRY_RUN:
            log.info(f"🔸 [DRY RUN] Giả lập mở vị thế:")
            log.info(f"   LONG  {long_venue.value}  {Config.POSITION_USDC} USDC @ {self.state.pyth_price:.4f}")
            log.info(f"   SHORT {short_venue.value} {Config.POSITION_USDC} USDC @ {self.state.pyth_price:.4f}")

            # Simulate positions
            for venue, side in [(long_venue, Side.LONG), (short_venue, Side.SHORT)]:
                self.state.positions.append(Position(
                    venue=venue, side=side,
                    size_usdc=Config.POSITION_USDC,
                    entry_price=self.state.pyth_price,
                ))
            self.state.trade_count += 1
            return

        # ── THẬT: Gửi lệnh song song ──────────────────────────
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    self._open_binance(Side.LONG if long_venue == Venue.BINANCE else Side.SHORT),
                    self._open_deeptrade(Side.LONG if long_venue == Venue.DEEPTRADE else Side.SHORT),
                    return_exceptions=True
                ),
                timeout=Config.LEG_TIMEOUT_MS / 1000
            )

            # Kiểm tra nếu 1 leg fail → cancel leg kia
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                log.error(f"❌ Leg error: {errors} — đang cancel vị thế")
                await self._emergency_close()
            else:
                log.info("✅ Cả 2 legs đã mở thành công")
                self.state.trade_count += 1

        except asyncio.TimeoutError:
            log.error(f"❌ Timeout {Config.LEG_TIMEOUT_MS}ms — đang cancel")
            await self._emergency_close()

    async def close_arb(self, reason: str = ""):
        """Đóng tất cả positions"""
        if not self.state.positions:
            return

        log.info(f"🔒 Đóng arb position | Lý do: {reason}")

        if self.DRY_RUN:
            # Tính PnL từ funding đã thu
            hold_hours = (int(time.time() * 1000) - self.state.positions[0].open_ts) / 3_600_000
            funding_collected = abs(self.state.funding_diff) * Config.POSITION_USDC * hold_hours
            self.state.total_funding_collected += funding_collected
            log.info(f"🔸 [DRY RUN] Đóng vị thế | Funding thu: ${funding_collected:.4f} | "
                     f"Tổng: ${self.state.total_funding_collected:.4f}")
            self.state.positions.clear()
            return

        await asyncio.gather(
            self._close_binance(),
            self._close_deeptrade(),
            return_exceptions=True
        )
        self.state.positions.clear()

    async def _open_binance(self, side: Side):
        """Đặt lệnh market trên Binance Futures"""
        # TODO: Implement với python-binance hoặc aiohttp trực tiếp
        # from binance.client import AsyncClient
        # client = AsyncClient(Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET)
        # await client.futures_create_order(
        #     symbol=Config.SYMBOL + "USDT",
        #     side="BUY" if side == Side.LONG else "SELL",
        #     type="MARKET",
        #     quantity=Config.POSITION_USDC / self.state.binance_price
        # )
        log.debug(f"[Binance] Open {side.value} — implement me")

    async def _open_deeptrade(self, side: Side):
        """Đặt lệnh margin trên DeepTrade (Sui transaction)"""
        # TODO: Implement với pysui
        # from pysui import SuiConfig, SyncClient
        # Build transaction để call DeepTrade contract
        # open_position(market_id, is_long, size, leverage)
        log.debug(f"[DeepTrade] Open {side.value} — implement me")

    async def _close_binance(self):
        log.debug("[Binance] Close position — implement me")

    async def _close_deeptrade(self):
        log.debug("[DeepTrade] Close position — implement me")

    async def _emergency_close(self):
        """Đóng khẩn cấp khi có lỗi leg"""
        log.critical("🚨 EMERGENCY CLOSE — Đóng tất cả vị thế ngay!")
        await self.close_arb(reason="emergency")


# ════════════════════════════════════════════════════════════
#  5. RISK MANAGER
# ════════════════════════════════════════════════════════════
class RiskManager:
    def __init__(self, state: ArbState, engine: ExecutionEngine):
        self.state  = state
        self.engine = engine

    def should_open(self) -> tuple[bool, Venue, Venue]:
        """
        Kiểm tra điều kiện để mở arb:
        Returns (should_open, long_venue, short_venue)
        """
        diff = self.state.funding_diff

        # Chưa có đủ dữ liệu
        if not self.state.binance_rate or not self.state.deeptrade_rate:
            return False, None, None

        # Đang có position rồi
        if self.state.has_open_position:
            return False, None, None

        # Chênh lệch không đủ lớn
        if abs(diff) < Config.MIN_FUNDING_DIFF:
            return False, None, None

        # Giá chưa có
        if self.state.pyth_price <= 0:
            return False, None, None

        # DeepTrade rate cao hơn → SHORT DeepTrade, LONG Binance
        if diff > 0:
            return True, Venue.BINANCE, Venue.DEEPTRADE
        # Binance rate cao hơn → SHORT Binance, LONG DeepTrade
        else:
            return True, Venue.DEEPTRADE, Venue.BINANCE

    def should_close(self) -> tuple[bool, str]:
        """Kiểm tra điều kiện đóng position"""
        if not self.state.has_open_position:
            return False, ""

        diff = self.state.funding_diff

        # Rate hội tụ → đóng
        if abs(diff) < Config.MIN_FUNDING_DIFF * 0.3:
            return True, f"Rate hội tụ (diff={diff*100:.4f}%/h)"

        # Rate đổi chiều → đóng ngay
        pos_side = self.state.positions[0].side
        if pos_side == Side.SHORT and diff < 0:
            return True, "Rate đổi chiều — SHORT bên sai"
        if pos_side == Side.LONG and diff > 0:
            return True, "Rate đổi chiều — LONG bên sai"

        # Giữ quá 8 giờ → review lại
        hold_ms = int(time.time() * 1000) - self.state.positions[0].open_ts
        if hold_ms > 8 * 3_600_000:
            return True, "Giữ > 8 giờ — đóng để review"

        return False, ""


# ════════════════════════════════════════════════════════════
#  6. MONITOR — In trạng thái real-time
# ════════════════════════════════════════════════════════════
async def monitor_loop(state: ArbState):
    while True:
        await asyncio.sleep(10)

        b_rate = state.binance_rate.rate_per_hour * 100 if state.binance_rate else 0
        d_rate = state.deeptrade_rate.rate_per_hour * 100 if state.deeptrade_rate else 0
        diff   = state.funding_diff * 100

        pos_info = f"{len(state.positions)} positions" if state.has_open_position else "No position"

        log.info(
            f"📡 MONITOR | "
            f"Pyth: ${state.pyth_price:.4f} | "
            f"Binance rate: {b_rate:.4f}%/h | "
            f"DeepTrade rate: {d_rate:.4f}%/h | "
            f"Diff: {diff:+.4f}%/h | "
            f"{pos_info} | "
            f"Funding thu: ${state.total_funding_collected:.4f} | "
            f"Uptime: {state.uptime_hours:.1f}h"
        )


# ════════════════════════════════════════════════════════════
#  7. MAIN ARB LOOP
# ════════════════════════════════════════════════════════════
async def arb_loop(state: ArbState, engine: ExecutionEngine, risk: RiskManager):
    """Vòng lặp chính — kiểm tra signal mỗi 1 giây"""
    log.info("🚀 Arb loop started")

    while True:
        try:
            # ── Kiểm tra đóng position ──
            should_close, reason = risk.should_close()
            if should_close:
                await engine.close_arb(reason=reason)

            # ── Kiểm tra mở position ──
            should_open, long_v, short_v = risk.should_open()
            if should_open:
                await engine.open_arb(long_venue=long_v, short_venue=short_v)

        except Exception as e:
            log.error(f"❌ Arb loop error: {e}")

        await asyncio.sleep(1)


# ════════════════════════════════════════════════════════════
#  8. ENTRY POINT
# ════════════════════════════════════════════════════════════
async def main():
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║   FUNDING ARB BOT — DeepTrade vs Binance        ║")
    log.info(f"║   Symbol  : {Config.SYMBOL:<36}║")
    log.info(f"║   Min diff: {Config.MIN_FUNDING_DIFF*100:.4f}%/h{'':<29}║")
    log.info(f"║   Size    : ${Config.POSITION_USDC} USDC per leg{'':<20}║")
    log.info(f"║   Mode    : {'DRY RUN (simulation)' if ExecutionEngine.DRY_RUN else '⚠️  LIVE TRADING':<33}║")
    log.info("╚══════════════════════════════════════════════════╝")

    # Khởi tạo shared state
    state  = ArbState()
    engine = ExecutionEngine(state)
    risk   = RiskManager(state, engine)

    # Feed objects
    binance_feed    = BinanceFeed(Config.SYMBOL, state)
    pyth_feed       = PythFeed(state)
    deeptrade_feed  = DeepTradeFeed(state)

    # Lấy initial data trước khi start loop
    await binance_feed.fetch_initial_rate()
    await deeptrade_feed._fetch_rate()

    # Chạy tất cả coroutines song song
    await asyncio.gather(
        # Data feeds
        binance_feed.connect(),       # Binance WS — funding rate + price
        pyth_feed.connect(),          # Pyth WS — oracle price
        deeptrade_feed.poll(),        # Sui RPC poll — DeepTrade rate

        # Logic
        arb_loop(state, engine, risk),  # Arb decision loop
        monitor_loop(state),            # Logging monitor
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("🛑 Bot dừng theo yêu cầu")
