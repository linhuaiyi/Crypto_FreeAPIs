import time
import requests
from typing import List, Dict, Set, Optional
from models import OHLCV
from .base import BaseFetcher
from .resample import resample_ohlcv


HYPERLIQUID_INTERVAL_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
    "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h",
    "8h": "8h", "12h": "12h", "1d": "1d",
    "3d": "3d", "1w": "1w", "1M": "1M",
}

# Hyperliquid 月线为 30 天滚动窗口而非自然月; 周线为周四锚定的 7 天
# 滚动窗口, 与 Binance/Deribit 的自然月/周一锚定不一致 — 均用日线
# 拉取后本地重采样对齐
RESAMPLE_BASE_INTERVAL = {
    "1M": "1d",
    "1w": "1d",
}

MAX_CANDLES = 5000

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
    "3d": 259_200_000, "1w": 604_800_000, "1M": 2_592_000_000,
}


class HyperliquidFetcher(BaseFetcher):
    def __init__(self, config: dict, rate_limiter):
        super().__init__("Hyperliquid", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://api.hyperliquid.xyz')
        self._available_coins: Optional[Set[str]] = None

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('symbols', {})

    def _get_available_coins(self) -> Set[str]:
        if self._available_coins is not None:
            return self._available_coins

        try:
            response = self.session.post(
                f"{self.base_url}/info",
                json={"type": "meta"},
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            coins = set()
            universe = data.get('universe', [])
            for item in universe:
                if isinstance(item, dict) and 'name' in item:
                    coins.add(item['name'])

            self._available_coins = coins
            self.logger.info(f"Hyperliquid 可用标的: {sorted(coins)[:20]}")
            return coins
        except Exception as e:
            self.logger.warning(f"获取可用标的失败，使用配置中的标的列表: {e}")
            mapping = self.get_symbol_mapping()
            self._available_coins = set(mapping.keys())
            return self._available_coins

    def _is_symbol_available(self, exchange_symbol: str) -> bool:
        available = self._get_available_coins()
        return exchange_symbol in available

    def _fetch_one_batch(
        self,
        exchange_symbol: str,
        unified_symbol: str,
        interval: str,
        timeframe: str,
        batch_start: int,
        batch_end: int,
    ) -> List[OHLCV]:
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": exchange_symbol,
                "interval": interval,
                "startTime": batch_start,
                "endTime": batch_end,
            }
        }

        self.rate_limiter.wait()
        response = self.session.post(
            f"{self.base_url}/info",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code == 429:
            self.logger.warning("触发限速，等待 5s")
            time.sleep(5)
            self.rate_limiter.wait()
            response = self.session.post(
                f"{self.base_url}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

        response.raise_for_status()
        candles = response.json()

        if not candles or candles is None:
            return []

        if not isinstance(candles, list):
            candles = candles.get('data', []) or []

        records = []
        for c in candles:
            volume = float(c.get('v', 0))
            if volume <= 0:
                continue
            records.append(OHLCV(
                timestamp=int(c['t']),
                open=float(c['o']),
                high=float(c['h']),
                low=float(c['l']),
                close=float(c['c']),
                volume=volume,
                quote_volume=0.0,
                exchange=self.name,
                symbol=unified_symbol,
                timeframe=timeframe,
                trades=int(c.get('n', 0)) if c.get('n') else None,
            ))

        return records

    def _do_fetch(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        # 需重采样的周期改用基础 interval 拉取, 最后本地聚合
        resample_base = RESAMPLE_BASE_INTERVAL.get(timeframe)
        interval = resample_base or HYPERLIQUID_INTERVAL_MAP.get(timeframe, "1d")
        unified_symbol = self.get_unified_symbol(exchange_symbol)

        if not self._is_symbol_available(exchange_symbol):
            self.logger.warning(f"标的 {unified_symbol} 在 Hyperliquid 不可用，跳过")
            return []

        interval_ms = INTERVAL_MS.get(interval, 86_400_000)
        batch_span = interval_ms * MAX_CANDLES

        all_records: List[OHLCV] = []
        cursor_end = end_ts
        consecutive_failures = 0

        while cursor_end > start_ts:
            batch_start = max(cursor_end - batch_span, start_ts)

            try:
                batch = self._fetch_one_batch(
                    exchange_symbol, unified_symbol, interval, timeframe,
                    batch_start, cursor_end,
                )
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else 0
                if status_code in (500, 422):
                    consecutive_failures += 1
                    if consecutive_failures >= 2:
                        self.logger.info(
                            f"{unified_symbol} [{timeframe}]: "
                            f"到达 Hyperliquid 数据边界 ({status_code})，停止回填 "
                            f"(共获取 {len(all_records)} 条)"
                        )
                        break
                    cursor_end = batch_start
                    continue
                raise

            if not batch:
                consecutive_failures += 1
                if consecutive_failures >= 2:
                    self.logger.info(
                        f"{unified_symbol} [{timeframe}]: "
                        f"连续空数据，停止回填 "
                        f"(共获取 {len(all_records)} 条)"
                    )
                    break
                cursor_end = batch_start
                continue

            consecutive_failures = 0
            all_records.extend(batch)
            earliest = min(r.timestamp for r in batch)
            # API 的 startTime/endTime 均含端点, 减 1 避免边界蜡烛
            # 重复拉取导致死循环与重采样双计
            cursor_end = earliest - 1

        all_records.sort(key=lambda r: r.timestamp)
        if resample_base:
            # 用真实当前时间判定: 已走完的周期即使超出请求范围也完整
            return resample_ohlcv(all_records, timeframe, now_ms=int(time.time() * 1000))

        # 原生周期: 丢弃尚未走完的末根蜡烛, 避免半周期值被落盘冻结
        now_ms = int(time.time() * 1000)
        return [r for r in all_records if r.timestamp + interval_ms <= now_ms]
