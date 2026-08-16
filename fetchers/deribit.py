import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple

from models import OHLCV
from .base import BaseFetcher
from .resample import resample_ohlcv


# Deribit get_tradingview_chart_data 实测支持的 resolution (分钟数或 1D)
DERIBIT_RESOLUTION_MAP = {
    "1m": "1", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "6h": "360", "12h": "720", "1d": "1D",
}

# API 不支持的周期: 用基础 resolution 拉取后本地重采样
RESAMPLE_BASE_RESOLUTION = {
    "4h": "60",   # 无 240
    "1w": "1D",   # 无 1W
    "1M": "1D",   # 无月线
}

RESOLUTION_INTERVAL_MS = {
    "1": 60_000, "5": 300_000, "15": 900_000, "30": 1_800_000,
    "60": 3_600_000, "360": 21_600_000, "720": 43_200_000,
    "1D": 86_400_000,
}

# API 单次响应上限约 5000 根, 分块时留余量, 否则每块尾部会被静默截断
MAX_CANDLES_PER_REQUEST = 4500


class DeribitFetcher(BaseFetcher):
    # 单请求延迟 ~0.6s, 串行分块过慢; 并发拉取, 由线程安全的
    # RateLimiter 统一限速 (config rps=15, 并发 6 时延迟自限速 ~9 rps)
    CHUNK_CONCURRENCY = 6

    def __init__(self, config: dict, rate_limiter):
        super().__init__("Deribit", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://www.deribit.com/api/v2')
        # 出错不整段重试: 原生周期残余空洞留给下一次 gapfill,
        # 重采样周期由 _do_fetch 上抛后由人工/下次任务重拉
        self.max_retries = 1

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('symbols', {})

    def _do_fetch(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        resolution = DERIBIT_RESOLUTION_MAP.get(
            timeframe, RESAMPLE_BASE_RESOLUTION.get(timeframe)
        )
        if resolution is None:
            self.logger.warning(f"Deribit 不支持的周期: {timeframe}")
            return []

        interval_ms = RESOLUTION_INTERVAL_MS[resolution]
        chunk_size = min(365 * 86400000, MAX_CANDLES_PER_REQUEST * interval_ms)

        chunks: List[Tuple[int, int]] = []
        current_start = start_ts
        while current_start < end_ts:
            chunk_end = min(current_start + chunk_size, end_ts)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end + 1

        failed: List[Tuple[int, int]] = []

        def fetch_range(chunk: Tuple[int, int]) -> List[OHLCV]:
            try:
                return self._fetch_chunk(
                    exchange_symbol, timeframe, resolution, chunk[0], chunk[1]
                )
            except Exception as e:
                # 单次失败不重试
                self.logger.warning(
                    f"Deribit 分块失败 (不重试): {exchange_symbol} "
                    f"{chunk[0]}-{chunk[1]}: {e}"
                )
                failed.append(chunk)
                return []

        with ThreadPoolExecutor(max_workers=self.CHUNK_CONCURRENCY) as pool:
            results = list(pool.map(fetch_range, chunks))

        if failed and timeframe in RESAMPLE_BASE_RESOLUTION:
            # 重采样周期缺任一分块会聚合出看似完整的错误周期值,
            # 必须整段失败 (外层 max_retries=1, 不会反复重试)
            raise RuntimeError(
                f"Deribit {len(failed)}/{len(chunks)} 个分块失败, "
                f"重采样周期需完整数据: {failed[:3]}"
            )

        all_records = [r for batch in results for r in batch]
        all_records.sort(key=lambda r: r.timestamp)

        if timeframe in RESAMPLE_BASE_RESOLUTION:
            # 用真实当前时间判定: 已走完的周期即使超出请求范围也完整
            return resample_ohlcv(all_records, timeframe, now_ms=int(time.time() * 1000))

        # 原生周期: 丢弃尚未走完的末根蜡烛, 避免半周期值被落盘冻结;
        # 失败分块跳过即可, 残余空洞由下一次 gapfill 重新检测
        now_ms = int(time.time() * 1000)
        return [r for r in all_records if r.timestamp + interval_ms <= now_ms]

    def _fetch_chunk(
        self,
        exchange_symbol: str,
        timeframe: str,
        resolution: str,
        current_start: int,
        chunk_end: int,
    ) -> List[OHLCV]:
        self.rate_limiter.wait()
        response = self.session.get(
            f"{self.base_url}/public/get_tradingview_chart_data",
            params={
                'instrument_name': exchange_symbol,
                'start_timestamp': current_start,
                'end_timestamp': chunk_end,
                'resolution': resolution,
            },
            timeout=30
        )

        if response.status_code == 400:
            # 400 通常为请求范围早于标的上架时间, 该块无数据
            self.logger.warning(
                f"Deribit 标的 {exchange_symbol} 时间范围超出: "
                f"{current_start} - {chunk_end}"
            )
            return []

        # 429/5xx 等直接抛出, 单次不重试
        response.raise_for_status()

        result = response.json()

        if 'result' not in result:
            return []

        data = result['result']
        ticks = data.get('ticks', [])
        opens = data.get('open', [])
        highs = data.get('high', [])
        lows = data.get('low', [])
        closes = data.get('close', [])
        volumes = data.get('volume', [])

        # 数组长度不一致时按最短截断, 不用 0 填充伪造蜡烛
        n = min(len(ticks), len(opens), len(highs), len(lows), len(closes), len(volumes))
        return [
            OHLCV(
                timestamp=int(ticks[i]),
                open=float(opens[i]),
                high=float(highs[i]),
                low=float(lows[i]),
                close=float(closes[i]),
                volume=float(volumes[i]),
                quote_volume=0.0,
                exchange=self.name,
                symbol=self.get_unified_symbol(exchange_symbol),
                timeframe=timeframe,
            )
            for i in range(n)
        ]
