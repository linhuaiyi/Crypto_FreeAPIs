import os
import pandas as pd
from typing import List, Optional
from models import OHLCV
from utils import get_logger


logger = get_logger("ParquetStore")


class ParquetStore:
    # Windows/macOS 文件系统大小写不敏感: "1M" 与 "1m" 会指向同一文件,
    # 月线在文件名中使用独立后缀规避冲突
    _FILENAME_TIMEFRAME_MAP = {"1M": "1mon"}

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def _get_file_path(self, exchange: str, symbol: str, timeframe: str) -> str:
        suffix = self._FILENAME_TIMEFRAME_MAP.get(timeframe, timeframe)
        return os.path.join(self.data_dir, exchange, f"{symbol}_{suffix}.parquet")

    def save(self, exchange: str, symbol: str, timeframe: str, records: List[OHLCV]) -> int:
        if not records:
            return 0

        file_path = self._get_file_path(exchange, symbol, timeframe)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        df_new = pd.DataFrame([r.to_dict() for r in records])

        if os.path.exists(file_path):
            df_existing = pd.read_parquet(file_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            # keep='last': 新拉取的数据覆盖同戳旧行, 保证未走完的周期
            # (如月初只落盘了部分天数) 在后续拉取时被补全修正
            df_combined.drop_duplicates(subset=['timestamp', 'exchange', 'symbol'], keep='last', inplace=True)
            df_combined.sort_values('timestamp', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            added = len(df_combined) - len(df_existing)
        else:
            df_combined = df_new.copy()
            df_combined.sort_values('timestamp', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            added = len(df_combined)

        # 原子写: 先写临时文件再替换, 进程被杀时不会留下截断的损坏文件
        tmp_path = file_path + '.tmp'
        df_combined.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, file_path)
        logger.info(f"[{exchange}] {symbol}: 写入 {len(df_new)} 条，去重后新增 {added} 条，文件已保存")
        return added

    def get_last_timestamp(self, exchange: str, symbol: str, timeframe: str) -> Optional[int]:
        file_path = self._get_file_path(exchange, symbol, timeframe)
        if not os.path.exists(file_path):
            return None
        df = pd.read_parquet(file_path)
        if df.empty:
            return None
        return int(df['timestamp'].max())

    def get_first_timestamp(self, exchange: str, symbol: str, timeframe: str) -> Optional[int]:
        file_path = self._get_file_path(exchange, symbol, timeframe)
        if not os.path.exists(file_path):
            return None
        df = pd.read_parquet(file_path)
        if df.empty:
            return None
        return int(df['timestamp'].min())

    def get_timestamps(self, exchange: str, symbol: str, timeframe: str) -> List[int]:
        """返回文件内全部时间戳(升序),用于内部空洞检测。"""
        file_path = self._get_file_path(exchange, symbol, timeframe)
        if not os.path.exists(file_path):
            return []
        df = pd.read_parquet(file_path, columns=['timestamp'])
        return sorted(int(x) for x in df['timestamp'].tolist())

    def load_all(self, exchange: str, symbol: str, timeframe: str) -> List[OHLCV]:
        from models.ohlcv import OHLCV as OHLCVModel
        file_path = self._get_file_path(exchange, symbol, timeframe)
        if not os.path.exists(file_path):
            return []
        df = pd.read_parquet(file_path)
        return [OHLCVModel.from_dict(row) for row in df.to_dict('records')]

    def get_stats(self, exchange: str, symbol: str, timeframe: str) -> dict:
        file_path = self._get_file_path(exchange, symbol, timeframe)
        if not os.path.exists(file_path):
            return {'exists': False, 'count': 0}
        df = pd.read_parquet(file_path)
        return {
            'exists': True,
            'count': len(df),
            'start_time': int(df['timestamp'].min()) if not df.empty else None,
            'end_time': int(df['timestamp'].max()) if not df.empty else None,
        }
