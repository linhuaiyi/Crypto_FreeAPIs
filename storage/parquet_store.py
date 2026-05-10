import os
import pandas as pd
from typing import List, Optional
from models import OHLCV
from utils import get_logger


logger = get_logger("ParquetStore")


class ParquetStore:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def _get_file_path(self, exchange: str, symbol: str, timeframe: str) -> str:
        return os.path.join(self.data_dir, exchange, f"{symbol}_{timeframe}.parquet")

    def save(self, exchange: str, symbol: str, timeframe: str, records: List[OHLCV]) -> int:
        if not records:
            return 0

        file_path = self._get_file_path(exchange, symbol, timeframe)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        df_new = pd.DataFrame([r.to_dict() for r in records])

        if os.path.exists(file_path):
            df_existing = pd.read_parquet(file_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=['timestamp', 'exchange', 'symbol'], keep='first', inplace=True)
            df_combined.sort_values('timestamp', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            added = len(df_combined) - len(df_existing)
        else:
            df_combined = df_new.copy()
            df_combined.sort_values('timestamp', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            added = len(df_combined)

        df_combined.to_parquet(file_path, index=False)
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
