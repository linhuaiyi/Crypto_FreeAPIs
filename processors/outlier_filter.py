"""Z-Score based outlier filter for time series data."""

from typing import List

import numpy as np
import pandas as pd

from utils import get_logger

logger = get_logger("OutlierFilter")


class OutlierFilter:
    """Flags rows as outliers using rolling Z-Score analysis."""

    def __init__(self, z_threshold: float = 5.0, window_size: int = 100) -> None:
        self.z_threshold = z_threshold
        self.window_size = window_size

    def filter(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Add ``is_outlier`` boolean column; True when |Z| > threshold in ANY column."""
        result = df.copy()
        outlier_mask = pd.Series(False, index=result.index)

        for col in columns:
            if col not in result.columns:
                logger.warning("Column %s not found, skipping", col)
                continue

            rolling = result[col].rolling(window=self.window_size, min_periods=1)
            mean = rolling.mean()
            std = rolling.std()
            std = std.replace(0, np.nan)

            z_scores = (result[col] - mean) / std
            outlier_mask = outlier_mask | (z_scores.abs() > self.z_threshold)

        result["is_outlier"] = outlier_mask
        logger.info(
            "Flagged %d outlier(s) across %d column(s)",
            int(outlier_mask.sum()),
            len(columns),
        )
        return result

    def get_outliers(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Return only the rows flagged as outliers."""
        filtered = self.filter(df, columns)
        return filtered[filtered["is_outlier"]]
