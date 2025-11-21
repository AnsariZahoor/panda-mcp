"""
Divine Dip Metric
Implementation of the divine_dip metric from panda_jlabs_metrics
"""

from typing import Dict, List, Optional, Literal
import logging

logger = logging.getLogger(__name__)


class DivineDipMetric:
    """
    Divine Dip metric calculator and fetcher

    The Divine Dip metric identifies potential buying opportunities
    in cryptocurrency markets by detecting significant price dips.
    """

    SUPPORTED_CEX_TIMEFRAMES = ["15m", "30m", "1H", "4H", "1D"]
    SUPPORTED_DEX_TIMEFRAMES = ["1H", "4H", "1D"]  # DEX doesn't support 15m, 30m

    SUPPORTED_CEX_EXCHANGES = [
        "binance-spot",
        "binance-futures",
        "bybit-spot",
        "bybit-futures",
        "hyperliquid-spot",
        "hyperliquid-futures"
    ]

    @staticmethod
    def validate_cex_params(
        exchange: str,
        token: str,
        timeframe: str,
        start_epoch: int,
        end_epoch: int
    ) -> None:
        """
        Validate CEX parameters

        Args:
            exchange: Exchange name
            token: Trading pair
            timeframe: Time interval
            start_epoch: Start timestamp
            end_epoch: End timestamp

        Raises:
            ValueError: If any parameter is invalid
        """
        if exchange not in DivineDipMetric.SUPPORTED_CEX_EXCHANGES:
            raise ValueError(
                f"Invalid CEX exchange: {exchange}. "
                f"Supported: {', '.join(DivineDipMetric.SUPPORTED_CEX_EXCHANGES)}"
            )

        if timeframe not in DivineDipMetric.SUPPORTED_CEX_TIMEFRAMES:
            raise ValueError(
                f"Invalid CEX timeframe: {timeframe}. "
                f"Supported: {', '.join(DivineDipMetric.SUPPORTED_CEX_TIMEFRAMES)}"
            )

        if not token:
            raise ValueError("Token parameter is required")

        if start_epoch >= end_epoch:
            raise ValueError("start_epoch must be less than end_epoch")

        if start_epoch < 0 or end_epoch < 0:
            raise ValueError("Epoch timestamps must be positive")

    @staticmethod
    def validate_dex_params(
        chain: str,
        pool_address: str,
        timeframe: str,
        start_epoch: int,
        end_epoch: int
    ) -> None:
        """
        Validate DEX parameters

        Args:
            chain: Blockchain network
            pool_address: DEX pool address
            timeframe: Time interval
            start_epoch: Start timestamp
            end_epoch: End timestamp

        Raises:
            ValueError: If any parameter is invalid
        """
        if timeframe not in DivineDipMetric.SUPPORTED_DEX_TIMEFRAMES:
            raise ValueError(
                f"Invalid DEX timeframe: {timeframe}. "
                f"Supported: {', '.join(DivineDipMetric.SUPPORTED_DEX_TIMEFRAMES)}"
            )

        if not chain:
            raise ValueError("Chain parameter is required")

        if not pool_address:
            raise ValueError("Pool address parameter is required")

        if start_epoch >= end_epoch:
            raise ValueError("start_epoch must be less than end_epoch")

        if start_epoch < 0 or end_epoch < 0:
            raise ValueError("Epoch timestamps must be positive")

    @staticmethod
    def format_response(raw_data: Dict) -> Dict:
        """
        Format the API response into a standardized structure

        Args:
            raw_data: Raw response from the API

        Returns:
            Formatted response with metadata

        Example:
            Input: {"data": [{"t": "2024-01-01T00:00:00", "dd": 1}]}
            Output: {
                "metric": "divine_dip",
                "count": 1,
                "data": [{"timestamp": "2024-01-01T00:00:00", "divine_dip": 1}]
            }
        """
        data = raw_data.get("data", [])

        # Transform data format from {"t": ..., "dd": ...} to readable format
        formatted_data = [
            {
                "timestamp": item.get("t"),
                "divine_dip": item.get("dd")
            }
            for item in data
        ]

        return {
            "metric": "divine_dip",
            "count": len(formatted_data),
            "data": formatted_data
        }

    @staticmethod
    def calculate_statistics(data: List[Dict]) -> Dict:
        """
        Calculate basic statistics for divine_dip values

        Args:
            data: List of divine_dip data points

        Returns:
            Dictionary with statistics (count, sum, signals, etc.)
        """
        if not data:
            return {
                "total_periods": 0,
                "divine_dip_signals": 0,
                "signal_percentage": 0.0
            }

        dd_values = [item.get("divine_dip", 0) for item in data]
        signals = sum(1 for val in dd_values if val == 1)
        total = len(dd_values)

        return {
            "total_periods": total,
            "divine_dip_signals": signals,
            "signal_percentage": round((signals / total * 100) if total > 0 else 0.0, 2)
        }
