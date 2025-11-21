"""
Orderbook Metrics
Implementation of orderbook metrics from the /workbench/orderbook/ endpoint
"""

from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class OrderbookMetric:
    """
    Orderbook metrics calculator and fetcher

    Provides metrics derived from orderbook data such as bid/ask ratios,
    deltas, and cumulative volume delta (CVD).
    """

    # Supported metrics
    SUPPORTED_METRICS = [
        "bid_ask",
        "bid_ask_ratio",
        "bid_ask_delta",
        "bid_ask_cvd",
        "total_volume",
        "bid_increase_decrease",
        "ask_increase_decrease",
        "bid_ask_ratio_inc_dec"
    ]

    # Supported exchanges (will be normalized to lowercase)
    SUPPORTED_EXCHANGES = [
        "binance-futures",
        "binance",
        "bybit-futures",
        "bybit",
        "hyperliquid-futures",
        "hyperliquid"
    ]

    # Supported timeframes
    SUPPORTED_TIMEFRAMES = [
        "1m", "5m", "15m", "30m",
        "1H", "4H",
        "1D", "1W", "1M"
    ]

    # Supported volume ranges
    SUPPORTED_VOLUME_RANGES = [
        # Single depth levels
        "0-1", "0-2.5", "0-5", "0-10", "0-25", "0-100",
        # From 1%
        "1-2.5", "1-5", "1-10", "1-25", "1-100",
        # From 2.5%
        "2.5-5", "2.5-10", "2.5-25", "2.5-100",
        # From 5%
        "5-10", "5-25", "5-100",
        # From 10%
        "10-25", "10-100",
        # From 25%
        "25-100"
    ]

    @staticmethod
    def normalize_exchange(exchange: str) -> str:
        """
        Normalize exchange name (lowercase, remove -spot suffix)

        Args:
            exchange: Exchange name

        Returns:
            Normalized exchange name
        """
        exchange_lower = exchange.lower()
        # Remove -spot suffix
        if exchange_lower.endswith("-spot"):
            exchange_lower = exchange_lower[:-5]
        return exchange_lower

    @staticmethod
    def validate_params(
        metric: str,
        symbol: str,
        exchange: str,
        timeframe: str,
        volume: str,
        epoch_low: int,
        epoch_high: int
    ) -> None:
        """
        Validate orderbook metric parameters

        Args:
            metric: Metric type
            symbol: Trading pair symbol
            exchange: Exchange name
            timeframe: Time interval
            volume: Volume depth range
            epoch_low: Start epoch timestamp
            epoch_high: End epoch timestamp

        Raises:
            ValueError: If any parameter is invalid
        """
        # Validate metric
        metric_lower = metric.lower()
        if metric_lower not in OrderbookMetric.SUPPORTED_METRICS:
            raise ValueError(
                f"Invalid metric: {metric}. "
                f"Supported: {', '.join(OrderbookMetric.SUPPORTED_METRICS)}"
            )

        # Validate symbol
        if not symbol or not symbol.strip():
            raise ValueError("Symbol parameter is required")

        # Validate exchange
        exchange_normalized = OrderbookMetric.normalize_exchange(exchange)
        if exchange_normalized not in OrderbookMetric.SUPPORTED_EXCHANGES:
            raise ValueError(
                f"Invalid exchange: {exchange}. "
                f"Supported: {', '.join(OrderbookMetric.SUPPORTED_EXCHANGES)}"
            )

        # Validate timeframe
        if timeframe not in OrderbookMetric.SUPPORTED_TIMEFRAMES:
            raise ValueError(
                f"Invalid timeframe: {timeframe}. "
                f"Supported: {', '.join(OrderbookMetric.SUPPORTED_TIMEFRAMES)}"
            )

        # Validate volume range
        volume_lower = volume.lower()
        if volume_lower not in OrderbookMetric.SUPPORTED_VOLUME_RANGES:
            raise ValueError(
                f"Invalid volume range: {volume}. "
                f"Supported: {', '.join(OrderbookMetric.SUPPORTED_VOLUME_RANGES)}"
            )

        # Validate epoch timestamps
        if epoch_low >= epoch_high:
            raise ValueError("epoch_low must be less than epoch_high")

        if epoch_low < 0 or epoch_high < 0:
            raise ValueError("Epoch timestamps must be positive")

    @staticmethod
    def get_response_fields(metric: str) -> List[str]:
        """
        Get expected response fields for a given metric

        Args:
            metric: Metric type

        Returns:
            List of field names expected in response
        """
        metric_lower = metric.lower()

        field_map = {
            "bid_ask": ["t", "bid", "ask"],
            "bid_ask_ratio": ["t", "bid_ask_ratio"],
            "bid_ask_delta": ["t", "bid_ask_delta"],
            "bid_ask_cvd": ["t", "cvd"],
            "total_volume": ["t", "total_volume"],
            "bid_increase_decrease": ["t", "bid_delta"],
            "ask_increase_decrease": ["t", "ask_delta"],
            "bid_ask_ratio_inc_dec": ["t", "bid_ask_ratio_delta"]
        }

        return field_map.get(metric_lower, ["t"])

    @staticmethod
    def format_response(raw_data: Dict, metric: str) -> Dict:
        """
        Format the API response into a standardized structure

        Args:
            raw_data: Raw response from the API
            metric: Metric type for proper field naming

        Returns:
            Formatted response with metadata
        """
        data = raw_data.get("data", [])

        return {
            "metric": metric.lower(),
            "count": len(data),
            "data": data
        }

    @staticmethod
    def calculate_statistics(data: List[Dict], metric: str) -> Dict:
        """
        Calculate basic statistics for orderbook metric values

        Args:
            data: List of orderbook metric data points
            metric: Metric type

        Returns:
            Dictionary with statistics
        """
        if not data:
            return {
                "total_periods": 0,
                "min": None,
                "max": None,
                "avg": None
            }

        metric_lower = metric.lower()

        # Determine which field to analyze
        value_field = None
        if metric_lower == "bid_ask":
            # For bid_ask, calculate ratio statistics
            values = []
            for item in data:
                bid = item.get("bid")
                ask = item.get("ask")
                if bid is not None and ask is not None and ask != 0:
                    values.append(bid / ask)
            value_field = "bid_ask_ratio (calculated)"
        elif metric_lower == "bid_ask_ratio":
            value_field = "bid_ask_ratio"
            values = [item.get(value_field) for item in data if item.get(value_field) is not None]
        elif metric_lower == "bid_ask_delta":
            value_field = "bid_ask_delta"
            values = [item.get(value_field) for item in data if item.get(value_field) is not None]
        elif metric_lower == "bid_ask_cvd":
            value_field = "cvd"
            values = [item.get(value_field) for item in data if item.get(value_field) is not None]
        elif metric_lower == "total_volume":
            value_field = "total_volume"
            values = [item.get(value_field) for item in data if item.get(value_field) is not None]
        else:
            # For other metrics, return basic count
            return {"total_periods": len(data)}

        if not values:
            return {
                "total_periods": len(data),
                "field_analyzed": value_field,
                "min": None,
                "max": None,
                "avg": None
            }

        return {
            "total_periods": len(data),
            "field_analyzed": value_field,
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "avg": round(sum(values) / len(values), 4)
        }
