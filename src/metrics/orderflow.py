"""
Orderflow / Tradebook Metrics
Implementation of orderflow metrics from the /workbench/orderflow/ endpoint
"""

from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class OrderflowMetric:
    """
    Orderflow metrics calculator and fetcher

    Provides metrics derived from trade data including buy/sell volumes,
    trade counts, deltas, and cumulative volume delta (CVD).
    """

    # Supported metrics
    SUPPORTED_METRICS = [
        "trade_vol",
        "trade_count",
        "tradebook_delta",
        "tradebook_cumulative_delta"
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

    # Supported volume ranges (trade size tiers in USD)
    SUPPORTED_VOLUME_RANGES = [
        # From 0
        "0-1k", "0-10k", "0-100k", "0-1m", "0-10m",
        # From 1k
        "1k-10k", "1k-100k", "1k-1m", "1k-10m",
        # From 10k
        "10k-100k", "10k-1m", "10k-10m",
        # From 100k
        "100k-1m", "100k-10m",
        # From 1m
        "1m-10m"
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
        Validate orderflow metric parameters

        Args:
            metric: Metric type
            symbol: Trading pair symbol
            exchange: Exchange name
            timeframe: Time interval
            volume: Volume tier range
            epoch_low: Start epoch timestamp
            epoch_high: End epoch timestamp

        Raises:
            ValueError: If any parameter is invalid
        """
        # Validate metric
        metric_lower = metric.lower()
        if metric_lower not in OrderflowMetric.SUPPORTED_METRICS:
            raise ValueError(
                f"Invalid metric: {metric}. "
                f"Supported: {', '.join(OrderflowMetric.SUPPORTED_METRICS)}"
            )

        # Validate symbol
        if not symbol or not symbol.strip():
            raise ValueError("Symbol parameter is required")

        if not symbol.replace("USDT", "").replace("USDC", "").replace("USD", "").isalnum():
            raise ValueError("Symbol must be alphanumeric")

        # Validate exchange
        exchange_normalized = OrderflowMetric.normalize_exchange(exchange)
        if exchange_normalized not in OrderflowMetric.SUPPORTED_EXCHANGES:
            raise ValueError(
                f"Invalid exchange: {exchange}. "
                f"Supported: {', '.join(OrderflowMetric.SUPPORTED_EXCHANGES)}"
            )

        # Validate timeframe
        if timeframe not in OrderflowMetric.SUPPORTED_TIMEFRAMES:
            raise ValueError(
                f"Invalid timeframe: {timeframe}. "
                f"Supported: {', '.join(OrderflowMetric.SUPPORTED_TIMEFRAMES)}"
            )

        # Validate volume range
        volume_lower = volume.lower()
        if volume_lower not in OrderflowMetric.SUPPORTED_VOLUME_RANGES:
            raise ValueError(
                f"Invalid volume range: {volume}. "
                f"Supported: {', '.join(OrderflowMetric.SUPPORTED_VOLUME_RANGES)}"
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
            "trade_vol": ["t", "buy", "sell"],
            "trade_count": ["t", "buy", "sell"],
            "tradebook_delta": ["t", "delta"],
            "tradebook_cumulative_delta": ["t", "cvd"]
        }

        return field_map.get(metric_lower, ["t"])

    @staticmethod
    def get_volume_interpretation(volume_range: str) -> str:
        """
        Get interpretation of volume range

        Args:
            volume_range: Volume range string

        Returns:
            Human-readable interpretation
        """
        volume_lower = volume_range.lower()

        interpretations = {
            "0-1k": "Micro trades ($0-$1K) - Small retail",
            "0-10k": "Small trades ($0-$10K) - Retail",
            "0-100k": "Small-Medium trades ($0-$100K) - Retail to semi-pro",
            "1k-10k": "Medium-small trades ($1K-$10K) - Active retail",
            "10k-100k": "Medium trades ($10K-$100K) - Semi-professional",
            "100k-1m": "Large trades ($100K-$1M) - Professional",
            "1m-10m": "Whale trades ($1M-$10M) - Institutional/Whales",
            "0-1m": "Full retail spectrum ($0-$1M)",
            "0-10m": "All trade sizes ($0-$10M) - Complete market"
        }

        return interpretations.get(volume_lower, f"Trade range: {volume_range}")

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
        Calculate basic statistics for orderflow metric values

        Args:
            data: List of orderflow metric data points
            metric: Metric type

        Returns:
            Dictionary with statistics
        """
        if not data:
            return {
                "total_periods": 0,
                "analysis": "No data available"
            }

        metric_lower = metric.lower()

        # For buy/sell metrics
        if metric_lower in ["trade_vol", "trade_count"]:
            buy_values = [item.get("buy", 0) for item in data if item.get("buy") is not None]
            sell_values = [item.get("sell", 0) for item in data if item.get("sell") is not None]

            if not buy_values or not sell_values:
                return {"total_periods": len(data), "analysis": "Insufficient data"}

            total_buy = sum(buy_values)
            total_sell = sum(sell_values)
            avg_buy = total_buy / len(buy_values)
            avg_sell = total_sell / len(sell_values)

            buy_sell_ratio = total_buy / total_sell if total_sell > 0 else 0

            sentiment = "Bullish" if buy_sell_ratio > 1.1 else "Bearish" if buy_sell_ratio < 0.9 else "Neutral"

            return {
                "total_periods": len(data),
                "total_buy": round(total_buy, 2),
                "total_sell": round(total_sell, 2),
                "avg_buy_per_period": round(avg_buy, 2),
                "avg_sell_per_period": round(avg_sell, 2),
                "buy_sell_ratio": round(buy_sell_ratio, 4),
                "market_sentiment": sentiment,
                "dominant_side": "buyers" if buy_sell_ratio > 1 else "sellers"
            }

        # For delta metrics
        elif metric_lower == "tradebook_delta":
            delta_values = [item.get("delta", 0) for item in data if item.get("delta") is not None]

            if not delta_values:
                return {"total_periods": len(data), "analysis": "Insufficient data"}

            positive_periods = sum(1 for d in delta_values if d > 0)
            negative_periods = sum(1 for d in delta_values if d < 0)
            neutral_periods = len(delta_values) - positive_periods - negative_periods

            avg_delta = sum(delta_values) / len(delta_values)
            max_delta = max(delta_values)
            min_delta = min(delta_values)

            return {
                "total_periods": len(data),
                "positive_periods": positive_periods,
                "negative_periods": negative_periods,
                "neutral_periods": neutral_periods,
                "avg_delta": round(avg_delta, 2),
                "max_delta": round(max_delta, 2),
                "min_delta": round(min_delta, 2),
                "trend": "Buying pressure" if avg_delta > 0 else "Selling pressure" if avg_delta < 0 else "Balanced"
            }

        # For CVD
        elif metric_lower == "tradebook_cumulative_delta":
            cvd_values = [item.get("cvd", 0) for item in data if item.get("cvd") is not None]

            if len(cvd_values) < 2:
                return {"total_periods": len(data), "analysis": "Insufficient data for trend"}

            start_cvd = cvd_values[0]
            end_cvd = cvd_values[-1]
            cvd_change = end_cvd - start_cvd

            # Check for trend direction
            trend = "Accumulation" if cvd_change > 0 else "Distribution" if cvd_change < 0 else "Sideways"

            return {
                "total_periods": len(data),
                "start_cvd": round(start_cvd, 2),
                "end_cvd": round(end_cvd, 2),
                "cvd_change": round(cvd_change, 2),
                "trend_direction": trend,
                "strength": "Strong" if abs(cvd_change) > abs(start_cvd) * 0.1 else "Weak"
            }

        return {"total_periods": len(data)}
