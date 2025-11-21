"""
JLabs Analytics
Implementation of slippage and price equilibrium metrics
"""

from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class JLabsAnalytics:
    """
    JLabs Analytics calculator and fetcher

    Provides slippage and price equilibrium metrics:
    - Slippage: Measures price impact and market depth
    - Price Equilibrium: Measures absorption capacity
    """

    SUPPORTED_METRICS = [
        "slippage",
        "price_equilibrium"
    ]

    # Common timezone offsets (in minutes)
    COMMON_TIMEZONES = {
        "UTC": 0,
        "India": 330,        # UTC+5:30
        "China": 480,        # UTC+8:00
        "US_Eastern": -300,  # UTC-5:00
        "US_Pacific": -420,  # UTC-7:00
        "UK": 0,             # UTC+0:00
        "Japan": 540,        # UTC+9:00
        "Australia": 600     # UTC+10:00
    }

    @staticmethod
    def validate_params(
        metric: str,
        symbol: str,
        time_delta: int,
        start_epoch: int,
        end_epoch: int
    ) -> None:
        """
        Validate JLabs V1 metric parameters

        Args:
            metric: Metric type ('slippage' or 'price_equilibrium')
            symbol: Trading pair symbol
            time_delta: Timezone offset in minutes
            start_epoch: Start epoch timestamp
            end_epoch: End epoch timestamp

        Raises:
            ValueError: If any parameter is invalid
        """
        # Validate metric
        metric_lower = metric.lower()
        if metric_lower not in JLabsMetricV1.SUPPORTED_METRICS:
            raise ValueError(
                f"Invalid metric: {metric}. "
                f"Supported: {', '.join(JLabsMetricV1.SUPPORTED_METRICS)}"
            )

        # Validate symbol
        if not symbol or not symbol.strip():
            raise ValueError("Symbol parameter is required")

        if len(symbol) > 10:
            raise ValueError("Symbol must be maximum 10 characters")

        if not symbol.isalnum():
            raise ValueError("Symbol must be alphanumeric")

        # Validate time_delta
        if not isinstance(time_delta, int):
            raise ValueError("time_delta must be an integer (timezone offset in minutes)")

        # Reasonable timezone range check (-12 to +14 hours in minutes)
        if time_delta < -720 or time_delta > 840:
            logger.warning(
                f"time_delta {time_delta} is outside typical range (-720 to +840 minutes). "
                "This may be intentional but could indicate an error."
            )

        # Validate epoch timestamps
        if start_epoch >= end_epoch:
            raise ValueError("start_epoch must be less than end_epoch")

        if start_epoch < 0 or end_epoch < 0:
            raise ValueError("Epoch timestamps must be positive")

    @staticmethod
    def get_timezone_offset(timezone_name: str) -> int:
        """
        Get timezone offset in minutes by name

        Args:
            timezone_name: Timezone name (e.g., 'India', 'UTC', 'US_Eastern')

        Returns:
            Offset in minutes

        Raises:
            ValueError: If timezone name not found
        """
        if timezone_name not in JLabsMetricV1.COMMON_TIMEZONES:
            available = ", ".join(JLabsMetricV1.COMMON_TIMEZONES.keys())
            raise ValueError(
                f"Unknown timezone: {timezone_name}. "
                f"Available: {available}. "
                "Or provide time_delta directly in minutes."
            )

        return JLabsMetricV1.COMMON_TIMEZONES[timezone_name]

    @staticmethod
    def format_response(raw_data: Dict, metric: str) -> Dict:
        """
        Format the API response into a standardized structure

        Args:
            raw_data: Raw response from the API
            metric: Metric type

        Returns:
            Formatted response with metadata

        Example:
            Input: {"success": true, "data": [{"t": "2024-01-01T00:00:00", "v": 125.5}]}
            Output: {
                "metric": "slippage",
                "success": true,
                "count": 1,
                "data": [{"timestamp": "2024-01-01T00:00:00", "value": 125.5}]
            }
        """
        success = raw_data.get("success", True)
        data = raw_data.get("data", [])

        # Transform data format from {"t": ..., "v": ...} to readable format
        formatted_data = [
            {
                "timestamp": item.get("t"),
                "value": item.get("v")
            }
            for item in data
        ]

        return {
            "metric": metric.lower(),
            "success": success,
            "count": len(formatted_data),
            "data": formatted_data
        }

    @staticmethod
    def calculate_statistics(data: List[Dict], metric: str) -> Dict:
        """
        Calculate basic statistics for metric values

        Args:
            data: List of metric data points
            metric: Metric type

        Returns:
            Dictionary with statistics
        """
        if not data:
            return {
                "total_periods": 0,
                "min": None,
                "max": None,
                "avg": None,
                "std_dev": None
            }

        values = [item.get("value") for item in data if item.get("value") is not None]

        if not values:
            return {
                "total_periods": len(data),
                "min": None,
                "max": None,
                "avg": None,
                "std_dev": None
            }

        min_val = min(values)
        max_val = max(values)
        avg_val = sum(values) / len(values)

        # Calculate standard deviation
        variance = sum((x - avg_val) ** 2 for x in values) / len(values)
        std_dev = variance ** 0.5

        result = {
            "total_periods": len(data),
            "min": round(min_val, 4),
            "max": round(max_val, 4),
            "avg": round(avg_val, 4),
            "std_dev": round(std_dev, 4)
        }

        # Add metric-specific interpretation
        if metric.lower() == "slippage":
            result["interpretation"] = {
                "liquidity_level": "high" if avg_val < 100 else "medium" if avg_val < 200 else "low",
                "note": "Lower slippage indicates higher liquidity"
            }
        elif metric.lower() == "price_equilibrium":
            result["interpretation"] = {
                "stability_level": "high" if avg_val > 2000 else "medium" if avg_val > 1000 else "low",
                "note": "Higher absorption indicates more price stability"
            }

        return result
