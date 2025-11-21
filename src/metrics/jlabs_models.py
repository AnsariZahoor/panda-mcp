"""
JLabs Models
Implementation of CARI, DXY Risk, ROSI, and Token Rating from JLabs Digital
"""

from typing import Dict, List, Optional, Literal
import logging

logger = logging.getLogger(__name__)


class JLabsModels:
    """
    JLabs Models calculator and fetcher

    Provides advanced proprietary models from JLabs Digital:
    - CARI: Crypto Asset Risk Indicator (bubble detection)
    - DXY Risk: Dollar correlation analysis
    - ROSI: Relative Open Short Interest (momentum)
    - Token Rating: Multi-dimensional token scoring
    """

    # Supported metrics
    SUPPORTED_METRICS = [
        "cari",
        "dxy_risk",
        "rosi",
        "token_rating"
    ]

    # Token Rating sub-metrics
    TOKEN_RATING_SUB_METRICS = [
        "User Score",
        "Demand Shock Score",
        "Price Level Score",
        "Accumulation Score",
        "Diversity Score",
        "Price Strength Score",
        "Token Usage Score",
        "Sentiment Score",
        "Leverage Score",
        "Overall Rating"
    ]

    # Timeframe support matrix
    TIMEFRAME_SUPPORT = {
        "cari": ["1D", "1W", "1M"],
        "dxy_risk": ["1D", "1W", "1M"],
        "rosi": ["4H", "1D", "1W", "1M"],
        "token_rating": ["1D", "1W", "1M"]
    }

    @staticmethod
    def strip_usdt_suffix(symbol: str) -> str:
        """
        Strip USDT/USDC/USD suffix from symbol

        Args:
            symbol: Trading pair symbol

        Returns:
            Symbol without quote currency suffix
        """
        for suffix in ["USDT", "USDC", "USD"]:
            if symbol.upper().endswith(suffix):
                return symbol[:-len(suffix)]
        return symbol

    @staticmethod
    def validate_params(
        metric: str,
        symbol: Optional[str],
        timeframe: str,
        start_epoch: Optional[int] = None,
        end_epoch: Optional[int] = None,
        metric_param: Optional[str] = None,
        api_version: Literal["v1", "v2", "v3"] = "v1"
    ) -> None:
        """
        Validate JLabs proprietary metric parameters

        Args:
            metric: Metric type
            symbol: Trading pair symbol (optional for some metrics)
            timeframe: Time interval
            start_epoch: Start epoch timestamp (V1 only)
            end_epoch: End epoch timestamp (V1 only)
            metric_param: Sub-metric for Token Rating
            api_version: API version to use

        Raises:
            ValueError: If any parameter is invalid
        """
        metric_lower = metric.lower()

        # Validate metric
        if metric_lower not in JLabsModels.SUPPORTED_METRICS:
            raise ValueError(
                f"Invalid metric: {metric}. "
                f"Supported: {', '.join(JLabsModels.SUPPORTED_METRICS)}"
            )

        # Validate symbol requirement
        if metric_lower in ["rosi", "token_rating"] and not symbol:
            raise ValueError(f"{metric} requires a symbol parameter")

        # Validate timeframe
        if timeframe not in JLabsModels.TIMEFRAME_SUPPORT[metric_lower]:
            raise ValueError(
                f"Invalid timeframe for {metric}: {timeframe}. "
                f"Supported: {', '.join(JLabsModels.TIMEFRAME_SUPPORT[metric_lower])}"
            )

        # Validate Token Rating specific parameters
        if metric_lower == "token_rating":
            if api_version in ["v2", "v3"] and not metric_param:
                raise ValueError("Token Rating requires metric_param (sub-metric)")

            if metric_param and metric_param not in JLabsModels.TOKEN_RATING_SUB_METRICS:
                raise ValueError(
                    f"Invalid metric_param: {metric_param}. "
                    f"Supported: {', '.join(JLabsModels.TOKEN_RATING_SUB_METRICS)}"
                )

        # Validate epochs for V1
        if api_version == "v1":
            if start_epoch is None or end_epoch is None:
                raise ValueError("V1 API requires start_epoch and end_epoch")

            if start_epoch >= end_epoch:
                raise ValueError("start_epoch must be less than end_epoch")

            if start_epoch < 0 or end_epoch < 0:
                raise ValueError("Epoch timestamps must be positive")

    @staticmethod
    def format_response_v1(raw_data: Dict, metric: str) -> Dict:
        """
        Format V1 API response

        Args:
            raw_data: Raw response from V1 API
            metric: Metric type

        Returns:
            Formatted response
        """
        success = raw_data.get("success", True)
        data = raw_data.get("data", [])

        return {
            "metric": metric.lower(),
            "api_version": "v1",
            "success": success,
            "count": len(data),
            "data": data
        }

    @staticmethod
    def format_response_v2(raw_data: Dict, metric: str, metric_param: Optional[str] = None) -> Dict:
        """
        Format V2/V3 API response

        Args:
            raw_data: Raw response from V2/V3 API
            metric: Metric type
            metric_param: Sub-metric (for Token Rating)

        Returns:
            Formatted response
        """
        data = raw_data.get("data", [])

        result = {
            "metric": metric.lower(),
            "api_version": "v2/v3",
            "count": len(data),
            "data": data
        }

        if metric_param:
            result["sub_metric"] = metric_param

        return result

    @staticmethod
    def interpret_cari(value: float) -> Dict:
        """
        Interpret CARI value

        Args:
            value: CARI value (0-1 scale)

        Returns:
            Interpretation dictionary
        """
        if value < 0.3:
            risk_level = "Low"
            phase = "Accumulation"
            action = "Favorable for buying"
        elif value < 0.6:
            risk_level = "Moderate"
            phase = "Caution"
            action = "Monitor closely"
        elif value < 0.8:
            risk_level = "High"
            phase = "Bubble forming"
            action = "Consider reducing exposure"
        else:
            risk_level = "Extreme"
            phase = "Bubble territory"
            action = "High risk, avoid new positions"

        return {
            "value": round(value, 4),
            "risk_level": risk_level,
            "market_phase": phase,
            "recommendation": action
        }

    @staticmethod
    def interpret_rosi(value: float) -> Dict:
        """
        Interpret ROSI value

        Args:
            value: ROSI value (0-100 scale)

        Returns:
            Interpretation dictionary
        """
        if value < 30:
            condition = "Oversold"
            signal = "Potential buy"
        elif value < 50:
            condition = "Below neutral"
            signal = "Accumulation zone"
        elif value < 70:
            condition = "Above neutral"
            signal = "Distribution zone"
        else:
            condition = "Overbought"
            signal = "Potential sell"

        return {
            "value": round(value, 2),
            "condition": condition,
            "signal": signal
        }

    @staticmethod
    def interpret_token_rating(value: float, sub_metric: str) -> Dict:
        """
        Interpret Token Rating value

        Args:
            value: Rating value
            sub_metric: Sub-metric name

        Returns:
            Interpretation dictionary
        """
        if sub_metric == "Overall Rating":
            # 0-10 scale
            if value < 2:
                rating = "Very Weak"
            elif value < 4:
                rating = "Weak"
            elif value < 6:
                rating = "Neutral"
            elif value < 8:
                rating = "Strong"
            else:
                rating = "Very Strong"

            return {
                "value": round(value, 2),
                "rating": rating,
                "scale": "0-10"
            }
        else:
            # Other sub-metrics (interpretation varies)
            return {
                "value": round(value, 2),
                "sub_metric": sub_metric
            }

    @staticmethod
    def calculate_statistics(data: List[Dict], metric: str, api_version: str = "v1") -> Dict:
        """
        Calculate statistics for metric values

        Args:
            data: List of metric data points
            metric: Metric type
            api_version: API version used

        Returns:
            Dictionary with statistics
        """
        if not data:
            return {
                "total_periods": 0,
                "analysis": "No data available"
            }

        # Extract values based on metric type
        if metric == "cari":
            values = [item.get("value", 0) for item in data if item.get("value") is not None]
        elif metric == "dxy_risk":
            values = [item.get("v", 0) for item in data if item.get("v") is not None]
        elif metric == "rosi":
            values = [item.get("rsi", 0) for item in data if item.get("rsi") is not None]
        elif metric == "token_rating":
            values = [item.get("value", 0) for item in data if item.get("value") is not None]
        else:
            values = []

        if not values:
            return {"total_periods": len(data), "analysis": "Insufficient data"}

        min_val = min(values)
        max_val = max(values)
        avg_val = sum(values) / len(values)

        # Calculate trend
        if len(values) >= 2:
            start_val = values[0]
            end_val = values[-1]
            change = end_val - start_val
            trend = "Increasing" if change > 0 else "Decreasing" if change < 0 else "Stable"
        else:
            trend = "N/A"

        result = {
            "total_periods": len(data),
            "min": round(min_val, 4),
            "max": round(max_val, 4),
            "avg": round(avg_val, 4),
            "trend": trend
        }

        # Add metric-specific interpretations
        if metric == "cari":
            result["current_interpretation"] = JLabsModels.interpret_cari(values[-1]) if values else None
        elif metric == "rosi":
            result["current_interpretation"] = JLabsModels.interpret_rosi(values[-1]) if values else None

        return result
