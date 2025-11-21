"""
Panda MCP Server
A Model Context Protocol server built with FastMCP 2.0
Provides tools and resources for cryptocurrency exchange data
"""

from fastmcp import FastMCP
from starlette.responses import JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
# from fastmcp.server.auth.providers.auth0 import Auth0Provider

from typing import Literal, Optional, List
import json
import httpx
from .core.exchange_factory import ExchangeFactory
from .utils.export import DataExporter
from .utils.indicators import TechnicalIndicators
from .metrics.api_client import PandaMetricsClient
from .metrics.divine_dip import DivineDipMetric
from .metrics.orderbook import OrderbookMetric
from .metrics.jlabs_analytics import JLabsAnalytics
from .metrics.jlabs_models import JLabsModels
from .metrics.orderflow import OrderflowMetric


# Initialize Auth0Provider
# Environment variables provide default values, so we don't need to pass parameters:
# - FASTMCP_SERVER_AUTH_AUTH0_CONFIG_URL
# - FASTMCP_SERVER_AUTH_AUTH0_CLIENT_ID
# - FASTMCP_SERVER_AUTH_AUTH0_CLIENT_SECRET
# - FASTMCP_SERVER_AUTH_AUTH0_AUDIENCE
# - FASTMCP_SERVER_AUTH_AUTH0_BASE_URL
# auth_provider = Auth0Provider()

# Initialize the FastMCP server without authentication (for development)
mcp = FastMCP(
    name="PandaMCP",
    instructions=(
        "A versatile MCP server for cryptocurrency exchange data. "
        "Provides tools to fetch active and inactive trading pairs from multiple exchanges, "
        "kline/candlestick data, funding rates, open interest, technical indicators, "
        "panda_jlabs_metrics (Divine Dip, Slippage, Price Equilibrium), "
        "orderbook metrics (bid/ask ratios, CVD), and orderflow metrics (trade volume, deltas). "
        "Supports Binance, Bybit, and Hyperliquid exchanges (spot and futures). "
        "Use the tools to query trading pairs, market data, or access advanced panda liquidity, depth, and flow metrics."
    )
)

# Configure CORS for browser-based clients
middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Allow all origins; use specific origins for security
        allow_methods=["GET"],
        allow_headers=[
            "mcp-protocol-version",
            "mcp-session-id",
            "Authorization",
            "Content-Type",
        ],
        expose_headers=["mcp-session-id"],
    )
]



# ============================================================================
# EXCHANGE DATA TOOLS
# ============================================================================

@mcp.tool
def get_trading_pairs(
    exchange: str,
    market: str,
    status: Literal["active", "inactive", "all"] = "active"
) -> dict:
    """
    Fetch trading pairs from a cryptocurrency exchange.

    Args:
        exchange: Exchange name (e.g., 'binance')
        market: Market type (e.g., 'spot', 'futures')
        status: Filter by pair status - 'active', 'inactive', or 'all' (default: 'active')

    Returns:
        Dictionary containing trading pairs with their status

    Example:
        get_trading_pairs("binance", "spot", "active")
        Returns: {
            "exchange": "binance",
            "market": "spot",
            "count": 450,
            "pairs": [
                {"symbol": "BTC", "pair": "BTCUSDT", "exchange": "binance-spot", "is_active": true},
                ...
            ]
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Fetch all pairs for the market
            result = exchange_instance.fetch_all_pairs(market)

            # Filter based on status
            if status == "active":
                pairs = result["active"]
            elif status == "inactive":
                pairs = result["inactive"]
            else:  # all
                pairs = result["active"] + result["inactive"]

            return {
                "exchange": exchange,
                "market": market,
                "status_filter": status,
                "count": len(pairs),
                "pairs": pairs
            }
    except ValueError as e:
        # User input error (invalid exchange or market)
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "market": market
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "market": market
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "market": market
        }


@mcp.tool
def list_supported_exchanges() -> dict:
    """
    List all supported exchanges and their available markets.

    Returns:
        Dictionary with exchange information

    Example:
        list_supported_exchanges()
        Returns: {
            "exchanges": [
                {
                    "name": "binance",
                    "markets": ["spot", "futures"],
                    "description": "Binance exchange implementation..."
                }
            ]
        }
    """
    try:
        exchanges = ExchangeFactory.list_exchanges()
        exchange_info = []

        for exchange_name in exchanges:
            info = ExchangeFactory.get_exchange_info(exchange_name)
            exchange_info.append({
                "name": info["name"],
                "markets": info["supported_markets"],
                "description": info["description"].strip()
            })

        return {
            "count": len(exchanges),
            "exchanges": exchange_info
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Failed to list exchanges",
            "error_type": type(e).__name__,
            "message": str(e)
        }


@mcp.tool
def compare_exchange_pairs(
    exchange: str,
    markets: list[str]
) -> dict:
    """
    Compare trading pairs across multiple markets for the same exchange.

    Args:
        exchange: Exchange name (e.g., 'binance')
        markets: List of market types to compare (e.g., ['spot', 'futures'])

    Returns:
        Dictionary with comparison results showing pairs in each market

    Example:
        compare_exchange_pairs("binance", ["spot", "futures"])
        Returns: {
            "exchange": "binance",
            "markets_compared": ["spot", "futures"],
            "spot_only": ["PAIR1", "PAIR2"],
            "futures_only": ["PAIR3"],
            "both_markets": ["BTCUSDT", "ETHUSDT"]
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Fetch pairs from each market
            market_pairs = {}
            for market in markets:
                result = exchange_instance.fetch_all_pairs(market)
                # Get active pairs only
                market_pairs[market] = {
                    pair["pair"] for pair in result["active"]
                }

            # Compare pairs across markets
            if len(markets) == 2:
                market1, market2 = markets
                only_in_first = market_pairs[market1] - market_pairs[market2]
                only_in_second = market_pairs[market2] - market_pairs[market1]
                in_both = market_pairs[market1] & market_pairs[market2]

                return {
                    "exchange": exchange,
                    "markets_compared": markets,
                    f"{market1}_only": sorted(list(only_in_first)),
                    f"{market2}_only": sorted(list(only_in_second)),
                    "both_markets": sorted(list(in_both)),
                    "counts": {
                        f"{market1}_only": len(only_in_first),
                        f"{market2}_only": len(only_in_second),
                        "both_markets": len(in_both)
                    }
                }
            else:
                # For multiple markets, just return counts
                return {
                    "exchange": exchange,
                    "markets_compared": markets,
                    "pair_counts": {
                        market: len(pairs) for market, pairs in market_pairs.items()
                    }
                }
    except ValueError as e:
        # User input error (invalid exchange or market)
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "markets": markets
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "markets": markets
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "markets": markets
        }


@mcp.tool
def get_market_data(
    exchange: str,
    symbol: Optional[str] = None
) -> dict:
    """
    Fetch live market data including price, volume, funding rate, and open interest.

    Args:
        exchange: Exchange name (currently only 'hyperliquid' supported)
        symbol: Optional symbol filter (e.g., 'BTC', 'ETH'). If None, returns all markets.

    Returns:
        Dictionary containing market data

    Example:
        get_market_data("hyperliquid", "BTC")
        Returns: {
            "exchange": "hyperliquid",
            "symbol_filter": "BTC",
            "count": 1,
            "markets": [
                {
                    "symbol": "BTC",
                    "mark_price": "101540.0",
                    "oracle_price": "101566.0",
                    "prev_day_price": "102983.0",
                    "price_change_24h": -1.40,
                    "volume_24h_base": "33373.34864",
                    "volume_24h_usd": "3436572687.72",
                    "funding_rate": "0.0000125",
                    "open_interest": "28169.72524",
                    "premium": "-0.0002953744",
                    "max_leverage": 40
                }
            ]
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Check if exchange supports market data
            if not hasattr(exchange_instance, 'fetch_market_data'):
                return {
                    "error": "Feature not supported",
                    "error_type": "NotImplementedError",
                    "message": f"Exchange '{exchange}' does not support live market data fetching",
                    "exchange": exchange
                }

            # Fetch market data
            markets = exchange_instance.fetch_market_data(symbol)

            return {
                "exchange": exchange,
                "symbol_filter": symbol,
                "count": len(markets),
                "markets": markets
            }
    except ValueError as e:
        # User input error (invalid exchange, etc.)
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def get_klines(
    exchange: str,
    symbol: str,
    interval: str,
    market: str = "spot",
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 500,
    timezone: str = "0"
) -> dict:
    """
    Fetch kline/candlestick data for a trading pair.

    Args:
        exchange: Exchange name (e.g., 'binance')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Kline interval - '1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'
        market: Market type - 'spot' or 'futures' (default: 'spot')
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of klines to fetch (default: 500, max: 1000 for spot, 1500 for futures)
        timezone: Timezone offset for spot market (default: '0' for UTC)

    Returns:
        Dictionary containing kline data with metadata

    Example:
        get_klines("binance", "BTCUSDT", "1h", limit=100)
        Returns: {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "interval": "1h",
            "market": "spot",
            "count": 100,
            "klines": [
                {
                    "open_time": 1609459200000,
                    "open": "29000.00",
                    "high": "29500.00",
                    "low": "28800.00",
                    "close": "29200.00",
                    "volume": "1234.56",
                    "close_time": 1609462799999,
                    "quote_volume": "36000000.00",
                    "trades": 15000,
                    "taker_buy_base": "600.00",
                    "taker_buy_quote": "17500000.00"
                },
                ...
            ]
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Fetch klines
            klines = exchange_instance.fetch_klines(
                symbol=symbol,
                interval=interval,
                market=market,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
                timezone=timezone
            )

            return {
                "exchange": exchange,
                "symbol": symbol,
                "interval": interval,
                "market": market,
                "count": len(klines),
                "start_time": start_time,
                "end_time": end_time,
                "klines": klines
            }
    except ValueError as e:
        # User input error (invalid exchange, symbol, interval, etc.)
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
            "market": market
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
            "market": market
        }
    except NotImplementedError as e:
        # Exchange doesn't support klines
        return {
            "error": "Feature not supported",
            "error_type": "NotImplementedError",
            "message": str(e),
            "exchange": exchange
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol,
            "interval": interval,
            "market": market
        }


@mcp.tool
def get_funding_rate_history(
    exchange: str,
    symbol: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 100
) -> dict:
    """
    Fetch historical funding rate data for futures contracts.

    Args:
        exchange: Exchange name (currently only 'binance' supported)
        symbol: Trading pair symbol (e.g., 'BTCUSDT'). If None, returns data for all symbols
        start_time: Start time in milliseconds (optional, inclusive)
        end_time: End time in milliseconds (optional, inclusive)
        limit: Number of records to fetch (default: 100, max: 1000)

    Returns:
        Dictionary containing funding rate history

    Example:
        get_funding_rate_history("binance", "BTCUSDT", limit=50)
        Returns: {
            "exchange": "binance",
            "symbol_filter": "BTCUSDT",
            "count": 50,
            "funding_rates": [
                {
                    "symbol": "BTCUSDT",
                    "funding_rate": "0.00010000",
                    "funding_time": 1609459200000,
                    "mark_price": "29000.00"
                },
                ...
            ]
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Check if exchange supports funding rate history
            if not hasattr(exchange_instance, 'fetch_funding_rate_history'):
                return {
                    "error": "Feature not supported",
                    "error_type": "NotImplementedError",
                    "message": f"Exchange '{exchange}' does not support funding rate history",
                    "exchange": exchange
                }

            # Fetch funding rate history
            funding_rates = exchange_instance.fetch_funding_rate_history(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )

            return {
                "exchange": exchange,
                "symbol_filter": symbol,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
                "count": len(funding_rates),
                "funding_rates": funding_rates
            }
    except ValueError as e:
        # User input error (invalid limit, etc.)
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def get_funding_rate_info(exchange: str) -> dict:
    """
    Fetch funding rate configuration info (caps, floors, intervals).

    Args:
        exchange: Exchange name (currently only 'binance' supported)

    Returns:
        Dictionary containing funding rate configuration info

    Example:
        get_funding_rate_info("binance")
        Returns: {
            "exchange": "binance",
            "count": 45,
            "funding_info": [
                {
                    "symbol": "BLZUSDT",
                    "adjusted_funding_rate_cap": "0.02500000",
                    "adjusted_funding_rate_floor": "-0.02500000",
                    "funding_interval_hours": 8
                },
                ...
            ]
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Check if exchange supports funding rate info
            if not hasattr(exchange_instance, 'fetch_funding_rate_info'):
                return {
                    "error": "Feature not supported",
                    "error_type": "NotImplementedError",
                    "message": f"Exchange '{exchange}' does not support funding rate info",
                    "exchange": exchange
                }

            # Fetch funding rate info
            funding_info = exchange_instance.fetch_funding_rate_info()

            return {
                "exchange": exchange,
                "count": len(funding_info),
                "funding_info": funding_info
            }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange
        }


@mcp.tool
def get_open_interest(exchange: str, symbol: str) -> dict:
    """
    Fetch current open interest for a futures contract.

    Args:
        exchange: Exchange name (currently only 'binance' supported)
        symbol: Trading pair symbol (e.g., 'BTCUSDT')

    Returns:
        Dictionary containing current open interest data

    Example:
        get_open_interest("binance", "BTCUSDT")
        Returns: {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "open_interest": "10659.509",
            "timestamp": 1589437530011
        }
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Check if exchange supports open interest
            if not hasattr(exchange_instance, 'fetch_open_interest'):
                return {
                    "error": "Feature not supported",
                    "error_type": "NotImplementedError",
                    "message": f"Exchange '{exchange}' does not support open interest fetching",
                    "exchange": exchange
                }

            # Fetch open interest
            oi_data = exchange_instance.fetch_open_interest(symbol)

            return {
                "exchange": exchange,
                "symbol": oi_data["symbol"],
                "open_interest": oi_data["open_interest"],
                "timestamp": oi_data["timestamp"]
            }
    except ValueError as e:
        # User input error
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def get_open_interest_history(
    exchange: str,
    symbol: str,
    period: str,
    limit: int = 30,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None
) -> dict:
    """
    Fetch historical open interest statistics for a futures contract.

    Args:
        exchange: Exchange name (currently only 'binance' supported)
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        period: Time period - '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d'
        limit: Number of records to fetch (default: 30, max: 500)
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)

    Returns:
        Dictionary containing historical open interest data

    Example:
        get_open_interest_history("binance", "BTCUSDT", "1h", limit=24)
        Returns: {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "period": "1h",
            "count": 24,
            "history": [
                {
                    "symbol": "BTCUSDT",
                    "sum_open_interest": "45123.456",
                    "sum_open_interest_value": "4512345678.90",
                    "timestamp": 1589437530011
                },
                ...
            ]
        }

    Note:
        - Historical data limited to the latest 1 month
        - If startTime and endTime not sent, returns most recent data
    """
    try:
        # Use context manager for proper resource cleanup
        with ExchangeFactory.create(exchange) as exchange_instance:
            # Check if exchange supports open interest history
            if not hasattr(exchange_instance, 'fetch_open_interest_history'):
                return {
                    "error": "Feature not supported",
                    "error_type": "NotImplementedError",
                    "message": f"Exchange '{exchange}' does not support open interest history",
                    "exchange": exchange
                }

            # Fetch open interest history
            history = exchange_instance.fetch_open_interest_history(
                symbol=symbol,
                period=period,
                limit=limit,
                start_time=start_time,
                end_time=end_time
            )

            return {
                "exchange": exchange,
                "symbol": symbol,
                "period": period,
                "limit": limit,
                "start_time": start_time,
                "end_time": end_time,
                "count": len(history),
                "history": history
            }
    except ValueError as e:
        # User input error (invalid period, limit, etc.)
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol,
            "period": period
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol,
            "period": period
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol,
            "period": period
        }


# ============================================================================
# DATA EXPORT TOOLS
# ============================================================================

@mcp.tool
def export_klines(
    exchange: str,
    symbol: str,
    interval: str,
    market: str = "spot",
    file_path: Optional[str] = None,
    format: Literal["json", "csv"] = "json",
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 500
) -> dict:
    """
    Fetch kline/candlestick data and export to CSV or JSON file.

    Args:
        exchange: Exchange name ('binance' or 'bybit')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Kline interval (e.g., '1h', '1d' for Binance; '60', 'D' for Bybit)
        market: Market type ('spot' or 'futures', default: 'spot')
        file_path: Output file path (optional, auto-generated if not provided)
        format: Export format - 'json' or 'csv' (default: 'json')
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of klines to fetch (default: 500)

    Returns:
        Dictionary with export status and file details

    Example:
        export_klines(
            "binance", "BTCUSDT", "1h", market="futures",
            format="csv", limit=100
        )
    """
    try:
        # Fetch klines data
        with ExchangeFactory.create(exchange) as exchange_instance:
            klines = exchange_instance.fetch_klines(
                symbol=symbol,
                interval=interval,
                market=market,
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )

        # Generate file path if not provided
        if file_path is None:
            filename = DataExporter.generate_filename(
                exchange=exchange,
                data_type=f"klines_{interval}",
                symbol=symbol,
                extension=format
            )
            file_path = f"exports/{filename}"

        # Export based on format
        if format == "json":
            result = DataExporter.export_to_json(klines, file_path)
        else:  # csv
            result = DataExporter.export_to_csv(klines, file_path)

        # Add metadata
        result["exchange"] = exchange
        result["symbol"] = symbol
        result["interval"] = interval
        result["market"] = market

        return result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def export_funding_rate(
    exchange: str,
    symbol: str,
    file_path: Optional[str] = None,
    format: Literal["json", "csv"] = "json",
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 100
) -> dict:
    """
    Fetch funding rate history and export to CSV or JSON file.

    Args:
        exchange: Exchange name ('binance' or 'bybit')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        file_path: Output file path (optional, auto-generated if not provided)
        format: Export format - 'json' or 'csv' (default: 'json')
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of records to fetch (default: 100)

    Returns:
        Dictionary with export status and file details

    Example:
        export_funding_rate(
            "binance", "BTCUSDT", format="csv", limit=50
        )
    """
    try:
        # Fetch funding rate data
        with ExchangeFactory.create(exchange) as exchange_instance:
            if exchange == "binance":
                funding_data = exchange_instance.fetch_funding_rate_history(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit
                )
            elif exchange == "bybit":
                funding_data = exchange_instance.fetch_funding_rate_history(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                    market="futures"
                )
            else:
                raise ValueError(f"Funding rate not supported for {exchange}")

        # Generate file path if not provided
        if file_path is None:
            filename = DataExporter.generate_filename(
                exchange=exchange,
                data_type="funding_rate",
                symbol=symbol,
                extension=format
            )
            file_path = f"exports/{filename}"

        # Export based on format
        if format == "json":
            result = DataExporter.export_to_json(funding_data, file_path)
        else:  # csv
            result = DataExporter.export_to_csv(funding_data, file_path)

        # Add metadata
        result["exchange"] = exchange
        result["symbol"] = symbol
        result["data_type"] = "funding_rate"

        return result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def export_open_interest(
    exchange: str,
    symbol: str,
    file_path: Optional[str] = None,
    format: Literal["json", "csv"] = "json",
    interval: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 50
) -> dict:
    """
    Fetch open interest data and export to CSV or JSON file.

    Args:
        exchange: Exchange name ('binance' or 'bybit')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        file_path: Output file path (optional, auto-generated if not provided)
        format: Export format - 'json' or 'csv' (default: 'json')
        interval: For Bybit: '5min', '15min', '30min', '1h', '4h', '1d' (required)
                 For Binance history: '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d' (optional)
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of records to fetch (default: 50)

    Returns:
        Dictionary with export status and file details

    Example:
        export_open_interest(
            "bybit", "BTCUSDT", interval="1h", format="csv", limit=24
        )
    """
    try:
        # Fetch open interest data
        with ExchangeFactory.create(exchange) as exchange_instance:
            if exchange == "binance":
                if interval:
                    # Historical OI
                    oi_data = exchange_instance.fetch_open_interest_history(
                        symbol=symbol,
                        period=interval,
                        start_time=start_time,
                        end_time=end_time,
                        limit=limit
                    )
                else:
                    # Current OI (returns single dict, convert to list)
                    oi_data = [exchange_instance.fetch_open_interest(symbol=symbol)]
            elif exchange == "bybit":
                if not interval:
                    raise ValueError("interval parameter is required for Bybit open interest")
                oi_data = exchange_instance.fetch_open_interest(
                    symbol=symbol,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                    market="futures"
                )
            else:
                raise ValueError(f"Open interest not supported for {exchange}")

        # Generate file path if not provided
        if file_path is None:
            data_type = f"open_interest_{interval}" if interval else "open_interest"
            filename = DataExporter.generate_filename(
                exchange=exchange,
                data_type=data_type,
                symbol=symbol,
                extension=format
            )
            file_path = f"exports/{filename}"

        # Export based on format
        if format == "json":
            result = DataExporter.export_to_json(oi_data, file_path)
        else:  # csv
            result = DataExporter.export_to_csv(oi_data, file_path)

        # Add metadata
        result["exchange"] = exchange
        result["symbol"] = symbol
        result["data_type"] = "open_interest"
        if interval:
            result["interval"] = interval

        return result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def export_trading_pairs(
    exchange: str,
    market: str,
    status: Literal["active", "inactive", "all"] = "active",
    file_path: Optional[str] = None,
    format: Literal["json", "csv"] = "json"
) -> dict:
    """
    Fetch trading pairs and export to CSV or JSON file.

    Args:
        exchange: Exchange name (e.g., 'binance', 'bybit', 'hyperliquid')
        market: Market type (e.g., 'spot', 'futures')
        status: Filter by pair status - 'active', 'inactive', or 'all' (default: 'active')
        file_path: Output file path (optional, auto-generated if not provided)
        format: Export format - 'json' or 'csv' (default: 'json')

    Returns:
        Dictionary with export status and file details

    Example:
        export_trading_pairs(
            "binance", "spot", status="active", format="csv"
        )
    """
    try:
        # Fetch trading pairs
        with ExchangeFactory.create(exchange) as exchange_instance:
            result = exchange_instance.fetch_all_pairs(market)

            # Filter based on status
            if status == "active":
                pairs = result["active"]
            elif status == "inactive":
                pairs = result["inactive"]
            else:  # all
                pairs = result["active"] + result["inactive"]

        # Generate file path if not provided
        if file_path is None:
            filename = DataExporter.generate_filename(
                exchange=exchange,
                data_type=f"trading_pairs_{market}_{status}",
                extension=format
            )
            file_path = f"exports/{filename}"

        # Export based on format
        if format == "json":
            export_result = DataExporter.export_to_json(pairs, file_path)
        else:  # csv
            export_result = DataExporter.export_to_csv(pairs, file_path)

        # Add metadata
        export_result["exchange"] = exchange
        export_result["market"] = market
        export_result["status_filter"] = status

        return export_result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "market": market
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "market": market
        }


# ============================================================================
# TECHNICAL INDICATORS TOOLS
# ============================================================================

@mcp.tool
def calculate_indicator(
    exchange: str,
    symbol: str,
    interval: str,
    indicator: Literal[
        "RSI", "MACD", "SMA", "EMA", "BB", "ATR",
        "STOCH", "CCI", "OBV", "VWAP", "MFI", "KC"
    ],
    market: str = "spot",
    period: Optional[int] = None,
    limit: int = 100
) -> dict:
    """
    Calculate a single technical indicator for a trading pair.

    Args:
        exchange: Exchange name ('binance', 'bybit')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Kline interval (e.g., '1h', '1d' for Binance; '60', 'D' for Bybit)
        indicator: Indicator name - RSI, MACD, SMA, EMA, BB (Bollinger Bands),
                  ATR, STOCH (Stochastic), CCI, OBV, VWAP, MFI, KC (Keltner Channels)
        market: Market type ('spot' or 'futures', default: 'spot')
        period: Period for calculation (optional, uses defaults if not provided)
        limit: Number of klines to fetch (default: 100)

    Returns:
        Dictionary with indicator values and metadata

    Example:
        calculate_indicator("binance", "BTCUSDT", "1h", "RSI", period=14)
    """
    try:
        # Fetch klines data
        with ExchangeFactory.create(exchange) as exchange_instance:
            klines = exchange_instance.fetch_klines(
                symbol=symbol,
                interval=interval,
                market=market,
                limit=limit
            )

        # Calculate indicator based on type
        if indicator == "RSI":
            result = TechnicalIndicators.calculate_rsi(klines, period=period or 14)
        elif indicator == "MACD":
            result = TechnicalIndicators.calculate_macd(klines)
        elif indicator == "SMA":
            result = TechnicalIndicators.calculate_sma(klines, period=period or 20)
        elif indicator == "EMA":
            result = TechnicalIndicators.calculate_ema(klines, period=period or 20)
        elif indicator == "BB":
            result = TechnicalIndicators.calculate_bollinger_bands(klines, period=period or 20)
        elif indicator == "ATR":
            result = TechnicalIndicators.calculate_atr(klines, period=period or 14)
        elif indicator == "STOCH":
            result = TechnicalIndicators.calculate_stochastic(klines)
        elif indicator == "CCI":
            result = TechnicalIndicators.calculate_cci(klines, period=period or 20)
        elif indicator == "OBV":
            result = TechnicalIndicators.calculate_obv(klines)
        elif indicator == "VWAP":
            result = TechnicalIndicators.calculate_vwap(klines)
        elif indicator == "MFI":
            result = TechnicalIndicators.calculate_mfi(klines, period=period or 14)
        elif indicator == "KC":
            result = TechnicalIndicators.calculate_keltner_channels(klines, period=period or 20)
        else:
            raise ValueError(f"Unknown indicator: {indicator}")

        # Add metadata
        result["exchange"] = exchange
        result["symbol"] = symbol
        result["interval"] = interval
        result["market"] = market

        return result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def calculate_multiple_indicators(
    exchange: str,
    symbol: str,
    interval: str,
    indicators: List[str],
    market: str = "spot",
    limit: int = 100
) -> dict:
    """
    Calculate multiple technical indicators at once for a trading pair.

    Args:
        exchange: Exchange name ('binance', 'bybit')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Kline interval (e.g., '1h', '1d' for Binance; '60', 'D' for Bybit)
        indicators: List of indicator names (e.g., ['RSI', 'MACD', 'BB', 'EMA_50'])
                   Supported: RSI, MACD, SMA_20, SMA_50, SMA_200, EMA_20, EMA_50,
                             EMA_200, BB, ATR, STOCH, OBV, VWAP, MFI, CCI
        market: Market type ('spot' or 'futures', default: 'spot')
        limit: Number of klines to fetch (default: 100)

    Returns:
        Dictionary with all calculated indicators

    Example:
        calculate_multiple_indicators(
            "binance", "BTCUSDT", "1h",
            indicators=["RSI", "MACD", "EMA_50", "BB"]
        )
    """
    try:
        # Fetch klines data
        with ExchangeFactory.create(exchange) as exchange_instance:
            klines = exchange_instance.fetch_klines(
                symbol=symbol,
                interval=interval,
                market=market,
                limit=limit
            )

        # Calculate multiple indicators
        result = TechnicalIndicators.calculate_multiple_indicators(klines, indicators)

        # Add metadata
        result["exchange"] = exchange
        result["symbol"] = symbol
        result["interval"] = interval
        result["market"] = market
        result["klines_count"] = len(klines)

        return result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


@mcp.tool
def export_indicator_data(
    exchange: str,
    symbol: str,
    interval: str,
    indicators: List[str],
    market: str = "spot",
    file_path: Optional[str] = None,
    format: Literal["json", "csv"] = "json",
    limit: int = 100
) -> dict:
    """
    Calculate indicators and export to CSV or JSON file.

    Args:
        exchange: Exchange name ('binance', 'bybit')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Kline interval (e.g., '1h', '1d' for Binance; '60', 'D' for Bybit)
        indicators: List of indicator names to calculate
        market: Market type ('spot' or 'futures', default: 'spot')
        file_path: Output file path (optional, auto-generated if not provided)
        format: Export format - 'json' or 'csv' (default: 'json')
        limit: Number of klines to fetch (default: 100)

    Returns:
        Dictionary with export status and file details

    Example:
        export_indicator_data(
            "binance", "BTCUSDT", "1h",
            indicators=["RSI", "MACD", "SMA_50"],
            format="csv"
        )
    """
    try:
        # Fetch klines and calculate indicators
        with ExchangeFactory.create(exchange) as exchange_instance:
            klines = exchange_instance.fetch_klines(
                symbol=symbol,
                interval=interval,
                market=market,
                limit=limit
            )

        # Calculate indicators
        indicator_result = TechnicalIndicators.calculate_multiple_indicators(klines, indicators)
        data = indicator_result["data"]

        # Generate file path if not provided
        if file_path is None:
            indicators_str = "_".join(indicators[:3])  # Use first 3 indicators in filename
            filename = DataExporter.generate_filename(
                exchange=exchange,
                data_type=f"indicators_{indicators_str}_{interval}",
                symbol=symbol,
                extension=format
            )
            file_path = f"exports/{filename}"

        # Export based on format
        if format == "json":
            export_result = DataExporter.export_to_json(data, file_path)
        else:  # csv
            export_result = DataExporter.export_to_csv(data, file_path)

        # Add metadata
        export_result["exchange"] = exchange
        export_result["symbol"] = symbol
        export_result["interval"] = interval
        export_result["market"] = market
        export_result["indicators_calculated"] = indicator_result["indicators_calculated"]

        return export_result

    except ValueError as e:
        return {
            "status": "error",
            "error_type": "ValueError",
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }
    except Exception as e:
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange": exchange,
            "symbol": symbol
        }


# ============================================================================
# PANDA METRICS TOOLS
# ============================================================================

@mcp.tool
def get_divine_dip_metric(
    exchange_type: Literal["CEX", "DEX"],
    timeframe: str,
    start_epoch: int,
    end_epoch: int,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    version: int = 4,
    # CEX parameters
    exchange: Optional[str] = None,
    token: Optional[str] = None,
    # DEX parameters
    chain: Optional[str] = None,
    pool_address: Optional[str] = None,
    include_statistics: bool = True
) -> dict:
    """
    Fetch Divine Dip metric from panda-backend-api.

    Divine Dip identifies potential buying opportunities by detecting significant price dips.

    Configuration can be provided via parameters or environment variables (.env file):
    - PANDA_BACKEND_API_URL: Base URL for the API
    - PANDA_API_KEY: API key for authentication

    Args:
        exchange_type: Either 'CEX' or 'DEX'
        timeframe: Time interval - CEX: '15m', '30m', '1H', '4H', '1D' | DEX: '1H', '4H', '1D'
        start_epoch: Start time as Unix timestamp in seconds
        end_epoch: End time as Unix timestamp in seconds
        api_base_url: Base URL for panda-backend-api (defaults to PANDA_BACKEND_API_URL env var)
        api_key: API key for authentication (defaults to PANDA_API_KEY env var)
        version: API version (default: 4)
        exchange: Exchange name for CEX (e.g., 'binance-spot', 'bybit-futures')
        token: Trading pair for CEX (e.g., 'BTCUSDT')
        chain: Blockchain network for DEX (e.g., 'ethereum', 'bsc', 'solana')
        pool_address: DEX pool address (e.g., '0x1234...')
        include_statistics: Include statistical summary (default: True)

    Returns:
        Dictionary containing divine_dip metric data with timestamps and values

    Example (CEX):
        # Using environment variables from .env file
        get_divine_dip_metric(
            exchange_type="CEX",
            exchange="bybit-futures",
            token="BTCUSDT",
            timeframe="1D",
            start_epoch=1648923900,
            end_epoch=1763231400
        )

        # Or explicitly providing credentials
        get_divine_dip_metric(
            exchange_type="CEX",
            exchange="bybit-futures",
            token="BTCUSDT",
            timeframe="1D",
            start_epoch=1648923900,
            end_epoch=1763231400,
            api_base_url="https://api.example.com",
            api_key="your-api-key"
        )
        Returns: {
            "metric": "divine_dip",
            "exchange_type": "CEX",
            "exchange": "bybit-futures",
            "token": "BTCUSDT",
            "timeframe": "1D",
            "count": 100,
            "data": [
                {"timestamp": "2024-01-01T00:00:00", "divine_dip": 1},
                {"timestamp": "2024-01-02T00:00:00", "divine_dip": 0}
            ],
            "statistics": {
                "total_periods": 100,
                "divine_dip_signals": 15,
                "signal_percentage": 15.0
            }
        }

    Example (DEX):
        get_divine_dip_metric(
            exchange_type="DEX",
            chain="ethereum",
            pool_address="0x1234...",
            timeframe="1D",
            start_epoch=1648923900,
            end_epoch=1763231400,
            api_base_url="https://api.example.com",
            api_key="your-api-key"
        )
    """
    try:
        # Validate parameters based on exchange type
        if exchange_type == "CEX":
            if not exchange or not token:
                return {
                    "error": "Invalid input",
                    "error_type": "ValueError",
                    "message": "CEX metrics require 'exchange' and 'token' parameters",
                    "exchange_type": exchange_type
                }

            # Validate CEX parameters
            try:
                DivineDipMetric.validate_cex_params(
                    exchange=exchange,
                    token=token,
                    timeframe=timeframe,
                    start_epoch=start_epoch,
                    end_epoch=end_epoch
                )
            except ValueError as e:
                return {
                    "error": "Invalid input",
                    "error_type": "ValueError",
                    "message": str(e),
                    "exchange_type": exchange_type,
                    "exchange": exchange,
                    "token": token,
                    "timeframe": timeframe
                }

        elif exchange_type == "DEX":
            if not chain or not pool_address:
                return {
                    "error": "Invalid input",
                    "error_type": "ValueError",
                    "message": "DEX metrics require 'chain' and 'pool_address' parameters",
                    "exchange_type": exchange_type
                }

            # Validate DEX parameters
            try:
                DivineDipMetric.validate_dex_params(
                    chain=chain,
                    pool_address=pool_address,
                    timeframe=timeframe,
                    start_epoch=start_epoch,
                    end_epoch=end_epoch
                )
            except ValueError as e:
                return {
                    "error": "Invalid input",
                    "error_type": "ValueError",
                    "message": str(e),
                    "exchange_type": exchange_type,
                    "chain": chain,
                    "pool_address": pool_address,
                    "timeframe": timeframe
                }
        else:
            return {
                "error": "Invalid input",
                "error_type": "ValueError",
                "message": f"Invalid exchange_type: {exchange_type}. Must be 'CEX' or 'DEX'",
                "exchange_type": exchange_type
            }

        # Create API client and fetch data (will use env vars if not provided)
        try:
            client = PandaMetricsClient(base_url=api_base_url, api_key=api_key)
        except ValueError as e:
            return {
                "error": "Configuration error",
                "error_type": "ValueError",
                "message": str(e),
                "hint": "Set PANDA_BACKEND_API_URL in .env file or provide api_base_url parameter"
            }

        with client:
            raw_data = client.fetch_metric(
                metric="divine_dip",
                exchange_type=exchange_type,
                timeframe=timeframe,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                version=version,
                exchange=exchange,
                token=token,
                chain=chain,
                pool_address=pool_address
            )

        # Format response
        result = DivineDipMetric.format_response(raw_data)

        # Add request metadata
        result["exchange_type"] = exchange_type
        result["timeframe"] = timeframe
        result["start_epoch"] = start_epoch
        result["end_epoch"] = end_epoch
        result["version"] = version

        if exchange_type == "CEX":
            result["exchange"] = exchange
            result["token"] = token
        else:
            result["chain"] = chain
            result["pool_address"] = pool_address

        # Add statistics if requested
        if include_statistics:
            result["statistics"] = DivineDipMetric.calculate_statistics(result["data"])

        return result

    except ValueError as e:
        # User input error
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "exchange_type": exchange_type
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "exchange_type": exchange_type
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "exchange_type": exchange_type
        }


@mcp.tool
def get_orderbook_metric(
    metric: str,
    symbol: str,
    exchange: str,
    timeframe: str,
    volume: str,
    epoch_low: int,
    epoch_high: int,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    include_statistics: bool = True
) -> dict:
    """
    Fetch orderbook metrics from panda-backend-api.

    Retrieves metrics derived from orderbook data such as bid/ask ratios,
    deltas, and cumulative volume delta (CVD).

    Configuration can be provided via parameters or environment variables (.env file):
    - PANDA_BACKEND_API_URL: Base URL for the API
    - PANDA_API_KEY: API key for authentication

    Args:
        metric: Type of orderbook metric
                Options: 'bid_ask', 'bid_ask_ratio', 'bid_ask_delta', 'bid_ask_cvd',
                        'total_volume', 'bid_increase_decrease', 'ask_increase_decrease',
                        'bid_ask_ratio_inc_dec'
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        exchange: Exchange name (e.g., 'binance-futures', 'bybit-futures', 'hyperliquid-futures')
        timeframe: Time interval - '1m', '5m', '15m', '30m', '1H', '4H', '1D', '1W', '1M'
        volume: Volume depth range (% from best bid/ask)
                Single levels: '0-1', '0-2.5', '0-5', '0-10', '0-25', '0-100'
                Ranges: '1-2.5', '1-5', '1-10', '5-10', '10-25', etc.
        epoch_low: Start time as Unix timestamp in seconds
        epoch_high: End time as Unix timestamp in seconds
        api_base_url: Base URL for panda-backend-api (defaults to PANDA_BACKEND_API_URL env var)
        api_key: API key for authentication (defaults to PANDA_API_KEY env var)
        include_statistics: Include statistical summary (default: True)

    Returns:
        Dictionary containing orderbook metric data with timestamps and values

    Example (Bid-Ask Ratio):
        get_orderbook_metric(
            metric="bid_ask_ratio",
            symbol="BTCUSDT",
            exchange="binance-futures",
            timeframe="1D",
            volume="0-1",
            epoch_low=1628360700,
            epoch_high=1763317860
        )
        Returns: {
            "metric": "bid_ask_ratio",
            "symbol": "BTCUSDT",
            "exchange": "binance-futures",
            "timeframe": "1D",
            "volume": "0-1",
            "count": 100,
            "data": [
                {"t": "2024-01-01", "bid_ask_ratio": 1.275},
                {"t": "2024-01-02", "bid_ask_ratio": 1.320}
            ],
            "statistics": {
                "total_periods": 100,
                "field_analyzed": "bid_ask_ratio",
                "min": 0.85,
                "max": 1.45,
                "avg": 1.12
            }
        }

    Example (CVD):
        get_orderbook_metric(
            metric="bid_ask_cvd",
            symbol="BTCUSDT",
            exchange="binance-futures",
            timeframe="1H",
            volume="0-10",
            epoch_low=1628360700,
            epoch_high=1763317860
        )
    """
    try:
        # Validate parameters
        try:
            OrderbookMetric.validate_params(
                metric=metric,
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                volume=volume,
                epoch_low=epoch_low,
                epoch_high=epoch_high
            )
        except ValueError as e:
            return {
                "error": "Invalid input",
                "error_type": "ValueError",
                "message": str(e),
                "metric": metric,
                "symbol": symbol,
                "exchange": exchange
            }

        # Create API client and fetch data (will use env vars if not provided)
        try:
            client = PandaMetricsClient(base_url=api_base_url, api_key=api_key)
        except ValueError as e:
            return {
                "error": "Configuration error",
                "error_type": "ValueError",
                "message": str(e),
                "hint": "Set PANDA_BACKEND_API_URL in .env file or provide api_base_url parameter"
            }

        with client:
            raw_data = client.fetch_orderbook_metric(
                metric=metric,
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                volume=volume,
                epoch_low=epoch_low,
                epoch_high=epoch_high
            )

        # Format response
        result = OrderbookMetric.format_response(raw_data, metric)

        # Add request metadata
        result["symbol"] = symbol
        result["exchange"] = OrderbookMetric.normalize_exchange(exchange)
        result["timeframe"] = timeframe
        result["volume"] = volume.lower()
        result["epoch_low"] = epoch_low
        result["epoch_high"] = epoch_high

        # Add statistics if requested
        if include_statistics:
            result["statistics"] = OrderbookMetric.calculate_statistics(
                result["data"],
                metric
            )

        return result

    except ValueError as e:
        # User input error
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }


@mcp.tool
def get_jlabs_metric(
    metric: str,
    symbol: str,
    time_delta: int,
    start_epoch: int,
    end_epoch: int,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    include_statistics: bool = True
) -> dict:
    """
    Fetch JLabs V1 metrics (slippage or price_equilibrium) from panda-backend-api.

    These metrics analyze liquidity and price movement characteristics:
    - Slippage: Measures price impact and market depth
    - Price Equilibrium: Measures absorption capacity

    Both metrics use 30-minute time binning.

    Configuration can be provided via parameters or environment variables (.env file):
    - PANDA_BACKEND_API_URL: Base URL for the API
    - PANDA_API_KEY: API key for authentication

    Args:
        metric: Type of metric ('slippage' or 'price_equilibrium')
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        time_delta: Timezone offset in minutes
                    Common values: 0 (UTC), 330 (India), 480 (China),
                                  -300 (US Eastern), -420 (US Pacific)
        start_epoch: Start time as Unix timestamp in seconds
        end_epoch: End time as Unix timestamp in seconds
        api_base_url: Base URL for panda-backend-api (defaults to PANDA_BACKEND_API_URL env var)
        api_key: API key for authentication (defaults to PANDA_API_KEY env var)
        include_statistics: Include statistical summary (default: True)

    Returns:
        Dictionary containing metric data with timestamps and values

    Example (Slippage):
        get_jlabs_metric(
            metric="slippage",
            symbol="BTCUSDT",
            time_delta=330,
            start_epoch=1758110100,
            end_epoch=1763247780
        )
        Returns: {
            "metric": "slippage",
            "success": true,
            "symbol": "BTCUSDT",
            "time_delta": 330,
            "count": 100,
            "data": [
                {"timestamp": "2024-11-16T00:00:00", "value": 125.5},
                {"timestamp": "2024-11-16T00:30:00", "value": 132.8}
            ],
            "statistics": {
                "total_periods": 100,
                "min": 98.3,
                "max": 156.7,
                "avg": 128.4,
                "std_dev": 15.2,
                "interpretation": {
                    "liquidity_level": "medium",
                    "note": "Lower slippage indicates higher liquidity"
                }
            }
        }

    Example (Price Equilibrium):
        get_jlabs_metric(
            metric="price_equilibrium",
            symbol="ETHUSDT",
            time_delta=0,  # UTC
            start_epoch=1758110100,
            end_epoch=1763247780
        )

    Metric Interpretations:
        Slippage:
            - Lower values = Higher liquidity, easier to execute large orders
            - Higher values = Lower liquidity, larger price impact
            - Use to find optimal execution times

        Price Equilibrium:
            - Higher values = More price stability, strong absorption
            - Lower values = Less stable, potential for volatility
            - Use to identify support/resistance levels
    """
    try:
        # Validate parameters
        try:
            JLabsAnalytics.validate_params(
                metric=metric,
                symbol=symbol,
                time_delta=time_delta,
                start_epoch=start_epoch,
                end_epoch=end_epoch
            )
        except ValueError as e:
            return {
                "error": "Invalid input",
                "error_type": "ValueError",
                "message": str(e),
                "metric": metric,
                "symbol": symbol
            }

        # Create API client and fetch data (will use env vars if not provided)
        try:
            client = PandaMetricsClient(base_url=api_base_url, api_key=api_key)
        except ValueError as e:
            return {
                "error": "Configuration error",
                "error_type": "ValueError",
                "message": str(e),
                "hint": "Set PANDA_BACKEND_API_URL in .env file or provide api_base_url parameter"
            }

        with client:
            raw_data = client.fetch_jlabs_v1_metric(
                metric=metric,
                symbol=symbol,
                time_delta=time_delta,
                start_epoch=start_epoch,
                end_epoch=end_epoch
            )

        # Format response
        result = JLabsAnalytics.format_response(raw_data, metric)

        # Add request metadata
        result["symbol"] = symbol
        result["time_delta"] = time_delta
        result["start_epoch"] = start_epoch
        result["end_epoch"] = end_epoch

        # Add statistics if requested
        if include_statistics:
            result["statistics"] = JLabsAnalytics.calculate_statistics(
                result["data"],
                metric
            )

        return result

    except ValueError as e:
        # User input error
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }


@mcp.tool
def get_orderflow_metric(
    metric: str,
    symbol: str,
    exchange: str,
    timeframe: str,
    volume: str,
    epoch_low: int,
    epoch_high: int,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    include_statistics: bool = True
) -> dict:
    """
    Fetch orderflow/tradebook metrics from panda-backend-api.

    Retrieves metrics derived from trade data including buy/sell volumes,
    trade counts, deltas, and cumulative volume delta (CVD). Analyzes market
    sentiment and order flow dynamics by tracking aggressive buy/sell orders.

    Configuration can be provided via parameters or environment variables (.env file):
    - PANDA_BACKEND_API_URL: Base URL for the API
    - PANDA_API_KEY: API key for authentication

    Args:
        metric: Type of orderflow metric
                Options: 'trade_vol', 'trade_count', 'tradebook_delta', 'tradebook_cumulative_delta'
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        exchange: Exchange name (e.g., 'binance-futures', 'bybit-futures', 'hyperliquid-futures')
        timeframe: Time interval - '1m', '5m', '15m', '30m', '1H', '4H', '1D', '1W', '1M'
        volume: Volume tier range (trade size in USD)
                Ranges: '0-1k', '0-10k', '1k-10k', '10k-100k', '100k-1m', '1m-10m', etc.
                Examples:
                - '0-1k': Micro trades ($0-$1K) - Small retail
                - '1k-10k': Medium-small trades - Active retail
                - '100k-1m': Large trades - Professional
                - '1m-10m': Whale trades - Institutional
        epoch_low: Start time as Unix timestamp in seconds
        epoch_high: End time as Unix timestamp in seconds
        api_base_url: Base URL for panda-backend-api (defaults to PANDA_BACKEND_API_URL env var)
        api_key: API key for authentication (defaults to PANDA_API_KEY env var)
        include_statistics: Include statistical summary (default: True)

    Returns:
        Dictionary containing orderflow metric data with timestamps and values

    Example (Trade Volume):
        get_orderflow_metric(
            metric="trade_vol",
            symbol="BTCUSDT",
            exchange="binance-futures",
            timeframe="4H",
            volume="0-1k",
            epoch_low=1758110100,
            epoch_high=1763249400
        )
        Returns: {
            "metric": "trade_vol",
            "symbol": "BTCUSDT",
            "exchange": "binance-futures",
            "timeframe": "4H",
            "volume": "0-1k",
            "volume_interpretation": "Micro trades ($0-$1K) - Small retail",
            "count": 100,
            "data": [
                {"t": "2024-11-16T00:00:00Z", "buy": 12500000, "sell": 9800000},
                {"t": "2024-11-16T04:00:00Z", "buy": 15200000, "sell": 11300000}
            ],
            "statistics": {
                "total_periods": 100,
                "total_buy": 1250000000,
                "total_sell": 980000000,
                "buy_sell_ratio": 1.2755,
                "market_sentiment": "Bullish",
                "dominant_side": "buyers"
            }
        }

    Example (CVD):
        get_orderflow_metric(
            metric="tradebook_cumulative_delta",
            symbol="BTCUSDT",
            exchange="bybit-futures",
            timeframe="1D",
            volume="1m-10m",  # Whale trades only
            epoch_low=1758110100,
            epoch_high=1763249400
        )

    Metric Interpretations:
        trade_vol:
            - Analyze distribution of trade sizes by market participant type
            - Compare buy vs sell volumes to gauge sentiment

        trade_count:
            - High count + low volume = Retail activity
            - Low count + high volume = Institutional activity

        tradebook_delta:
            - Positive delta = Buying pressure
            - Negative delta = Selling pressure
            - Track delta changes for sentiment shifts

        tradebook_cumulative_delta:
            - Rising CVD = Accumulation
            - Falling CVD = Distribution
            - CVD divergence from price = potential reversal
    """
    try:
        # Validate parameters
        try:
            OrderflowMetric.validate_params(
                metric=metric,
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                volume=volume,
                epoch_low=epoch_low,
                epoch_high=epoch_high
            )
        except ValueError as e:
            return {
                "error": "Invalid input",
                "error_type": "ValueError",
                "message": str(e),
                "metric": metric,
                "symbol": symbol,
                "exchange": exchange
            }

        # Create API client and fetch data (will use env vars if not provided)
        try:
            client = PandaMetricsClient(base_url=api_base_url, api_key=api_key)
        except ValueError as e:
            return {
                "error": "Configuration error",
                "error_type": "ValueError",
                "message": str(e),
                "hint": "Set PANDA_BACKEND_API_URL in .env file or provide api_base_url parameter"
            }

        with client:
            raw_data = client.fetch_orderflow_metric(
                metric=metric,
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                volume=volume,
                epoch_low=epoch_low,
                epoch_high=epoch_high
            )

        # Format response
        result = OrderflowMetric.format_response(raw_data, metric)

        # Add request metadata
        result["symbol"] = symbol
        result["exchange"] = OrderflowMetric.normalize_exchange(exchange)
        result["timeframe"] = timeframe
        result["volume"] = volume.lower()
        result["volume_interpretation"] = OrderflowMetric.get_volume_interpretation(volume)
        result["epoch_low"] = epoch_low
        result["epoch_high"] = epoch_high

        # Add statistics if requested
        if include_statistics:
            result["statistics"] = OrderflowMetric.calculate_statistics(
                result["data"],
                metric
            )

        return result

    except ValueError as e:
        # User input error
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "metric": metric,
            "symbol": symbol
        }


@mcp.tool
def get_jlabs_model(
    metric: Literal["cari", "dxy_risk", "rosi", "token_rating"],
    timeframe: str,
    symbol: Optional[str] = None,
    start_epoch: Optional[int] = None,
    end_epoch: Optional[int] = None,
    metric_param: Optional[str] = None,
    api_version: Literal["v1", "v2", "v3"] = "v1",
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    include_statistics: bool = True
) -> dict:
    """
    Fetch JLabs proprietary models (CARI, DXY Risk, ROSI, Token Rating) from panda-backend-api.

    These are advanced proprietary models from JLabs Digital:
    - CARI: Crypto Asset Risk Indicator (bubble detection)
    - DXY Risk: Dollar correlation analysis
    - ROSI: Relative Open Short Interest (momentum indicator)
    - Token Rating: Multi-dimensional token scoring

    Configuration can be provided via parameters or environment variables (.env file):
    - PANDA_BACKEND_API_URL: Base URL for the API
    - PANDA_API_KEY: API key for authentication

    Args:
        metric: Type of model ('cari', 'dxy_risk', 'rosi', 'token_rating')
        timeframe: Time interval
                  CARI/DXY Risk: '1D', '1W', '1M'
                  ROSI: '4H', '1D', '1W', '1M'
                  Token Rating: '1D', '1W', '1M'
        symbol: Trading pair symbol (required for ROSI and Token Rating)
                Examples: 'BTC', 'ETH', 'BTCUSDT'
                Note: USDT suffix is automatically stripped
        start_epoch: Start time as Unix timestamp in seconds (required for V1 API)
        end_epoch: End time as Unix timestamp in seconds (required for V1 API)
        metric_param: Sub-metric for Token Rating (required for V2/V3 API)
                     Options: 'User Score', 'Demand Shock Score', 'Price Level Score',
                             'Accumulation Score', 'Diversity Score', 'Price Strength Score',
                             'Token Usage Score', 'Sentiment Score', 'Leverage Score',
                             'Overall Rating'
        api_version: API version to use ('v1', 'v2', 'v3', default: 'v1')
        api_base_url: Base URL for panda-backend-api (defaults to PANDA_BACKEND_API_URL env var)
        api_key: API key for authentication (defaults to PANDA_API_KEY env var)
        include_statistics: Include statistical summary (default: True)

    Returns:
        Dictionary containing model data with timestamps and values

    Example (CARI - Market Risk):
        get_jlabs_model(
            metric="cari",
            timeframe="1D",
            start_epoch=1758110100,
            end_epoch=1763247780,
            api_version="v1"
        )
        Returns: {
            "metric": "cari",
            "api_version": "v1",
            "timeframe": "1D",
            "count": 60,
            "data": [
                {"timestamp": "2024-11-16", "value": 0.45},
                {"timestamp": "2024-11-17", "value": 0.52}
            ],
            "statistics": {
                "min": 0.32,
                "max": 0.75,
                "avg": 0.51,
                "trend": "Increasing",
                "current_interpretation": {
                    "value": 0.52,
                    "risk_level": "Moderate",
                    "market_phase": "Caution",
                    "recommendation": "Monitor closely"
                }
            }
        }

    Example (ROSI - Momentum):
        get_jlabs_model(
            metric="rosi",
            symbol="BTC",
            timeframe="1D",
            start_epoch=1758110100,
            end_epoch=1763247780
        )

    Example (Token Rating - V2 API):
        get_jlabs_model(
            metric="token_rating",
            symbol="ETH",
            timeframe="1W",
            metric_param="Overall Rating",
            api_version="v2"
        )

    Model Interpretations:
        CARI (0-1 scale):
            < 0.3: Low risk, accumulation phase
            0.3-0.6: Moderate risk, caution
            0.6-0.8: High risk, bubble forming
            > 0.8: Extreme risk, bubble territory

        ROSI (0-100 scale):
            < 30: Oversold, potential buy
            30-50: Below neutral, accumulation
            50-70: Above neutral, distribution
            > 70: Overbought, potential sell

        Token Rating (0-10 scale):
            < 2: Very Weak
            2-4: Weak
            4-6: Neutral
            6-8: Strong
            > 8: Very Strong
    """
    try:
        # Strip USDT suffix from symbol if provided
        if symbol:
            symbol = JLabsModels.strip_usdt_suffix(symbol)

        # Validate parameters
        try:
            JLabsModels.validate_params(
                metric=metric,
                symbol=symbol,
                timeframe=timeframe,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                metric_param=metric_param,
                api_version=api_version
            )
        except ValueError as e:
            return {
                "error": "Invalid input",
                "error_type": "ValueError",
                "message": str(e),
                "metric": metric
            }

        # Create API client and fetch data (will use env vars if not provided)
        try:
            client = PandaMetricsClient(base_url=api_base_url, api_key=api_key)
        except ValueError as e:
            return {
                "error": "Configuration error",
                "error_type": "ValueError",
                "message": str(e),
                "hint": "Set PANDA_BACKEND_API_URL in .env file or provide api_base_url parameter"
            }

        with client:
            # Fetch data based on API version
            if api_version == "v1":
                raw_data = client.fetch_jlabs_proprietary_v1(
                    metric=metric,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_epoch=start_epoch,
                    end_epoch=end_epoch
                )
                result = JLabsModels.format_response_v1(raw_data, metric)
            else:  # v2 or v3
                raw_data = client.fetch_jlabs_proprietary_v2(
                    metric=metric,
                    symbol=symbol,
                    timeframe=timeframe,
                    metric_param=metric_param,
                    api_version=api_version
                )
                result = JLabsModels.format_response_v2(raw_data, metric, metric_param)

        # Add request metadata
        result["timeframe"] = timeframe
        if symbol:
            result["symbol"] = symbol
        if start_epoch:
            result["start_epoch"] = start_epoch
        if end_epoch:
            result["end_epoch"] = end_epoch

        # Add statistics if requested
        if include_statistics and result.get("data"):
            result["statistics"] = JLabsModels.calculate_statistics(
                result["data"],
                metric,
                api_version
            )

        return result

    except ValueError as e:
        # User input error
        return {
            "error": "Invalid input",
            "error_type": "ValueError",
            "message": str(e),
            "metric": metric
        }
    except httpx.HTTPError as e:
        # Network or API error
        return {
            "error": "API request failed",
            "error_type": "HTTPError",
            "message": str(e),
            "metric": metric
        }
    except Exception as e:
        # Unexpected error
        return {
            "error": "Unexpected error",
            "error_type": type(e).__name__,
            "message": str(e),
            "metric": metric
        }


# ============================================================================
# MCP RESOURCES
# ============================================================================

@mcp.resource("exchange://list")
def get_exchanges_resource() -> str:
    """
    MCP Resource: List of all supported exchanges

    Returns:
        JSON string with exchange information
    """
    result = list_supported_exchanges()
    return json.dumps(result, indent=2)


@mcp.resource("exchange://{exchange}/{market}/active")
def get_active_pairs_resource(exchange: str, market: str) -> str:
    """
    MCP Resource: Active trading pairs for a specific exchange and market

    Args:
        exchange: Exchange name
        market: Market type

    Returns:
        JSON string with active trading pairs
    """
    # Resources use tools which already handle context management
    result = get_trading_pairs(exchange, market, "active")
    return json.dumps(result, indent=2)


@mcp.resource("exchange://{exchange}/{market}/inactive")
def get_inactive_pairs_resource(exchange: str, market: str) -> str:
    """
    MCP Resource: Inactive trading pairs for a specific exchange and market

    Args:
        exchange: Exchange name
        market: Market type

    Returns:
        JSON string with inactive trading pairs
    """
    # Resources use tools which already handle context management
    result = get_trading_pairs(exchange, market, "inactive")
    return json.dumps(result, indent=2)


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    return JSONResponse({"status": "healthy", "service": "mcp-server"})



# Add a protected tool to test authentication
# Commented out since authentication is disabled
# @mcp.tool
# async def get_token_info() -> dict:
#     """Returns information about the Auth0 token."""
#     from fastmcp.server.dependencies import get_access_token
#
#     token = get_access_token()
#     print(token)
#
#     return {
#         "issuer": token.claims.get("iss"),
#         "audience": token.claims.get("aud"),
#         "scope": token.claims.get("scope")
#     }

app = mcp.http_app(middleware=middleware)