"""
Base Exchange Interface
Abstract base class for cryptocurrency exchange adapters
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import time

logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    """Abstract base class for exchange implementations"""

    def __init__(self, db_handler=None, cache_ttl: int = 60):
        """
        Initialize base exchange handler

        Args:
            db_handler: Optional database handler for querying pair metadata
            cache_ttl: Cache time-to-live in seconds (default: 60)
        """
        self.db_handler = db_handler
        self._client: Optional[httpx.Client] = None
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[float, Dict]] = {}  # {market_type: (timestamp, data)}

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialized HTTP client"""
        if self._client is None:
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def close(self):
        """Explicitly close the HTTP client"""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures client cleanup"""
        self.close()
        return False

    def __del__(self):
        """Cleanup fallback - should not be relied upon"""
        if hasattr(self, '_client') and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass  # Silently ignore errors during cleanup

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _fetch_with_retry(self, url: str) -> dict:
        """
        Fetch data from URL with retry logic

        Args:
            url: API endpoint URL

        Returns:
            JSON response as dictionary

        Raises:
            httpx.HTTPError: If request fails after retries
        """
        logger.info(f"Fetching data from: {url}")
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()

    def fetch_symbols_retry(self, url: str, exchange: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Wrapper for fetch_symbols_from_exchange with consistent interface

        Args:
            url: API endpoint URL
            exchange: Exchange identifier

        Returns:
            Tuple of (trading_symbols, non_trading_symbols)
        """
        return self.fetch_symbols_from_exchange(url, exchange)

    @abstractmethod
    def fetch_symbols_from_exchange(self, url: str, exchange: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch trading and non-trading symbols from exchange API

        Args:
            url: API endpoint URL
            exchange: Exchange identifier (e.g., 'binance-spot')

        Returns:
            Tuple of (trading_symbols, non_trading_symbols)
            Each list contains dicts with 'symbol' and 'pair' keys
        """
        pass

    @classmethod
    @abstractmethod
    def get_supported_markets(cls) -> List[str]:
        """
        Get list of supported market types for this exchange

        Returns:
            List of market identifiers (e.g., ['spot', 'futures'])
        """
        pass

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        market: str = "spot",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500
    ) -> List[Dict]:
        """
        Fetch kline/candlestick data for a trading pair

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval (e.g., '1m', '5m', '1h', '1d')
            market: Market type (default: 'spot')
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)
            limit: Number of klines to fetch (default: 500)

        Returns:
            List of kline data dictionaries

        Note:
            This is a default implementation that should be overridden
            by specific exchange implementations for optimal performance
        """
        raise NotImplementedError(
            f"Kline fetching not implemented for {self.__class__.__name__}"
        )

    def generate_symbol_updates_with_non_trading(
        self,
        exchange: str,
        trading_pairs: List[Dict],
        non_trading_pairs: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate symbol updates by comparing trading and non-trading pairs

        This is a simplified version that doesn't require database.
        Override this method if you need database integration.

        Args:
            exchange: Exchange identifier
            trading_pairs: List of active trading pairs
            non_trading_pairs: List of inactive pairs

        Returns:
            Tuple of (active_pairs, inactive_pairs)
        """
        # Add exchange info and active status
        active = [
            {
                **pair,
                "exchange": exchange,
                "is_active": True
            }
            for pair in trading_pairs
        ]

        inactive = [
            {
                **pair,
                "exchange": exchange,
                "is_active": False
            }
            for pair in non_trading_pairs
        ]

        return active, inactive

    def fetch_all_pairs(self, market_type: str, use_cache: bool = True) -> Dict[str, List[Dict]]:
        """
        Fetch all pairs for a given market type with optional caching

        Args:
            market_type: Market type (e.g., 'spot', 'futures')
            use_cache: Whether to use cached data if available (default: True)

        Returns:
            Dictionary with 'active' and 'inactive' keys containing pair lists
        """
        if market_type not in self.__class__.get_supported_markets():
            raise ValueError(
                f"Unsupported market type '{market_type}'. "
                f"Supported markets: {self.__class__.get_supported_markets()}"
            )

        # Check cache
        if use_cache and market_type in self._cache:
            timestamp, cached_data = self._cache[market_type]
            if time.time() - timestamp < self.cache_ttl:
                logger.info(f"Using cached data for {market_type}")
                return cached_data

        # Use the specific market processing method
        method_name = f"process_{market_type}"
        if not hasattr(self, method_name):
            raise NotImplementedError(f"Method {method_name} not implemented")

        method = getattr(self, method_name)
        active, inactive = method()

        result = {
            "active": active,
            "inactive": inactive
        }

        # Update cache
        if use_cache:
            self._cache[market_type] = (time.time(), result)

        return result
