"""
Bybit Exchange Adapter
Handles both Spot and Futures markets for Bybit
"""

from typing import Dict, List, Tuple, Optional
from ..core.base_exchange import BaseExchange


class BybitExchange(BaseExchange):
    """Bybit exchange implementation for Spot and Futures markets"""

    def __init__(self, db_handler=None):
        """
        Initialize Bybit exchange handler

        Args:
            db_handler: Optional database handler instance for querying pair metadata
        """
        super().__init__(db_handler)
        self.spot_url = "https://api.bybit.com/v5/market/instruments-info?category=spot&status=Trading&limit=1000"
        self.futures_url = "https://api.bybit.com/v5/market/instruments-info?category=linear&status=Trading&limit=1000"
        self.default_quote_asset = "USDT"

    @classmethod
    def get_supported_markets(cls) -> List[str]:
        """
        Get list of supported market types

        Returns:
            List of market identifiers
        """
        return ["spot", "futures"]

    def fetch_symbols_from_exchange(self, url: str, exchange: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetches trading and non-trading symbols from Bybit API

        Args:
            url: API endpoint URL
            exchange: Exchange identifier (e.g., 'bybit-spot', 'bybit-futures')

        Returns:
            Tuple of (trading_symbols, non_trading_symbols)
            Each symbol dict contains 'symbol' (base asset) and 'pair' (trading pair)
        """
        quote_asset = self.default_quote_asset
        data = self._fetch_with_retry(url)

        # Get the result list from Bybit API response
        result_list = data.get("result", {}).get("list", [])

        if exchange == "bybit-spot":
            # For spot, we filter by status=Trading (already in URL)
            trading_symbols = [
                {"symbol": item.get("baseCoin"), "pair": item.get("symbol")}
                for item in result_list
                if item.get("quoteCoin") == quote_asset and item.get("status") == "Trading"
            ]

            # For inactive pairs, we'd need a separate API call with status=Closed
            # For now, return empty list for non-trading symbols
            non_trading_symbols = []

        elif exchange == "bybit-futures":
            # For futures, filter by LinearPerpetual contract type
            trading_symbols = [
                {"symbol": item.get("baseCoin"), "pair": item.get("symbol")}
                for item in result_list
                if (item.get("quoteCoin") == quote_asset and
                    item.get("contractType") == "LinearPerpetual" and
                    item.get("status") == "Trading")
            ]

            # For inactive pairs, return empty list
            non_trading_symbols = []

        else:
            raise ValueError(f"Invalid Bybit exchange type: {exchange}")

        return trading_symbols, non_trading_symbols

    def generate_symbol_updates(
        self,
        exchange: str,
        trading_pairs: List[Dict],
        non_trading_pairs: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate symbol updates for database comparison

        Args:
            exchange: Exchange identifier (e.g., 'bybit-spot', 'bybit-futures')
            trading_pairs: List of dictionaries with trading pair data from API
            non_trading_pairs: List of dictionaries with non-trading pair data from API

        Returns:
            Tuple of (active_pairs, inactive_pairs)
        """
        return self.generate_symbol_updates_with_non_trading(
            exchange, trading_pairs, non_trading_pairs
        )

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        market: str = "spot",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 200,
        timezone: str = "0"
    ) -> List[Dict]:
        """
        Fetch kline/candlestick data for a trading pair from Bybit

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval - Supported: 1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, W, M
                     (numbers are in minutes)
            market: Market type ('spot' or 'futures', default: 'spot')
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)
            limit: Number of klines to fetch (default: 200, max: 1000)
            timezone: Timezone offset (ignored - for API compatibility only)

        Returns:
            List of kline data dictionaries with keys:
                - open_time: Kline start time (ms)
                - open: Open price
                - high: High price
                - low: Low price
                - close: Close price
                - volume: Trading volume
                - turnover: Turnover value

        Raises:
            ValueError: If market type is invalid or interval is not supported

        Example:
            klines = exchange.fetch_klines('BTCUSDT', '60', market='spot', limit=100)

        Note:
            - Interval values: 1,3,5,15,30,60,120,240,360,720 (minutes), D (day), W (week), M (month)
            - Results are sorted in reverse by startTime (most recent first)
            - For spot: volume is base coin, turnover is quote coin
            - For futures: volume is base coin, turnover is quote coin (USDT/USDC)
        """
        # Validate interval
        valid_intervals = ['1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M']
        if interval not in valid_intervals:
            raise ValueError(
                f"Invalid interval '{interval}'. "
                f"Supported intervals: {', '.join(valid_intervals)}"
            )

        # Validate limit
        if limit > 1000 or limit < 1:
            raise ValueError("Limit must be between 1 and 1000")

        # Determine category based on market type
        if market == "spot":
            category = "spot"
        elif market == "futures":
            category = "linear"  # USDT perpetuals
        else:
            raise ValueError(f"Invalid market type '{market}'. Supported: 'spot', 'futures'")

        # Build URL
        base_url = "https://api.bybit.com/v5/market/kline"

        # Build query parameters
        params = {
            "symbol": symbol,
            "interval": interval,
            "category": category,
            "limit": limit
        }

        if start_time:
            params["start"] = start_time

        if end_time:
            params["end"] = end_time

        # Build URL with parameters
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"

        # Fetch data
        response = self._fetch_with_retry(url)

        # Parse response
        if response.get("retCode") != 0:
            raise Exception(f"Bybit API error: {response.get('retMsg', 'Unknown error')}")

        raw_data = response.get("result", {}).get("list", [])

        # Parse response into structured format
        # Bybit returns: [startTime, open, high, low, close, volume, turnover]
        klines = []
        for item in raw_data:
            klines.append({
                "open_time": int(item[0]),
                "open": item[1],
                "high": item[2],
                "low": item[3],
                "close": item[4],
                "volume": item[5],
                "turnover": item[6]
            })

        # Reverse the list since Bybit returns newest first, but we want oldest first
        klines.reverse()

        return klines

    def fetch_funding_rate_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 200,
        market: str = "futures"
    ) -> List[Dict]:
        """
        Fetch historical funding rate data for Bybit futures

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start_time: Start time in milliseconds (optional, must be used with end_time)
            end_time: End time in milliseconds (optional)
            limit: Number of records to fetch (default: 200, max: 200)
            market: Market type ('futures' for linear USDT perpetuals, default: 'futures')

        Returns:
            List of funding rate records with keys:
                - symbol: Trading pair
                - funding_rate: Funding rate value
                - funding_rate_timestamp: Timestamp when funding was charged (ms)

        Example:
            # Get last 100 funding rates
            history = exchange.fetch_funding_rate_history('BTCUSDT', limit=100)

            # Get funding rates in a time range
            import time
            end = int(time.time() * 1000)
            start = end - (7 * 24 * 60 * 60 * 1000)  # 7 days ago
            history = exchange.fetch_funding_rate_history('ETHUSDT', start_time=start, end_time=end)

        Note:
            - Passing only startTime returns an error (must include endTime)
            - Passing only endTime retrieves records through that timestamp
            - Omitting both returns 200 most recent records
            - Default limit is 200, maximum is 200
        """
        # Validate limit
        if limit > 200 or limit < 1:
            raise ValueError("Limit must be between 1 and 200")

        # Validate startTime and endTime combination
        if start_time is not None and end_time is None:
            raise ValueError("If startTime is provided, endTime must also be provided")

        # Determine category based on market type
        if market == "futures":
            category = "linear"  # USDT perpetuals
        else:
            raise ValueError(f"Invalid market type '{market}'. Supported: 'futures'")

        # Build URL
        base_url = "https://api.bybit.com/v5/market/funding/history"

        # Build query parameters
        params = {
            "category": category,
            "symbol": symbol,
            "limit": limit
        }

        if start_time:
            params["startTime"] = start_time

        if end_time:
            params["endTime"] = end_time

        # Build URL with parameters
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"

        # Fetch data
        response = self._fetch_with_retry(url)

        # Parse response
        if response.get("retCode") != 0:
            raise Exception(f"Bybit API error: {response.get('retMsg', 'Unknown error')}")

        raw_data = response.get("result", {}).get("list", [])

        # Parse response into structured format
        funding_rates = []
        for item in raw_data:
            funding_rates.append({
                "symbol": item.get("symbol"),
                "funding_rate": item.get("fundingRate"),
                "funding_rate_timestamp": item.get("fundingRateTimestamp")
            })

        return funding_rates

    def fetch_open_interest(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 50,
        market: str = "futures"
    ) -> List[Dict]:
        """
        Fetch open interest data for Bybit futures

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Data granularity - '5min', '15min', '30min', '1h', '4h', '1d'
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)
            limit: Number of records to fetch (default: 50, max: 200)
            market: Market type ('futures' for linear USDT perpetuals, default: 'futures')

        Returns:
            List of open interest records with keys:
                - symbol: Trading pair
                - open_interest: Open interest value
                - timestamp: Timestamp in milliseconds

        Example:
            # Get last 24 hourly OI data points
            oi_data = exchange.fetch_open_interest('BTCUSDT', '1h', limit=24)

            # Get OI for specific time range
            import time
            end = int(time.time() * 1000)
            start = end - (7 * 24 * 60 * 60 * 1000)  # 7 days ago
            oi_data = exchange.fetch_open_interest('ETHUSDT', '1d', start_time=start, end_time=end)

        Note:
            - For linear contracts (BTCUSDT), values are in base asset (BTC)
            - For inverse contracts (BTCUSD), values are in USD
            - Historical data limited to post-launch dates
            - Default limit is 50, maximum is 200
        """
        # Validate interval
        valid_intervals = ['5min', '15min', '30min', '1h', '4h', '1d']
        if interval not in valid_intervals:
            raise ValueError(
                f"Invalid interval '{interval}'. "
                f"Supported intervals: {', '.join(valid_intervals)}"
            )

        # Validate limit
        if limit > 200 or limit < 1:
            raise ValueError("Limit must be between 1 and 200")

        # Determine category based on market type
        if market == "futures":
            category = "linear"  # USDT perpetuals
        else:
            raise ValueError(f"Invalid market type '{market}'. Supported: 'futures'")

        # Build URL
        base_url = "https://api.bybit.com/v5/market/open-interest"

        # Build query parameters
        params = {
            "category": category,
            "symbol": symbol,
            "intervalTime": interval,
            "limit": limit
        }

        if start_time:
            params["startTime"] = start_time

        if end_time:
            params["endTime"] = end_time

        # Build URL with parameters
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"

        # Fetch data
        response = self._fetch_with_retry(url)

        # Parse response
        if response.get("retCode") != 0:
            raise Exception(f"Bybit API error: {response.get('retMsg', 'Unknown error')}")

        result = response.get("result", {})
        symbol_from_response = result.get("symbol")
        raw_data = result.get("list", [])

        # Parse response into structured format
        oi_data = []
        for item in raw_data:
            oi_data.append({
                "symbol": symbol_from_response,
                "open_interest": item.get("openInterest"),
                "timestamp": item.get("timestamp")
            })

        return oi_data

    def process_spot(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Bybit spot exchange data

        Returns:
            Tuple of (active_pairs, inactive_pairs)
            Each dict contains: pair, symbol, exchange, is_active

        Example:
            ([
                {'symbol': 'BTC', 'pair': 'BTCUSDT', 'exchange': 'bybit-spot', 'is_active': True},
                {'symbol': 'ETH', 'pair': 'ETHUSDT', 'exchange': 'bybit-spot', 'is_active': True}
            ],
            [])
        """
        exchange = 'bybit-spot'
        trading_symbols, non_trading_symbols = self.fetch_symbols_retry(
            self.spot_url, exchange
        )
        return self.generate_symbol_updates(exchange, trading_symbols, non_trading_symbols)

    def process_futures(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Bybit futures exchange data

        Returns:
            Tuple of (active_pairs, inactive_pairs)
            Each dict contains: pair, symbol, exchange, is_active

        Example:
            ([
                {'symbol': 'BTC', 'pair': 'BTCUSDT', 'exchange': 'bybit-futures', 'is_active': True},
                {'symbol': 'ETH', 'pair': 'ETHUSDT', 'exchange': 'bybit-futures', 'is_active': True}
            ],
            [])
        """
        exchange = 'bybit-futures'
        trading_symbols, non_trading_symbols = self.fetch_symbols_retry(
            self.futures_url, exchange
        )
        return self.generate_symbol_updates(exchange, trading_symbols, non_trading_symbols)
