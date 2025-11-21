"""
Binance Exchange Adapter
Handles both Spot and Futures markets for Binance
"""

from typing import Dict, List, Tuple, Optional
from ..core.base_exchange import BaseExchange


class BinanceExchange(BaseExchange):
    """Binance exchange implementation for Spot and Futures markets"""

    def __init__(self, db_handler=None):
        """
        Initialize Binance exchange handler

        Args:
            db_handler: Optional database handler instance for querying pair metadata
        """
        super().__init__(db_handler)
        self.spot_url = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
        self.futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
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
        Fetches trading and non-trading symbols from Binance API

        Args:
            url: API endpoint URL
            exchange: Exchange identifier (e.g., 'binance-spot', 'binance-futures')

        Returns:
            Tuple of (trading_symbols, non_trading_symbols)
            Each symbol dict contains 'symbol' (base asset) and 'pair' (trading pair)
        """
        quote_asset = self.default_quote_asset
        data = self._fetch_with_retry(url)

        trading_symbols = []
        non_trading_symbols = []

        if exchange == "binance-spot":
            # Single-pass filtering for better performance
            for item in data.get("symbols", []):
                if item.get("quoteAsset") == quote_asset:
                    symbol_data = {"symbol": item["baseAsset"], "pair": item["symbol"]}
                    if item.get("status") == "TRADING":
                        trading_symbols.append(symbol_data)
                    else:
                        non_trading_symbols.append(symbol_data)
        elif exchange == "binance-futures":
            # Single-pass filtering for better performance
            for item in data.get("symbols", []):
                if (item.get("quoteAsset") == quote_asset and
                    item.get("contractType") == "PERPETUAL"):
                    symbol_data = {"symbol": item["baseAsset"], "pair": item["pair"]}
                    if item.get("status") == "TRADING":
                        trading_symbols.append(symbol_data)
                    else:
                        non_trading_symbols.append(symbol_data)
        else:
            raise ValueError(f"Invalid Binance exchange type: {exchange}")

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
            exchange: Exchange identifier (e.g., 'binance-spot', 'binance-futures')
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
        limit: int = 500,
        timezone: str = "0"
    ) -> List[Dict]:
        """
        Fetch kline/candlestick data for a trading pair from Binance

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval - Supported: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
            market: Market type ('spot' or 'futures', default: 'spot')
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)
            limit: Number of klines to fetch (default: 500, max: 1000 for spot, 1500 for futures)
            timezone: Timezone offset (default: '0' for UTC)

        Returns:
            List of kline data dictionaries with keys:
                - open_time: Kline open time (ms)
                - open: Open price
                - high: High price
                - low: Low price
                - close: Close price
                - volume: Trading volume
                - close_time: Kline close time (ms)
                - quote_volume: Quote asset volume
                - trades: Number of trades
                - taker_buy_base: Taker buy base asset volume
                - taker_buy_quote: Taker buy quote asset volume

        Raises:
            ValueError: If market type is invalid or interval is not supported

        Example:
            klines = exchange.fetch_klines('BTCUSDT', '1h', limit=100)
        """
        # Validate interval
        valid_intervals = ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        if interval not in valid_intervals:
            raise ValueError(
                f"Invalid interval '{interval}'. "
                f"Supported intervals: {', '.join(valid_intervals)}"
            )

        # Validate limit
        max_limit = 1000 if market == "spot" else 1500
        if limit > max_limit:
            raise ValueError(f"Limit cannot exceed {max_limit} for {market} market")

        # Build URL based on market type
        if market == "spot":
            base_url = "https://api.binance.com/api/v3/klines"
        elif market == "futures":
            base_url = "https://fapi.binance.com/fapi/v1/klines"
        else:
            raise ValueError(f"Invalid market type '{market}'. Supported: 'spot', 'futures'")

        # Build query parameters
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }

        if market == "spot" and timezone:
            params["timeZone"] = timezone

        if start_time:
            params["startTime"] = start_time

        if end_time:
            params["endTime"] = end_time

        # Build URL with parameters
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"

        # Fetch data
        raw_data = self._fetch_with_retry(url)

        # Parse response into structured format
        klines = []
        for item in raw_data:
            klines.append({
                "open_time": item[0],
                "open": item[1],
                "high": item[2],
                "low": item[3],
                "close": item[4],
                "volume": item[5],
                "close_time": item[6],
                "quote_volume": item[7],
                "trades": item[8],
                "taker_buy_base": item[9],
                "taker_buy_quote": item[10]
            })

        return klines

    def fetch_funding_rate_history(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Fetch historical funding rate data for Binance futures

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT'). If None, returns data for all symbols
            start_time: Start time in milliseconds (optional, inclusive)
            end_time: End time in milliseconds (optional, inclusive)
            limit: Number of records to fetch (default: 100, max: 1000)

        Returns:
            List of funding rate records with keys:
                - symbol: Trading pair
                - funding_rate: Funding rate value
                - funding_time: Timestamp when funding was charged (ms)
                - mark_price: Mark price at funding time

        Example:
            # Get last 50 funding rates for BTC
            history = exchange.fetch_funding_rate_history('BTCUSDT', limit=50)

            # Get funding rates in a time range
            import time
            end = int(time.time() * 1000)
            start = end - (7 * 24 * 60 * 60 * 1000)  # 7 days ago
            history = exchange.fetch_funding_rate_history('ETHUSDT', start_time=start, end_time=end)

        Note:
            - If startTime and endTime are not sent, returns most recent 200 records
            - Default limit is 100, maximum is 1000
            - Results are ordered chronologically (oldest first)
            - Funding typically occurs every 8 hours
        """
        # Validate limit
        if limit > 1000 or limit < 1:
            raise ValueError("Limit must be between 1 and 1000")

        # Build URL
        base_url = "https://fapi.binance.com/fapi/v1/fundingRate"

        # Build query parameters
        params = {
            "limit": limit
        }

        if symbol:
            params["symbol"] = symbol

        if start_time:
            params["startTime"] = start_time

        if end_time:
            params["endTime"] = end_time

        # Build URL with parameters
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"

        # Fetch data
        raw_data = self._fetch_with_retry(url)

        # Parse response into structured format
        funding_rates = []
        for item in raw_data:
            funding_rates.append({
                "symbol": item.get("symbol"),
                "funding_rate": item.get("fundingRate"),
                "funding_time": item.get("fundingTime"),
                "mark_price": item.get("markPrice")
            })

        return funding_rates

    def fetch_funding_rate_info(self) -> List[Dict]:
        """
        Fetch funding rate configuration info for Binance futures

        Returns:
            List of funding rate info with keys:
                - symbol: Trading pair
                - adjusted_funding_rate_cap: Maximum funding rate cap
                - adjusted_funding_rate_floor: Minimum funding rate floor
                - funding_interval_hours: Funding settlement interval in hours

        Example:
            info = exchange.fetch_funding_rate_info()
            for item in info:
                print(f"{item['symbol']}: Cap={item['adjusted_funding_rate_cap']}, "
                      f"Floor={item['adjusted_funding_rate_floor']}, "
                      f"Interval={item['funding_interval_hours']}h")

        Note:
            - Returns info only for symbols that had FundingRateCap/Floor/Interval adjustments
            - Standard funding interval is 8 hours for most pairs
        """
        # Build URL
        url = "https://fapi.binance.com/fapi/v1/fundingInfo"

        # Fetch data
        raw_data = self._fetch_with_retry(url)

        # Parse response into structured format
        funding_info = []
        for item in raw_data:
            funding_info.append({
                "symbol": item.get("symbol"),
                "adjusted_funding_rate_cap": item.get("adjustedFundingRateCap"),
                "adjusted_funding_rate_floor": item.get("adjustedFundingRateFloor"),
                "funding_interval_hours": item.get("fundingIntervalHours")
            })

        return funding_info

    def fetch_open_interest(self, symbol: str) -> Dict:
        """
        Fetch current open interest for a futures symbol

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')

        Returns:
            Dictionary with keys:
                - symbol: Trading pair
                - open_interest: Current open interest value
                - timestamp: Server timestamp in milliseconds

        Example:
            oi = exchange.fetch_open_interest('BTCUSDT')
            print(f"BTC Open Interest: {oi['open_interest']}")

        Note:
            - Only available for futures contracts
            - Returns real-time open interest data
        """
        # Build URL
        base_url = "https://fapi.binance.com/fapi/v1/openInterest"

        # Build query parameters
        params = {"symbol": symbol}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"

        # Fetch data
        raw_data = self._fetch_with_retry(url)

        # Parse response
        return {
            "symbol": raw_data.get("symbol"),
            "open_interest": raw_data.get("openInterest"),
            "timestamp": raw_data.get("time")
        }

    def fetch_open_interest_history(
        self,
        symbol: str,
        period: str,
        limit: int = 30,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch historical open interest statistics for a futures symbol

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            period: Time period - Supported: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
            limit: Number of records to fetch (default: 30, max: 500)
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)

        Returns:
            List of open interest records with keys:
                - symbol: Trading pair
                - sum_open_interest: Total open interest (in contracts)
                - sum_open_interest_value: Total open interest value (in USD)
                - timestamp: Timestamp in milliseconds

        Example:
            # Get last 48 hourly OI data points
            history = exchange.fetch_open_interest_history('BTCUSDT', '1h', limit=48)

            # Get OI for specific time range
            import time
            end = int(time.time() * 1000)
            start = end - (7 * 24 * 60 * 60 * 1000)  # 7 days ago
            history = exchange.fetch_open_interest_history('ETHUSDT', '1d', start_time=start, end_time=end)

        Note:
            - Historical data limited to the latest 1 month
            - If startTime and endTime not sent, returns most recent data
            - Default limit is 30, maximum is 500
        """
        # Validate period
        valid_periods = ['5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d']
        if period not in valid_periods:
            raise ValueError(
                f"Invalid period '{period}'. "
                f"Supported periods: {', '.join(valid_periods)}"
            )

        # Validate limit
        if limit > 500 or limit < 1:
            raise ValueError("Limit must be between 1 and 500")

        # Build URL
        base_url = "https://fapi.binance.com/futures/data/openInterestHist"

        # Build query parameters
        params = {
            "symbol": symbol,
            "period": period,
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
        raw_data = self._fetch_with_retry(url)

        # Parse response into structured format
        oi_history = []
        for item in raw_data:
            oi_history.append({
                "symbol": item.get("symbol"),
                "sum_open_interest": item.get("sumOpenInterest"),
                "sum_open_interest_value": item.get("sumOpenInterestValue"),
                "timestamp": item.get("timestamp")
            })

        return oi_history

    def process_spot(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Binance spot exchange data

        Returns:
            Tuple of (active_pairs, inactive_pairs)
            Each dict contains: pair, symbol, exchange, is_active

        Example:
            ([
                {'symbol': 'BTC', 'pair': 'BTCUSDT', 'exchange': 'binance-spot', 'is_active': True},
                {'symbol': 'ETH', 'pair': 'ETHUSDT', 'exchange': 'binance-spot', 'is_active': True}
            ],
            [
                {'symbol': 'HIFI', 'pair': 'HIFIUSDT', 'exchange': 'binance-spot', 'is_active': False}
            ])
        """
        exchange = 'binance-spot'
        trading_symbols, non_trading_symbols = self.fetch_symbols_retry(
            self.spot_url, exchange
        )
        return self.generate_symbol_updates(exchange, trading_symbols, non_trading_symbols)

    def process_futures(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Binance futures exchange data

        Returns:
            Tuple of (active_pairs, inactive_pairs)
            Each dict contains: pair, symbol, exchange, is_active

        Example:
            ([
                {'symbol': 'BTC', 'pair': 'BTCUSDT', 'exchange': 'binance-futures', 'is_active': True},
                {'symbol': 'ETH', 'pair': 'ETHUSDT', 'exchange': 'binance-futures', 'is_active': True}
            ],
            [
                {'symbol': 'LUNA', 'pair': 'LUNAUSDT', 'exchange': 'binance-futures', 'is_active': False}
            ])
        """
        exchange = 'binance-futures'
        trading_symbols, non_trading_symbols = self.fetch_symbols_retry(
            self.futures_url, exchange
        )
        return self.generate_symbol_updates(exchange, trading_symbols, non_trading_symbols)
