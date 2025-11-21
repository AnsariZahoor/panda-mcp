"""
Hyperliquid Exchange Adapter
Handles both Spot and Futures markets for Hyperliquid
"""

from typing import Dict, List, Tuple, Optional
from ..core.base_exchange import BaseExchange
import logging

logger = logging.getLogger(__name__)


class HyperliquidExchange(BaseExchange):
    """Hyperliquid exchange implementation for Spot and Futures markets"""

    def __init__(self, db_handler=None):
        """
        Initialize Hyperliquid exchange handler

        Args:
            db_handler: Optional database handler instance
        """
        super().__init__(db_handler)
        self.spot_url = "https://api.hyperliquid.xyz/info"
        self.futures_url = "https://api.hyperliquid.xyz/info"

        # Symbol normalization mapping
        self.symbol_mapping = {
            'USDT0': 'USDT',
            'USDC': 'USDC',
        }

    @classmethod
    def get_supported_markets(cls) -> List[str]:
        """
        Get list of supported market types

        Returns:
            List of market identifiers
        """
        return ["spot", "futures"]

    def _normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol names (e.g., USDT0 -> USDT)

        Args:
            symbol: Original symbol name

        Returns:
            Normalized symbol name
        """
        return self.symbol_mapping.get(symbol, symbol)

    def _fetch_with_retry_post(self, url: str, payload: dict) -> dict:
        """
        Make POST request to Hyperliquid API with retry logic

        Args:
            url: API endpoint URL
            payload: Request payload

        Returns:
            JSON response data
        """
        from tenacity import retry, stop_after_attempt, wait_exponential

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10)
        )
        def _make_request():
            logger.info(f"Fetching data from: {url} with payload: {payload}")
            response = self.client.post(url, json=payload)
            response.raise_for_status()
            return response.json()

        return _make_request()

    def fetch_symbols_from_exchange(self, url: str, exchange: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetches trading and non-trading symbols from Hyperliquid API

        Args:
            url: API endpoint URL
            exchange: Exchange identifier (e.g., 'hyperliquid-spot', 'hyperliquid-futures')

        Returns:
            Tuple of (trading_symbols, non_trading_symbols)
            Each symbol dict contains 'symbol' (base asset) and 'pair' (trading pair)
        """
        if exchange == "hyperliquid-spot":
            # Fetch spot market data
            payload = {'type': 'spotMeta'}
            data = self._fetch_with_retry_post(url, payload)

            # Parse tokens
            tokens_map = {}
            for token in data.get('tokens', []):
                tokens_map[token['index']] = {
                    'symbol': self._normalize_symbol(token['name']),
                    'address': token.get('tokenId', ''),
                    'decimals': token.get('weiDecimals', 0)
                }

            # Parse universe (trading pairs)
            trading_symbols = []
            for pair in data.get('universe', []):
                token_indices = pair.get('tokens', [])
                if len(token_indices) >= 2:
                    base_idx, quote_idx = token_indices[0], token_indices[1]

                    if base_idx in tokens_map and quote_idx in tokens_map:
                        base_symbol = tokens_map[base_idx]['symbol']
                        quote_symbol = tokens_map[quote_idx]['symbol']

                        # Create pair name
                        pair_name = f"{base_symbol}/{quote_symbol}"

                        trading_symbols.append({
                            "symbol": base_symbol,
                            "pair": pair_name
                        })

            # Hyperliquid doesn't provide inactive pairs in this endpoint
            non_trading_symbols = []

        elif exchange == "hyperliquid-futures":
            # Fetch futures market data
            payload = {'type': 'meta'}
            data = self._fetch_with_retry_post(url, payload)

            trading_symbols = []
            non_trading_symbols = []

            for item in data.get('universe', []):
                symbol = item.get('name', '')
                is_delisted = item.get('isDelisted', False)

                pair_name = f"{symbol}-USD"

                if not is_delisted:
                    trading_symbols.append({
                        "symbol": symbol,
                        "pair": pair_name
                    })
                else:
                    non_trading_symbols.append({
                        "symbol": symbol,
                        "pair": pair_name
                    })

        else:
            raise ValueError(f"Invalid Hyperliquid exchange type: {exchange}")

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
            exchange: Exchange identifier (e.g., 'hyperliquid-spot', 'hyperliquid-futures')
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
        market: str = "futures",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
        timezone: str = "0"
    ) -> List[Dict]:
        """
        Hyperliquid does not support historical kline/candlestick data.
        Use fetch_market_data() instead to get live market data including
        current price, volume, funding rate, and open interest.

        Raises:
            NotImplementedError: Always raised as Hyperliquid doesn't provide kline data
        """
        raise NotImplementedError(
            "Hyperliquid does not provide historical kline/candlestick data. "
            "Use fetch_market_data() instead to get live market data including "
            "current price, 24h volume, funding rate, and open interest."
        )

    def fetch_market_data(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Fetch live market data for Hyperliquid perpetuals

        Args:
            symbol: Optional symbol filter (e.g., 'BTC', 'ETH'). If None, returns all markets.

        Returns:
            List of market data dictionaries with keys:
                - symbol: Asset symbol (e.g., 'BTC')
                - mark_price: Current mark price
                - oracle_price: Oracle price
                - mid_price: Mid price
                - prev_day_price: Previous day price
                - price_change_24h: 24h price change percentage
                - volume_24h_base: 24h volume in base asset
                - volume_24h_usd: 24h volume in USD
                - funding_rate: Current funding rate
                - open_interest: Open interest
                - premium: Premium rate
                - max_leverage: Maximum leverage available
                - size_decimals: Size decimals for the asset

        Example:
            # Get all markets
            all_markets = exchange.fetch_market_data()

            # Get specific market
            btc_data = exchange.fetch_market_data('BTC')
        """
        # Build request payload
        payload = {
            "type": "metaAndAssetCtxs"
        }

        # Fetch data using POST request
        url = "https://api.hyperliquid.xyz/info"
        data = self._fetch_with_retry_post(url, payload)

        # Parse response
        # data[0] = meta (universe info)
        # data[1] = assetCtxs (live market data)
        meta = data[0]
        asset_ctxs = data[1]

        # Build combined market data
        markets = []
        for i, asset_info in enumerate(meta.get("universe", [])):
            asset_name = asset_info.get("name")

            # Skip if symbol filter is provided and doesn't match
            if symbol and asset_name != symbol:
                continue

            # Get corresponding asset context
            if i < len(asset_ctxs):
                ctx = asset_ctxs[i]

                # Calculate 24h price change
                mark_px = float(ctx.get("markPx", 0))
                prev_day_px = float(ctx.get("prevDayPx", 0))
                price_change_24h = ((mark_px - prev_day_px) / prev_day_px * 100) if prev_day_px > 0 else 0

                market_data = {
                    "symbol": asset_name,
                    "mark_price": ctx.get("markPx"),
                    "oracle_price": ctx.get("oraclePx"),
                    "mid_price": ctx.get("midPx"),
                    "prev_day_price": ctx.get("prevDayPx"),
                    "price_change_24h": round(price_change_24h, 2),
                    "volume_24h_base": ctx.get("dayBaseVlm"),
                    "volume_24h_usd": ctx.get("dayNtlVlm"),
                    "funding_rate": ctx.get("funding"),
                    "open_interest": ctx.get("openInterest"),
                    "premium": ctx.get("premium"),
                    "max_leverage": asset_info.get("maxLeverage"),
                    "size_decimals": asset_info.get("szDecimals"),
                    "is_delisted": asset_info.get("isDelisted", False)
                }

                markets.append(market_data)

        return markets

    def process_spot(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Hyperliquid spot exchange data

        Returns:
            Tuple of (active_pairs, inactive_pairs)
            Each dict contains: pair, symbol, exchange, is_active

        Example:
            ([
                {'symbol': 'BTC', 'pair': 'BTC/USDC', 'exchange': 'hyperliquid-spot', 'is_active': True},
                {'symbol': 'ETH', 'pair': 'ETH/USDC', 'exchange': 'hyperliquid-spot', 'is_active': True}
            ],
            [])
        """
        exchange = 'hyperliquid-spot'
        trading_symbols, non_trading_symbols = self.fetch_symbols_retry(
            self.spot_url, exchange
        )
        return self.generate_symbol_updates(exchange, trading_symbols, non_trading_symbols)

    def process_futures(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Hyperliquid futures exchange data

        Returns:
            Tuple of (active_pairs, inactive_pairs)
            Each dict contains: pair, symbol, exchange, is_active

        Example:
            ([
                {'symbol': 'BTC', 'pair': 'BTC-USD', 'exchange': 'hyperliquid-futures', 'is_active': True},
                {'symbol': 'ETH', 'pair': 'ETH-USD', 'exchange': 'hyperliquid-futures', 'is_active': True}
            ],
            [])
        """
        exchange = 'hyperliquid-futures'
        trading_symbols, non_trading_symbols = self.fetch_symbols_retry(
            self.futures_url, exchange
        )
        return self.generate_symbol_updates(exchange, trading_symbols, non_trading_symbols)
