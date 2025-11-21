"""
Panda Metrics API Client
Handles communication with the panda-backend-api for metrics data
"""

import httpx
import os
from typing import Dict, Optional, Literal
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


class PandaMetricsClient:
    """
    Client for fetching metrics from panda-backend-api
    Supports both CEX and DEX metrics

    Configuration is loaded from environment variables:
    - PANDA_BACKEND_API_URL: Base URL for the API
    - PANDA_API_KEY: API key for authentication
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0
    ):
        """
        Initialize Panda Metrics API client

        Args:
            base_url: Base URL for the panda-backend-api (defaults to PANDA_BACKEND_API_URL env var)
            api_key: API key for authentication (defaults to PANDA_API_KEY env var)
            timeout: Request timeout in seconds (default: 30.0)

        Raises:
            ValueError: If base_url is not provided and PANDA_BACKEND_API_URL is not set
        """
        # Use environment variables as defaults
        self.base_url = (base_url or os.getenv("PANDA_BACKEND_API_URL", "")).rstrip('/')
        self.api_key = api_key or os.getenv("PANDA_API_KEY")

        if not self.base_url:
            raise ValueError(
                "base_url must be provided either as parameter or via PANDA_BACKEND_API_URL environment variable"
            )

        self._client: Optional[httpx.Client] = None
        self.timeout = timeout

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialized HTTP client"""
        if self._client is None:
            headers = {}
            if self.api_key:
                headers["X-API-KEY"] = self.api_key

            self._client = httpx.Client(
                timeout=self.timeout,
                headers=headers
            )
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _fetch_with_retry(self, url: str, params: Dict) -> Dict:
        """
        Fetch data from API with retry logic

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            httpx.HTTPError: If request fails after retries
        """
        logger.info(f"Fetching metrics from: {url}")
        logger.debug(f"Parameters: {params}")

        response = self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def fetch_cex_metric(
        self,
        metric: str,
        exchange: str,
        token: str,
        timeframe: str,
        start_epoch: int,
        end_epoch: int,
        version: int = 4
    ) -> Dict:
        """
        Fetch CEX metric data

        Args:
            metric: Metric name (e.g., 'divine_dip')
            exchange: Exchange name (e.g., 'binance-spot', 'bybit-futures')
            token: Trading pair (e.g., 'BTCUSDT')
            timeframe: Time interval (e.g., '15m', '30m', '1H', '4H', '1D')
            start_epoch: Start time as Unix timestamp in seconds
            end_epoch: End time as Unix timestamp in seconds
            version: API version (default: 4)

        Returns:
            Dictionary with metric data

        Example:
            client.fetch_cex_metric(
                metric="divine_dip",
                exchange="bybit-futures",
                token="BTCUSDT",
                timeframe="1D",
                start_epoch=1648923900,
                end_epoch=1763231400,
                version=4
            )
        """
        url = f"{self.base_url}/metrics/panda_jlabs_metrics/"

        params = {
            "metric": metric,
            "version": version,
            "exchange_type": "CEX",
            "exchange": exchange,
            "token": token,
            "timeframe": timeframe,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch
        }

        return self._fetch_with_retry(url, params)

    def fetch_dex_metric(
        self,
        metric: str,
        chain: str,
        pool_address: str,
        timeframe: str,
        start_epoch: int,
        end_epoch: int,
        version: int = 4
    ) -> Dict:
        """
        Fetch DEX metric data

        Args:
            metric: Metric name (e.g., 'divine_dip')
            chain: Blockchain network (e.g., 'ethereum', 'bsc', 'solana')
            pool_address: DEX pool address
            timeframe: Time interval (e.g., '1H', '4H', '1D')
            start_epoch: Start time as Unix timestamp in seconds
            end_epoch: End time as Unix timestamp in seconds
            version: API version (default: 4)

        Returns:
            Dictionary with metric data

        Example:
            client.fetch_dex_metric(
                metric="divine_dip",
                chain="ethereum",
                pool_address="0x1234...",
                timeframe="1D",
                start_epoch=1648923900,
                end_epoch=1763231400,
                version=4
            )
        """
        url = f"{self.base_url}/metrics/panda_jlabs_metrics/"

        params = {
            "metric": metric,
            "version": version,
            "exchange_type": "DEX",
            "chain": chain,
            "pool_address": pool_address,
            "timeframe": timeframe,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch
        }

        return self._fetch_with_retry(url, params)

    def fetch_metric(
        self,
        metric: str,
        exchange_type: Literal["CEX", "DEX"],
        timeframe: str,
        start_epoch: int,
        end_epoch: int,
        version: int = 4,
        # CEX params
        exchange: Optional[str] = None,
        token: Optional[str] = None,
        # DEX params
        chain: Optional[str] = None,
        pool_address: Optional[str] = None
    ) -> Dict:
        """
        Unified method to fetch metric data for both CEX and DEX

        Args:
            metric: Metric name (e.g., 'divine_dip')
            exchange_type: Either 'CEX' or 'DEX'
            timeframe: Time interval
            start_epoch: Start time as Unix timestamp in seconds
            end_epoch: End time as Unix timestamp in seconds
            version: API version (default: 4)
            exchange: Exchange name (required for CEX)
            token: Trading pair (required for CEX)
            chain: Blockchain network (required for DEX)
            pool_address: DEX pool address (required for DEX)

        Returns:
            Dictionary with metric data

        Raises:
            ValueError: If required parameters are missing
        """
        if exchange_type == "CEX":
            if not exchange or not token:
                raise ValueError("CEX metrics require 'exchange' and 'token' parameters")

            return self.fetch_cex_metric(
                metric=metric,
                exchange=exchange,
                token=token,
                timeframe=timeframe,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                version=version
            )

        elif exchange_type == "DEX":
            if not chain or not pool_address:
                raise ValueError("DEX metrics require 'chain' and 'pool_address' parameters")

            return self.fetch_dex_metric(
                metric=metric,
                chain=chain,
                pool_address=pool_address,
                timeframe=timeframe,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                version=version
            )

        raise ValueError(f"Invalid exchange_type: {exchange_type}. Must be 'CEX' or 'DEX'")

    def fetch_orderbook_metric(
        self,
        metric: str,
        symbol: str,
        exchange: str,
        timeframe: str,
        volume: str,
        epoch_low: int,
        epoch_high: int
    ) -> Dict:
        """
        Fetch orderbook metric data

        Args:
            metric: Type of orderbook metric (e.g., 'bid_ask', 'bid_ask_ratio')
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            exchange: Exchange name (e.g., 'binance-futures', 'bybit-futures')
            timeframe: Time interval (e.g., '1D', '1H', '15m')
            volume: Volume depth range (e.g., '0-1', '0-10', '1-25')
            epoch_low: Start time as Unix timestamp in seconds
            epoch_high: End time as Unix timestamp in seconds

        Returns:
            Dictionary with orderbook metric data

        Example:
            client.fetch_orderbook_metric(
                metric="bid_ask_ratio",
                symbol="BTCUSDT",
                exchange="binance-futures",
                timeframe="1D",
                volume="0-1",
                epoch_low=1628360700,
                epoch_high=1763317860
            )
        """
        url = f"{self.base_url}/workbench/orderbook/"

        params = {
            "metric": metric.lower(),
            "symbol": symbol,
            "exchange": exchange,
            "timeframe": timeframe,
            "volume": volume.lower(),
            "epoch_low": epoch_low,
            "epoch_high": epoch_high
        }

        return self._fetch_with_retry(url, params)

    def fetch_jlabs_v1_metric(
        self,
        metric: str,
        symbol: str,
        time_delta: int,
        start_epoch: int,
        end_epoch: int
    ) -> Dict:
        """
        Fetch JLabs V1 metrics (slippage or price_equilibrium)

        Args:
            metric: Type of metric ('slippage' or 'price_equilibrium')
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            time_delta: Timezone offset in minutes (e.g., 330 for India, 0 for UTC)
            start_epoch: Start time as Unix timestamp in seconds
            end_epoch: End time as Unix timestamp in seconds

        Returns:
            Dictionary with metric data

        Example:
            client.fetch_jlabs_v1_metric(
                metric="slippage",
                symbol="BTCUSDT",
                time_delta=330,
                start_epoch=1758110100,
                end_epoch=1763247780
            )
        """
        url = f"{self.base_url}/metrics/panda-jlabs-metrics/v1/"

        params = {
            "metric": metric.lower(),
            "symbol": symbol,
            "time_delta": time_delta,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch
        }

        return self._fetch_with_retry(url, params)

    def fetch_orderflow_metric(
        self,
        metric: str,
        symbol: str,
        exchange: str,
        timeframe: str,
        volume: str,
        epoch_low: int,
        epoch_high: int
    ) -> Dict:
        """
        Fetch orderflow/tradebook metric data

        Args:
            metric: Type of orderflow metric (e.g., 'trade_vol', 'tradebook_delta')
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            exchange: Exchange name (e.g., 'binance-futures', 'bybit-futures')
            timeframe: Time interval (e.g., '1D', '1H', '15m')
            volume: Volume tier range (e.g., '0-1k', '1m-10m')
            epoch_low: Start time as Unix timestamp in seconds
            epoch_high: End time as Unix timestamp in seconds

        Returns:
            Dictionary with orderflow metric data

        Example:
            client.fetch_orderflow_metric(
                metric="trade_vol",
                symbol="BTCUSDT",
                exchange="binance-futures",
                timeframe="4H",
                volume="0-1k",
                epoch_low=1758110100,
                epoch_high=1763249400
            )
        """
        url = f"{self.base_url}/workbench/orderflow/"

        params = {
            "metric": metric.lower(),
            "symbol": symbol,
            "exchange": exchange,
            "timeframe": timeframe,
            "volume": volume.lower(),
            "epoch_low": epoch_low,
            "epoch_high": epoch_high
        }

        return self._fetch_with_retry(url, params)

    def fetch_jlabs_proprietary_v1(
        self,
        metric: str,
        symbol: Optional[str],
        timeframe: str,
        start_epoch: int,
        end_epoch: int
    ) -> Dict:
        """
        Fetch JLabs proprietary metrics using V1 API

        Args:
            metric: Metric type ('cari', 'dxy_risk', 'rosi', 'token_rating')
            symbol: Trading pair symbol (optional for some metrics)
            timeframe: Time interval
            start_epoch: Start time as Unix timestamp in seconds
            end_epoch: End time as Unix timestamp in seconds

        Returns:
            Dictionary with metric data

        Example:
            client.fetch_jlabs_proprietary_v1(
                metric="cari",
                symbol="BTCUSDT",
                timeframe="1D",
                start_epoch=1707935100,
                end_epoch=1763322540
            )
        """
        url = f"{self.base_url}/metrics/panda-jlabs-metrics/v1/"

        params = {
            "metric": metric.lower(),
            "timeframe": timeframe,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch
        }

        if symbol:
            params["symbol"] = symbol

        return self._fetch_with_retry(url, params)

    def fetch_jlabs_proprietary_v2(
        self,
        metric: str,
        token: Optional[str],
        timeframe: str,
        version: int = 2,
        metric_param: Optional[str] = None
    ) -> Dict:
        """
        Fetch JLabs proprietary metrics using V2/V3 API

        Args:
            metric: Metric type (Title Case: 'CARI', 'DXY Risk', 'ROSI', 'Token rating')
            token: Trading pair symbol (optional for some metrics)
            timeframe: Time interval
            version: API version (2 or 3)
            metric_param: Sub-metric for Token Rating

        Returns:
            Dictionary with metric data

        Example:
            client.fetch_jlabs_proprietary_v2(
                metric="Token rating",
                token="BTCUSDT",
                timeframe="1D",
                version=2,
                metric_param="Overall Rating"
            )
        """
        url = f"{self.base_url}/metrics/panda_jlabs_metrics/"

        params = {
            "metric": metric,
            "timeframe": timeframe,
            "version": version
        }

        if token:
            params["token"] = token

        if metric_param:
            params["metric_param"] = metric_param

        return self._fetch_with_retry(url, params)
