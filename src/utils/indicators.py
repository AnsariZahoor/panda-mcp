"""
Technical Indicators Module
Provides technical analysis indicators using pandas-ta
"""

import pandas as pd
import pandas_ta as ta
import logging
from typing import List, Dict, Optional, Union, Literal

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """
    Technical analysis indicators calculator
    Supports trend, momentum, volatility, and volume indicators
    """

    @staticmethod
    def _klines_to_dataframe(klines: List[Dict]) -> pd.DataFrame:
        """
        Convert kline data to pandas DataFrame with proper column names

        Args:
            klines: List of kline dictionaries from exchange

        Returns:
            DataFrame with columns: open, high, low, close, volume, and timestamp

        Raises:
            ValueError: If klines data is invalid or empty
        """
        if not klines:
            raise ValueError("Klines data is empty")

        df = pd.DataFrame(klines)

        # Standardize column names for pandas-ta
        # Map common exchange formats to standard OHLCV format
        column_mapping = {
            'open_time': 'timestamp',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }

        # Select and rename relevant columns
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_cols].rename(columns=column_mapping)

        # Convert numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert timestamp to datetime and set as index for pandas-ta
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)

        return df

    @staticmethod
    def _dataframe_to_dict(df: pd.DataFrame) -> List[Dict]:
        """
        Convert DataFrame back to list of dictionaries

        Args:
            df: DataFrame to convert

        Returns:
            List of dictionaries
        """
        # Reset index to include timestamp in output
        df_reset = df.reset_index()
        # Replace NaN with None for better JSON serialization
        return df_reset.where(pd.notnull(df_reset), None).to_dict('records')

    # ========================================================================
    # TREND INDICATORS
    # ========================================================================

    @staticmethod
    def calculate_sma(
        klines: List[Dict],
        period: int = 20,
        source: str = "close"
    ) -> Dict:
        """
        Calculate Simple Moving Average (SMA)

        Args:
            klines: List of kline dictionaries
            period: Period for SMA calculation (default: 20)
            source: Price source - 'close', 'open', 'high', 'low' (default: 'close')

        Returns:
            Dictionary with original data and SMA values

        Example:
            result = TechnicalIndicators.calculate_sma(klines, period=50)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df[f'SMA_{period}'] = ta.sma(df[source], length=period)

        return {
            "indicator": "SMA",
            "period": period,
            "source": source,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_ema(
        klines: List[Dict],
        period: int = 20,
        source: str = "close"
    ) -> Dict:
        """
        Calculate Exponential Moving Average (EMA)

        Args:
            klines: List of kline dictionaries
            period: Period for EMA calculation (default: 20)
            source: Price source - 'close', 'open', 'high', 'low' (default: 'close')

        Returns:
            Dictionary with original data and EMA values

        Example:
            result = TechnicalIndicators.calculate_ema(klines, period=50)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df[f'EMA_{period}'] = ta.ema(df[source], length=period)

        return {
            "indicator": "EMA",
            "period": period,
            "source": source,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_macd(
        klines: List[Dict],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> Dict:
        """
        Calculate MACD (Moving Average Convergence Divergence)

        Args:
            klines: List of kline dictionaries
            fast: Fast period (default: 12)
            slow: Slow period (default: 26)
            signal: Signal period (default: 9)

        Returns:
            Dictionary with MACD, MACD signal, and MACD histogram

        Example:
            result = TechnicalIndicators.calculate_macd(klines)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        macd = ta.macd(df['close'], fast=fast, slow=slow, signal=signal)

        if macd is not None:
            df = pd.concat([df, macd], axis=1)

        return {
            "indicator": "MACD",
            "parameters": {"fast": fast, "slow": slow, "signal": signal},
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    # ========================================================================
    # MOMENTUM INDICATORS
    # ========================================================================

    @staticmethod
    def calculate_rsi(
        klines: List[Dict],
        period: int = 14,
        source: str = "close"
    ) -> Dict:
        """
        Calculate Relative Strength Index (RSI)

        Args:
            klines: List of kline dictionaries
            period: Period for RSI calculation (default: 14)
            source: Price source (default: 'close')

        Returns:
            Dictionary with RSI values (0-100 scale)

        Example:
            result = TechnicalIndicators.calculate_rsi(klines, period=14)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df[f'RSI_{period}'] = ta.rsi(df[source], length=period)

        return {
            "indicator": "RSI",
            "period": period,
            "source": source,
            "overbought": 70,
            "oversold": 30,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_stochastic(
        klines: List[Dict],
        k_period: int = 14,
        d_period: int = 3,
        smooth_k: int = 3
    ) -> Dict:
        """
        Calculate Stochastic Oscillator

        Args:
            klines: List of kline dictionaries
            k_period: %K period (default: 14)
            d_period: %D period (default: 3)
            smooth_k: Smoothing for %K (default: 3)

        Returns:
            Dictionary with %K and %D values

        Example:
            result = TechnicalIndicators.calculate_stochastic(klines)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        stoch = ta.stoch(df['high'], df['low'], df['close'],
                         k=k_period, d=d_period, smooth_k=smooth_k)

        if stoch is not None:
            df = pd.concat([df, stoch], axis=1)

        return {
            "indicator": "Stochastic",
            "parameters": {"k_period": k_period, "d_period": d_period, "smooth_k": smooth_k},
            "overbought": 80,
            "oversold": 20,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_cci(
        klines: List[Dict],
        period: int = 20
    ) -> Dict:
        """
        Calculate Commodity Channel Index (CCI)

        Args:
            klines: List of kline dictionaries
            period: Period for CCI calculation (default: 20)

        Returns:
            Dictionary with CCI values

        Example:
            result = TechnicalIndicators.calculate_cci(klines, period=20)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df[f'CCI_{period}'] = ta.cci(df['high'], df['low'], df['close'], length=period)

        return {
            "indicator": "CCI",
            "period": period,
            "overbought": 100,
            "oversold": -100,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    # ========================================================================
    # VOLATILITY INDICATORS
    # ========================================================================

    @staticmethod
    def calculate_bollinger_bands(
        klines: List[Dict],
        period: int = 20,
        std_dev: float = 2.0,
        source: str = "close"
    ) -> Dict:
        """
        Calculate Bollinger Bands

        Args:
            klines: List of kline dictionaries
            period: Period for moving average (default: 20)
            std_dev: Number of standard deviations (default: 2.0)
            source: Price source (default: 'close')

        Returns:
            Dictionary with upper band, middle band (SMA), and lower band

        Example:
            result = TechnicalIndicators.calculate_bollinger_bands(klines)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        bbands = ta.bbands(df[source], length=period, std=std_dev)

        if bbands is not None:
            df = pd.concat([df, bbands], axis=1)

        return {
            "indicator": "Bollinger Bands",
            "parameters": {"period": period, "std_dev": std_dev, "source": source},
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_atr(
        klines: List[Dict],
        period: int = 14
    ) -> Dict:
        """
        Calculate Average True Range (ATR)

        Args:
            klines: List of kline dictionaries
            period: Period for ATR calculation (default: 14)

        Returns:
            Dictionary with ATR values

        Example:
            result = TechnicalIndicators.calculate_atr(klines, period=14)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df[f'ATR_{period}'] = ta.atr(df['high'], df['low'], df['close'], length=period)

        return {
            "indicator": "ATR",
            "period": period,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_keltner_channels(
        klines: List[Dict],
        period: int = 20,
        atr_multiplier: float = 2.0
    ) -> Dict:
        """
        Calculate Keltner Channels

        Args:
            klines: List of kline dictionaries
            period: Period for EMA calculation (default: 20)
            atr_multiplier: ATR multiplier (default: 2.0)

        Returns:
            Dictionary with upper, middle (EMA), and lower channels

        Example:
            result = TechnicalIndicators.calculate_keltner_channels(klines)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        kc = ta.kc(df['high'], df['low'], df['close'], length=period, scalar=atr_multiplier)

        if kc is not None:
            df = pd.concat([df, kc], axis=1)

        return {
            "indicator": "Keltner Channels",
            "parameters": {"period": period, "atr_multiplier": atr_multiplier},
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    # ========================================================================
    # VOLUME INDICATORS
    # ========================================================================

    @staticmethod
    def calculate_obv(klines: List[Dict]) -> Dict:
        """
        Calculate On-Balance Volume (OBV)

        Args:
            klines: List of kline dictionaries

        Returns:
            Dictionary with OBV values

        Example:
            result = TechnicalIndicators.calculate_obv(klines)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df['OBV'] = ta.obv(df['close'], df['volume'])

        return {
            "indicator": "OBV",
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_vwap(klines: List[Dict]) -> Dict:
        """
        Calculate Volume Weighted Average Price (VWAP)

        Args:
            klines: List of kline dictionaries

        Returns:
            Dictionary with VWAP values

        Example:
            result = TechnicalIndicators.calculate_vwap(klines)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df['VWAP'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])

        return {
            "indicator": "VWAP",
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    @staticmethod
    def calculate_mfi(
        klines: List[Dict],
        period: int = 14
    ) -> Dict:
        """
        Calculate Money Flow Index (MFI)

        Args:
            klines: List of kline dictionaries
            period: Period for MFI calculation (default: 14)

        Returns:
            Dictionary with MFI values (0-100 scale)

        Example:
            result = TechnicalIndicators.calculate_mfi(klines, period=14)
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)
        df[f'MFI_{period}'] = ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=period)

        return {
            "indicator": "MFI",
            "period": period,
            "overbought": 80,
            "oversold": 20,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }

    # ========================================================================
    # MULTIPLE INDICATORS
    # ========================================================================

    @staticmethod
    def calculate_multiple_indicators(
        klines: List[Dict],
        indicators: List[str]
    ) -> Dict:
        """
        Calculate multiple indicators at once

        Args:
            klines: List of kline dictionaries
            indicators: List of indicator names to calculate
                       Supported: 'RSI', 'MACD', 'SMA', 'EMA', 'BB', 'ATR', 'STOCH', 'OBV', 'VWAP'

        Returns:
            Dictionary with all calculated indicators

        Example:
            result = TechnicalIndicators.calculate_multiple_indicators(
                klines, ['RSI', 'MACD', 'BB']
            )
        """
        df = TechnicalIndicators._klines_to_dataframe(klines)

        indicator_map = {
            'RSI': lambda: ta.rsi(df['close'], length=14),
            'RSI_14': lambda: ta.rsi(df['close'], length=14),
            'MACD': lambda: ta.macd(df['close']),
            'SMA_20': lambda: ta.sma(df['close'], length=20),
            'SMA_50': lambda: ta.sma(df['close'], length=50),
            'SMA_200': lambda: ta.sma(df['close'], length=200),
            'EMA_20': lambda: ta.ema(df['close'], length=20),
            'EMA_50': lambda: ta.ema(df['close'], length=50),
            'EMA_200': lambda: ta.ema(df['close'], length=200),
            'BB': lambda: ta.bbands(df['close'], length=20, std=2),
            'ATR': lambda: ta.atr(df['high'], df['low'], df['close'], length=14),
            'STOCH': lambda: ta.stoch(df['high'], df['low'], df['close']),
            'OBV': lambda: ta.obv(df['close'], df['volume']),
            'VWAP': lambda: ta.vwap(df['high'], df['low'], df['close'], df['volume']),
            'MFI': lambda: ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=14),
            'CCI': lambda: ta.cci(df['high'], df['low'], df['close'], length=20),
        }

        calculated = []
        for indicator in indicators:
            indicator_upper = indicator.upper()
            if indicator_upper in indicator_map:
                try:
                    result = indicator_map[indicator_upper]()
                    if result is not None:
                        if isinstance(result, pd.DataFrame):
                            df = pd.concat([df, result], axis=1)
                        else:
                            df[indicator_upper] = result
                        calculated.append(indicator_upper)
                except Exception as e:
                    logger.warning(f"Failed to calculate {indicator}: {str(e)}")
            else:
                logger.warning(f"Unknown indicator: {indicator}")

        return {
            "indicators_calculated": calculated,
            "data": TechnicalIndicators._dataframe_to_dict(df)
        }
