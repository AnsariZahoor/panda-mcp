"""
Exchange Factory
Registry and factory for managing exchange adapters
"""

from typing import Dict, Type, List
from .base_exchange import BaseExchange
from ..exchanges.binance import BinanceExchange
from ..exchanges.bybit import BybitExchange
from ..exchanges.hyperliquid import HyperliquidExchange


class ExchangeFactory:
    """Factory for creating and managing exchange instances"""

    _registry: Dict[str, Type[BaseExchange]] = {}

    @classmethod
    def register(cls, name: str, exchange_class: Type[BaseExchange]) -> None:
        """
        Register an exchange adapter

        Args:
            name: Exchange name (e.g., 'binance')
            exchange_class: Exchange class that inherits from BaseExchange
        """
        cls._registry[name.lower()] = exchange_class

    @classmethod
    def create(cls, name: str, db_handler=None) -> BaseExchange:
        """
        Create an exchange instance

        Args:
            name: Exchange name (e.g., 'binance')
            db_handler: Optional database handler

        Returns:
            Exchange instance

        Raises:
            ValueError: If exchange is not registered
        """
        exchange_class = cls._registry.get(name.lower())
        if not exchange_class:
            raise ValueError(
                f"Exchange '{name}' not found. "
                f"Available exchanges: {cls.list_exchanges()}"
            )
        return exchange_class(db_handler)

    @classmethod
    def list_exchanges(cls) -> List[str]:
        """
        Get list of registered exchanges

        Returns:
            List of exchange names
        """
        return list(cls._registry.keys())

    @classmethod
    def get_exchange_info(cls, name: str) -> Dict:
        """
        Get information about a registered exchange

        Args:
            name: Exchange name

        Returns:
            Dictionary with exchange information

        Raises:
            ValueError: If exchange is not registered
        """
        exchange_class = cls._registry.get(name.lower())
        if not exchange_class:
            raise ValueError(
                f"Exchange '{name}' not found. "
                f"Available exchanges: {cls.list_exchanges()}"
            )

        # Call classmethod directly without creating instance
        markets = exchange_class.get_supported_markets()

        return {
            "name": name,
            "class": exchange_class.__name__,
            "supported_markets": markets,
            "description": exchange_class.__doc__ or "No description available"
        }


# Register available exchanges
ExchangeFactory.register("binance", BinanceExchange)
ExchangeFactory.register("bybit", BybitExchange)
ExchangeFactory.register("hyperliquid", HyperliquidExchange)

# Add more exchanges here as they are implemented:
# ExchangeFactory.register("okx", OkxExchange)
# ExchangeFactory.register("coinbase", CoinbaseExchange)
