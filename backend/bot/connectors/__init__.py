"""Connector modules — data provider abstractions following the Polymarket agents pattern."""

from .binance_connector import BinanceStreamManager
from .gamma_connector import PolymarketFetcher

__all__ = ["BinanceStreamManager", "PolymarketFetcher"]
