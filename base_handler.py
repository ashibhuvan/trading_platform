"""
Abstract base class for all vendor feed handlers.
Uses Protocol for structural subtyping (duck typing with type hints).
"""
from abc import ABC, abstractmethod
from typing import Protocol, Callable, AsyncIterator, Optional
import asyncio
from collections.abc import Awaitable

from models import Tick, SubscriptionRequest, FeedStats, Vendor


# Type alias for tick callback
TickCallback = Callable[[Tick], Awaitable[None]]


class FeedHandler(ABC):
    """
    Abstract base class for market data feed handlers.
    
    Each vendor implementation must:
    1. Connect to the data source
    2. Subscribe to symbols
    3. Parse incoming data into normalized Tick objects
    4. Handle reconnection and error recovery
    """
    
    def __init__(
        self,
        vendor: Vendor,
        on_tick: TickCallback,
        on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
    ):
        self.vendor = vendor
        self._on_tick = on_tick
        self._on_error = on_error
        self._running = False
        self._connected = False
        self._stats: dict[str, FeedStats] = {}
        self._reconnect_delay = 1.0  # Initial reconnect delay in seconds
        self._max_reconnect_delay = 60.0
        self._subscriptions: list[str] = []
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    def get_stats(self, symbol: str) -> Optional[FeedStats]:
        return self._stats.get(symbol)
    
    def get_all_stats(self) -> dict[str, FeedStats]:
        return self._stats.copy()
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully disconnect from the data source."""
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to market data for given symbols."""
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from market data for given symbols."""
        pass
    
    @abstractmethod
    async def _read_messages(self) -> AsyncIterator[Tick]:
        """
        Generator that yields Tick objects from the feed.
        Must be implemented by each vendor handler.
        """
        yield  # type: ignore
    
    async def start(self) -> None:
        """Start the feed handler with automatic reconnection."""
        self._running = True
        
        while self._running:
            try:
                await self.connect()
                self._connected = True
                self._reconnect_delay = 1.0  # Reset on successful connect
                
                # Resubscribe if we have existing subscriptions
                if self._subscriptions:
                    await self.subscribe(self._subscriptions)
                
                # Main message loop
                async for tick in self._read_messages():
                    if not self._running:
                        break
                    await self._on_tick(tick)
                    
            except asyncio.CancelledError:
                self._running = False
                break
            except Exception as e:
                self._connected = False
                if self._on_error:
                    await self._on_error(e)
                
                if self._running:
                    # Exponential backoff for reconnection
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2, 
                        self._max_reconnect_delay
                    )
        
        await self.disconnect()
        self._connected = False
    
    async def stop(self) -> None:
        """Stop the feed handler gracefully."""
        self._running = False


class TickProcessor(Protocol):
    """Protocol for tick processing callbacks."""
    
    async def process(self, tick: Tick) -> None:
        """Process a single tick."""
        ...
    
    async def flush(self) -> None:
        """Flush any buffered data."""
        ...
