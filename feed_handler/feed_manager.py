"""
Feed Manager - Orchestrates multiple market data handlers.

Features:
- Spin up handlers for different vendors dynamically
- Unified tick stream from all sources
- Health monitoring and auto-recovery
- Graceful shutdown
"""
import asyncio
from typing import Optional, Callable, Awaitable, Type
from dataclasses import dataclass, field
from enum import Enum, auto
import logging

from models import Tick, Vendor, SubscriptionRequest, FeedStats, current_time_ns
from base_handler import FeedHandler, TickCallback
from tick_buffer import TickBuffer, TickAggregator

# Import handlers
import sys
sys.path.insert(0, '/home/claude/market_data_handler/handlers')
from handlers import DatabentoHandler
from handlers import BloombergHandler
from handlers import CMEHandler


logger = logging.getLogger(__name__)


class FeedState(Enum):
    """Feed handler states."""
    STOPPED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    RECONNECTING = auto()
    ERROR = auto()


@dataclass
class FeedConfig:
    """Configuration for a feed handler."""
    vendor: Vendor
    symbols: list[str]
    enabled: bool = True
    
    # Vendor-specific config
    api_key: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    dataset: Optional[str] = None
    
    # Connection settings
    reconnect_max_attempts: int = -1  # -1 = infinite
    reconnect_delay_seconds: float = 1.0


@dataclass
class FeedStatus:
    """Status of a feed handler."""
    vendor: Vendor
    state: FeedState
    connected: bool
    symbols: list[str]
    ticks_received: int = 0
    last_tick_time: int = 0
    errors: list[str] = field(default_factory=list)
    latency_avg_us: int = 0


class FeedManager:
    """
    Manages multiple market data feed handlers.
    
    Architecture:
    ```
    ┌─────────────────────────────────────────────────────────────┐
    │                      FeedManager                             │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
    │  │  Databento   │  │  Bloomberg   │  │     CME      │      │
    │  │   Handler    │  │   Handler    │  │   Handler    │      │
    │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
    │         │                 │                 │               │
    │         └─────────────────┼─────────────────┘               │
    │                           ▼                                  │
    │                    ┌──────────────┐                         │
    │                    │  TickBuffer  │                         │
    │                    └──────┬───────┘                         │
    │                           │                                  │
    │         ┌─────────────────┼─────────────────┐               │
    │         ▼                 ▼                 ▼               │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
    │  │   Database   │  │  Aggregator  │  │  Downstream  │      │
    │  │   Writer     │  │   (OHLCV)    │  │   Systems    │      │
    │  └──────────────┘  └──────────────┘  └──────────────┘      │
    └─────────────────────────────────────────────────────────────┘
    ```
    """
    
    def __init__(
        self,
        on_tick: Optional[TickCallback] = None,
        on_batch: Optional[Callable[[list[Tick]], Awaitable[None]]] = None,
        buffer_size: int = 65536,
        batch_size: int = 1000,
        flush_interval_ms: int = 100,
    ):
        """
        Initialize FeedManager.
        
        Args:
            on_tick: Callback for individual ticks (low-level)
            on_batch: Callback for tick batches (recommended for throughput)
            buffer_size: Ring buffer capacity
            batch_size: Ticks per batch
            flush_interval_ms: Max time between batch flushes
        """
        self._on_tick = on_tick
        self._on_batch = on_batch
        
        # Handler registry
        self._handlers: dict[Vendor, FeedHandler] = {}
        self._handler_tasks: dict[Vendor, asyncio.Task] = {}
        self._configs: dict[Vendor, FeedConfig] = {}
        self._states: dict[Vendor, FeedState] = {}
        
        # Tick buffer for batching
        self._buffer: Optional[TickBuffer] = None
        if on_batch:
            self._buffer = TickBuffer(
                on_batch=on_batch,
                batch_size=batch_size,
                flush_interval_ms=flush_interval_ms,
                buffer_capacity=buffer_size,
            )
        
        # Aggregator for OHLCV
        self._aggregator: Optional[TickAggregator] = None
        
        # Global stats
        self._total_ticks = 0
        self._start_time = 0
        
        self._running = False
    
    def _create_handler(self, config: FeedConfig) -> FeedHandler:
        """Factory method to create vendor-specific handler."""
        
        if config.vendor == Vendor.DATABENTO:
            return DatabentoHandler(
                api_key=config.api_key or "",
                dataset=config.dataset or "GLBX.MDP3",
                on_tick=self._handle_tick,
                on_error=self._handle_error,
                host=config.host or "localhost",
                port=config.port or 13000,
            )
        
        elif config.vendor == Vendor.BLOOMBERG:
            return BloombergHandler(
                on_tick=self._handle_tick,
                on_error=self._handle_error,
                host=config.host or "localhost",
                port=config.port or 8194,
            )
        
        elif config.vendor == Vendor.CME:
            return CMEHandler(
                on_tick=self._handle_tick,
                on_error=self._handle_error,
                multicast_group=config.host or "224.0.28.1",
                port=config.port or 14310,
            )
        
        else:
            raise ValueError(f"Unsupported vendor: {config.vendor}")
    
    async def _handle_tick(self, tick: Tick) -> None:
        """Central tick handler - routes ticks to buffer/callbacks."""
        self._total_ticks += 1
        
        # Route to buffer for batching
        if self._buffer:
            await self._buffer.push(tick)
        
        # Route to individual tick callback
        if self._on_tick:
            await self._on_tick(tick)
        
        # Route to aggregator
        if self._aggregator:
            await self._aggregator.process_tick(tick)
    
    async def _handle_error(self, error: Exception) -> None:
        """Central error handler."""
        logger.error(f"Feed error: {error}")
    
    def add_feed(self, config: FeedConfig) -> None:
        """Add a feed configuration."""
        self._configs[config.vendor] = config
        self._states[config.vendor] = FeedState.STOPPED
    
    async def start(self) -> None:
        """Start all configured feeds."""
        self._running = True
        self._start_time = current_time_ns()
        
        # Start buffer
        if self._buffer:
            await self._buffer.start()
        
        # Start each feed
        for vendor, config in self._configs.items():
            if config.enabled:
                await self._start_feed(vendor)
    
    async def _start_feed(self, vendor: Vendor) -> None:
        """Start a single feed."""
        config = self._configs.get(vendor)
        if not config:
            return
        
        self._states[vendor] = FeedState.CONNECTING
        
        try:
            # Create handler
            handler = self._create_handler(config)
            self._handlers[vendor] = handler
            
            # Subscribe to symbols
            await handler.connect()
            await handler.subscribe(config.symbols)
            
            self._states[vendor] = FeedState.CONNECTED
            
            # Start handler task
            task = asyncio.create_task(
                handler.start(),
                name=f"feed_{vendor.value}"
            )
            self._handler_tasks[vendor] = task
            
            logger.info(f"Started {vendor.value} feed with {len(config.symbols)} symbols")
            
        except Exception as e:
            self._states[vendor] = FeedState.ERROR
            logger.error(f"Failed to start {vendor.value} feed: {e}")
    
    async def stop(self) -> None:
        """Stop all feeds gracefully."""
        self._running = False
        
        # Stop all handlers
        for vendor, handler in self._handlers.items():
            try:
                await handler.stop()
                self._states[vendor] = FeedState.STOPPED
            except Exception as e:
                logger.error(f"Error stopping {vendor.value}: {e}")
        
        # Cancel tasks
        for task in self._handler_tasks.values():
            task.cancel()
        
        if self._handler_tasks:
            await asyncio.gather(*self._handler_tasks.values(), return_exceptions=True)
        
        # Stop buffer
        if self._buffer:
            await self._buffer.stop()
        
        # Flush aggregator
        if self._aggregator:
            await self._aggregator.flush_all()
        
        logger.info("All feeds stopped")
    
    async def subscribe(self, vendor: Vendor, symbols: list[str]) -> None:
        """Subscribe to additional symbols on a feed."""
        handler = self._handlers.get(vendor)
        if handler and handler.is_connected:
            await handler.subscribe(symbols)
            
            # Update config
            if vendor in self._configs:
                self._configs[vendor].symbols.extend(symbols)
    
    async def unsubscribe(self, vendor: Vendor, symbols: list[str]) -> None:
        """Unsubscribe from symbols on a feed."""
        handler = self._handlers.get(vendor)
        if handler and handler.is_connected:
            await handler.unsubscribe(symbols)
    
    def get_status(self, vendor: Vendor) -> Optional[FeedStatus]:
        """Get status of a specific feed."""
        handler = self._handlers.get(vendor)
        config = self._configs.get(vendor)
        state = self._states.get(vendor, FeedState.STOPPED)
        
        if not config:
            return None
        
        stats = handler.get_all_stats() if handler else {}
        total_ticks = sum(s.ticks_received for s in stats.values())
        avg_latency = (
            sum(s.latency_ns_avg for s in stats.values()) // len(stats)
            if stats else 0
        )
        
        return FeedStatus(
            vendor=vendor,
            state=state,
            connected=handler.is_connected if handler else False,
            symbols=config.symbols,
            ticks_received=total_ticks,
            last_tick_time=max((s.last_tick_time_ns for s in stats.values()), default=0),
            latency_avg_us=avg_latency // 1000,
        )
    
    def get_all_status(self) -> dict[Vendor, FeedStatus]:
        """Get status of all feeds."""
        return {
            vendor: status
            for vendor in self._configs
            if (status := self.get_status(vendor))
        }
    
    def get_stats(self) -> dict:
        """Get aggregate statistics."""
        uptime_ns = current_time_ns() - self._start_time if self._start_time else 0
        uptime_seconds = uptime_ns / 1_000_000_000
        
        buffer_stats = self._buffer.stats if self._buffer else None
        
        return {
            "total_ticks": self._total_ticks,
            "ticks_per_second": self._total_ticks / uptime_seconds if uptime_seconds > 0 else 0,
            "uptime_seconds": uptime_seconds,
            "feeds_connected": sum(1 for h in self._handlers.values() if h.is_connected),
            "feeds_total": len(self._handlers),
            "buffer_stats": {
                "ticks_received": buffer_stats.ticks_received if buffer_stats else 0,
                "ticks_processed": buffer_stats.ticks_processed if buffer_stats else 0,
                "ticks_dropped": buffer_stats.ticks_dropped if buffer_stats else 0,
                "batches_flushed": buffer_stats.batches_flushed if buffer_stats else 0,
                "avg_latency_us": buffer_stats.avg_latency_ns // 1000 if buffer_stats else 0,
            },
        }
    
    def enable_aggregation(
        self,
        timeframe_seconds: int = 60,
        on_bar: Optional[Callable] = None,
    ) -> None:
        """Enable real-time OHLCV aggregation."""
        self._aggregator = TickAggregator(
            timeframe_seconds=timeframe_seconds,
            on_bar=on_bar,
        )


async def run_feeds(configs: list[FeedConfig], duration_seconds: int = 60) -> None:
    """
    Convenience function to run feeds for a specified duration.
    
    Example usage:
    ```python
    configs = [
        FeedConfig(
            vendor=Vendor.DATABENTO,
            symbols=["ESZ3", "NQZ3"],
            api_key="your-key",
            dataset="GLBX.MDP3"
        ),
        FeedConfig(
            vendor=Vendor.BLOOMBERG,
            symbols=["ESZ3 Index", "NQZ3 Index"],
        ),
    ]
    
    await run_feeds(configs, duration_seconds=3600)
    ```
    """
    ticks_received = []
    
    async def on_batch(batch: list[Tick]) -> None:
        ticks_received.extend(batch)
        logger.info(f"Received batch of {len(batch)} ticks, total: {len(ticks_received)}")
    
    manager = FeedManager(on_batch=on_batch)
    
    for config in configs:
        manager.add_feed(config)
    
    await manager.start()
    
    try:
        await asyncio.sleep(duration_seconds)
    except asyncio.CancelledError:
        pass
    finally:
        await manager.stop()
        
        stats = manager.get_stats()
        logger.info(f"Final stats: {stats}")
